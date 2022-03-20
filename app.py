import threading
import time

from flask import Flask, request
from db import insert_data, get_all_data
import requests

from utils import make_ok_response, make_json_response, make_error_response
from redis_utils import add_node, del_node, get_all_nodes
from config import IS_CENTRAL_NODE, NODE_PORT, CENTRAL_NODE_ADDRESS, NODE_NAME, NODE_ADDRESS, NODE_CHECK_INTERVAL

app = Flask(__name__)


@app.route('/register/', methods=['GET', 'PUT'])
def register_handler():
    """
    register handler, action determine by http method
    If http method is "GET", will return all registered node.
    If http method is "PUT", will register a node.
    :return: Flask response
    """
    if not IS_CENTRAL_NODE:
        return make_error_response("This is a common node, not provide Registry service.")
    if request.method == 'GET':
        return make_json_response(get_all_nodes())
    if request.method == 'PUT':
        req = request.json
        res = add_node(req.get("name"), req.get("address"))
        if res:
            return make_ok_response()
        else:
            return make_error_response("Insert failed.")


@app.route('/data/', methods=['GET', 'PUT'])
def data_handler():
    """
    data handler, action determine by http method
    If http method is "GET", will return all data that storing in this node.
    If http method is "PUT", will receive a data and save it into db.
    :return: Flask response
    """
    if IS_CENTRAL_NODE:
        return make_error_response("This is a central node, not provide Data service.")
    if request.method == 'GET':
        return make_json_response(get_all_data())
    if request.method == 'PUT':
        req = request.json
        insert_data(req)
        return make_ok_response()


@app.route('/ping/', methods=['GET'])
def ping_handler():
    """
    ping handler, will return name and address of node, used to check if node is alive.
    :return: Flask response
    """
    if request.method == 'GET':
        return make_json_response({"name": NODE_NAME, "address": NODE_ADDRESS})


def register():
    reg = False
    try:
        r = requests.put('http://{}/register/'.format(CENTRAL_NODE_ADDRESS),
                         json={'name': NODE_NAME, "address": NODE_ADDRESS}).json()
        print("Register result: {}".format(r))
        if r.get("result", "") == "ok":
            reg = True
    except Exception as e:
        print(e)
    if not reg:
        print("Register failed! exiting...")
        exit(-1)


def check_node(name, address):
    alive = False
    try:
        r = requests.get('http://{}/ping/'.format(address)).json()
        if r.get("result", "") == "ok":
            alive = True
    except Exception as e:
        print(e)
    if not alive:
        print("Node {}({}) is gone".format(name, address))
        del_node(name)


def node_checker():
    while True:
        nodes = get_all_nodes()
        for name, address in nodes.items():
            check_node(name, address)
        time.sleep(NODE_CHECK_INTERVAL)


if __name__ == '__main__':
    if not IS_CENTRAL_NODE:
        register()
    if IS_CENTRAL_NODE:
        t = threading.Thread(target=node_checker)
        t.start()
    app.run(host="0.0.0.0", port=NODE_PORT, debug=False)
