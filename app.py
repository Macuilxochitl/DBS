from flask import Flask, request
from db import insert_data, get_all_data
import requests

from utils import make_ok_response, make_json_response, make_error_response
from redis_utils import add_node, get_all_nodes
from config import IS_CENTRAL_NODE, NODE_PORT, CENTRAL_NODE_ADDRESS, NODE_NAME, NODE_ADDRESS

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


def register():
    reg = False
    try:
        r = requests.put('http://{}/register/'.format(CENTRAL_NODE_ADDRESS),
                         json={'name': NODE_NAME, "address": NODE_ADDRESS})
        res = r.json()
        print("Register result: {}".format(res))
        if res.get("result", "") == "ok":
            reg = True
    except Exception as e:
        print(e)
    if not reg:
        print("Register failed! exiting...")
        exit(-1)


if __name__ == '__main__':
    if not IS_CENTRAL_NODE:
        register()
    app.run(host="0.0.0.0", port=NODE_PORT, debug=False)
