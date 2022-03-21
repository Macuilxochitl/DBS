import threading
import time

from flask import Flask, request
from db import check_data_id_dup, get_all_data, insert_data_to_prepare, submit_prepare, del_prepare
from collections import OrderedDict, defaultdict

from utils import make_ok_response, make_json_response, make_error_response
from api_utils import (ping, register, get_all_node, get_all_node_leader,
                       send_proposal, kill_node, send_prepare, send_submit, send_rollback)
from redis_utils import add_node, del_node, get_all_nodes_from_redis
from config import IS_CENTRAL_NODE, NODE_PORT, NODE_NAME, NODE_ADDRESS, NODE_CHECK_INTERVAL

app = Flask(__name__)

LEADER = None


def prepare(data) -> (bool, list):
    """

    :return: result of prepare, False if anyone is False
    """
    success = []
    for name, address in get_all_node().items():
        ret, msg = send_prepare(address, data)
        if not ret:
            return False, success
        else:
            success.append(name)
    return True, success


def submit() -> (bool, list):
    """

    :return: result of submit, False if anyone is False
    """
    success = []
    for name, address in get_all_node().items():
        ret, msg = send_submit(address)
        if not ret:
            return False, success
        else:
            success.append(name)
    return True


def rollback(nodes, data) -> list:
    """

    :return: result of rollback, list of node who is rollback failed
    """
    failed = []
    for name, address in get_all_node().items():
        if name not in nodes:
            continue
        ret, msg = send_rollback(address, data)
        if not ret:
            failed.append(name)
    return failed


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
        return make_json_response(get_all_nodes_from_redis())
    if request.method == 'PUT':
        req = request.json
        res = add_node(req.get("name"), req.get("address"))
        if res:
            return make_ok_response()
        else:
            return make_error_response("Insert failed.")


@app.route('/leader/', methods=['GET'])
def leader_handler():
    """
    leader handler, get node's now leader
    :return: Flask response
    """
    if IS_CENTRAL_NODE:
        return make_error_response("This is a central node, not provide Data service.")
    if request.method == 'GET':
        return make_json_response({"leader": LEADER})


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
        req['data_id'] = req.get('id')
        del req['id']
        ret, msg = send_proposal(get_all_node().get(LEADER), req)
        if ret:
            return make_ok_response()
        else:
            return make_error_response(msg)


@app.route('/prepare/', methods=['PUT'])
def prepare_handler():
    """
    prepare handler, will insert data to prepare db and return result.
    :return: Flask response
    """
    if IS_CENTRAL_NODE:
        return make_error_response("This is a central node, not provide Data service.")
    if request.method == 'PUT':
        try:
            req = request.json
            ret = insert_data_to_prepare(req)
            if ret:
                return make_ok_response()
            else:
                return make_error_response("prepare failed: {}".format(ret))
        except Exception as e:
            return make_error_response("prepare failed: {}".format(e))


@app.route('/submit/', methods=['PUT'])
def submit_handler():
    """
    submit handler, will move prepare data to data and return result.
    :return: Flask response
    """
    if IS_CENTRAL_NODE:
        return make_error_response("This is a central node, not provide Data service.")
    if request.method == 'PUT':
        try:
            ret = submit_prepare()
            if ret:
                return make_ok_response()
            else:
                return make_error_response("submit failed: {}".format(ret))
        except Exception as e:
            return make_error_response("submit failed: {}".format(e))


@app.route('/rollback/', methods=['PUT'])
def rollback_handler():
    """
    rollback handler, will delete prepare data.
    :return: Flask response
    """
    if IS_CENTRAL_NODE:
        return make_error_response("This is a central node, not provide Data service.")
    if request.method == 'PUT':
        try:
            req = request.json
            ret = del_prepare(req)
            if ret:
                return make_ok_response()
            else:
                return make_error_response("rollback failed: {}".format(ret))
        except Exception as e:
            return make_error_response("rollback failed: {}".format(e))


@app.route('/proposal/', methods=['PUT'])
def proposal_handler():
    """
    proposal handler, Leader only. Used to submit a proposal from client.
    :return: Flask response
    """
    if IS_CENTRAL_NODE:
        return make_error_response("This is a central node, not provide Data service.")
    if request.method == 'PUT':
        req = request.json

        ret = check_data_id_dup(req)
        if not ret:
            return make_error_response("dup id!")

        ret, success = prepare(req)
        if not ret:

            ret = rollback(success, req)

            if not ret:
                return make_error_response("prepare failed, rollback success!")
            else:
                nodes = get_all_node()
                for name in ret:
                    kill_node(nodes[name])
                return make_error_response("prepare failed, rollback failed!")

        ret, success = submit()
        if not ret:

            ret = rollback(success, req)

            if not ret:
                return make_error_response("submit failed, rollback success!")
            else:
                nodes = get_all_node()
                for name in ret:
                    kill_node(nodes[name])
                return make_error_response("submit failed, rollback failed!")

        return make_ok_response()


@app.route('/ping/', methods=['GET'])
def ping_handler():
    """
    ping handler, will return name and address of node, used to check if node is alive.
    :return: Flask response
    """
    if request.method == 'GET':
        return make_json_response({"name": NODE_NAME, "address": NODE_ADDRESS})


@app.route('/kill/', methods=['GET'])
def kill_handler():
    """
    kill node when needed...
    :return: Flask response
    """
    if request.method == 'GET':
        exit(-1)


def check_node(name, address) -> bool:
    alive = ping(address)
    if not alive:
        print("Node {}({}) is gone".format(name, address))
    return alive


def vote_leader() -> (str, str):
    while True:
        nodes = get_all_node()

        # get now all node's leader
        all_node_leader = get_all_node_leader()
        all_node_leader = {k: v for k, v in all_node_leader.items() if v}
        # cal leader appear count
        leader_count = defaultdict(int)
        for node_name, leader_name in all_node_leader.items():
            leader_count[leader_name] += 1

        leader_count = OrderedDict(sorted(leader_count.items(), key=lambda i: i[1]))

        leader_name = None

        if leader_count:
            leader_name = next(reversed(leader_count))

        # if the leader that have max count means this is our leader,
        #   but still need to check if it is alive to avoid this is due to other node's delay.

        if leader_name and leader_name in nodes and check_node(leader_name, nodes[leader_name]):
            return leader_name, nodes[leader_name]

        # if no leader or leader is unreachable, need to vote for new one.
        d = OrderedDict()
        for name, data in nodes.items():
            d[name] = len(data)
        leader = next(reversed(OrderedDict(sorted(d.items(), key=lambda i: (i[1], i[0])))))

        global LEADER
        LEADER = leader

        time.sleep(1)


def leader_checker():
    """
    Running in common node, used to check if leader node is alive or will vote for new one.
    """
    leader_name = None
    leader_address = None
    while True:
        if not leader_name:
            leader_name, leader_address = vote_leader()
        if not check_node(leader_name, leader_address):
            leader_name, leader_address = None, None
        else:
            global LEADER
            LEADER = leader_name
        time.sleep(NODE_CHECK_INTERVAL)


def node_checker():
    """
    Running in central node, used to check if there's dead node in all registered node,
        and remove it from registered node if it's.
    """
    while True:
        nodes = get_all_nodes_from_redis()
        for name, address in nodes.items():
            if not check_node(name, address):
                del_node(name)
        time.sleep(NODE_CHECK_INTERVAL)


if __name__ == '__main__':

    # if this is common node and register failed that exit.
    if not IS_CENTRAL_NODE and not register():
        print("Register failed! exiting...")
        exit(-1)

    if IS_CENTRAL_NODE:
        target = node_checker
    else:
        target = leader_checker
    t = threading.Thread(target=target)
    t.start()

    app.run(host="0.0.0.0", port=NODE_PORT, debug=False)
