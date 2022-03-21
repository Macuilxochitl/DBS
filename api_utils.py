import requests
from config import CENTRAL_NODE_ADDRESS, NODE_NAME, NODE_ADDRESS


def get_all_data(address) -> [dict]:
    try:
        r = requests.get('http://{}/data/'.format(address)).json()
        if r.get("result", "") == "ok":
            return r.get("data", [])
    except Exception as e:
        print(e)
        return []


def ping(address) -> bool:
    ret = False
    try:
        r = requests.get('http://{}/ping/'.format(address)).json()
        if r.get("result", "") == "ok":
            ret = True
    except Exception as e:
        print(e)
    return ret


def register() -> bool:
    ret = False
    try:
        r = requests.put('http://{}/register/'.format(CENTRAL_NODE_ADDRESS),
                         json={'name': NODE_NAME, "address": NODE_ADDRESS}).json()
        print("Register result: {}".format(r))
        if r.get("result", "") == "ok":
            ret = True
    except Exception as e:
        print(e)
    return ret


def send_proposal(address, data) -> (bool, str):
    ret = False
    msg = None
    try:
        r = requests.put('http://{}/proposal/'.format(address),
                         json=data).json()
        print("proposal result: {}".format(r))
        if r.get("result", "") == "ok":
            ret = True
        else:
            msg = r.get("msg", "")
    except Exception as e:
        print(e)
    return ret, msg


def send_prepare(address, data) -> (bool, str):
    ret = False
    msg = None
    try:
        r = requests.put('http://{}/prepare/'.format(address),
                         json=data).json()
        print("prepare result: {}".format(r))
        if r.get("result", "") == "ok":
            ret = True
        else:
            msg = r.get("msg", "")
    except Exception as e:
        print(e)
    return ret, msg


def send_submit(address) -> (bool, str):
    ret = False
    msg = None
    try:
        r = requests.put('http://{}/submit/'.format(address)).json()
        print("submit result: {}".format(r))
        if r.get("result", "") == "ok":
            ret = True
        else:
            msg = r.get("msg", "")
    except Exception as e:
        print(e)
    return ret, msg


def send_rollback(address, data) -> (bool, str):
    ret = False
    msg = None
    try:
        r = requests.put('http://{}/rollback/'.format(address),
                         json=data).json()
        print("rollback result: {}".format(r))
        if r.get("result", "") == "ok":
            ret = True
        else:
            msg = r.get("msg", "")
    except Exception as e:
        print(e)
    return ret, msg


def get_all_node() -> dict:
    try:
        r = requests.get('http://{}/register/'.format(CENTRAL_NODE_ADDRESS)).json()
        return r.get("data", [])
    except Exception as e:
        print(e)
        return []


def get_all_node_leader() -> dict:
    ret = {}
    for name, address in get_all_node().items():
        try:
            r = requests.get('http://{}/leader/'.format(address)).json()
            ret[name] = r.get("data", {}).get("leader", "")
        except Exception as e:
            print(e)
    return ret


def kill_node(address):
    try:
        requests.get('http://{}/kill/'.format(address)).json()
    except:
        ...
