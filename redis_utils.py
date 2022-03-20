import time

import redis

r = redis.StrictRedis(host='localhost', port=6379, db=10, decode_responses=True)


def add_node(name, address) -> int:
    """
    Add a node into redis that key is node's name and value is node's address.
    :param name: node's name
    :param address: node's address
    :return: insert result
    """
    return r.hset("node", name, address)


def get_all_nodes() -> dict:
    """
    return all node's information.
    :return:
    """
    res = r.hgetall("node")
    return res
