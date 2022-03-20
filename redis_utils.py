import time

import redis

r = redis.StrictRedis(host='localhost', port=6379, db=10, decode_responses=True)


def add_node(name, address) -> bool:
    """
    Add a node into redis that key is node's name and value is node's address.
    :param name: node's name
    :param address: node's address
    :return: insert result
    """
    try:
        r.hset("node", name, address)
        return True
    except Exception as e:
        print(e)
        return False


def del_node(name) -> int:
    """
    del a node in redis.
    :param name: node's name
    :return: del result
    """
    return r.hdel("node", name)


def get_all_nodes_from_redis() -> dict:
    """
    return all node's information.
    :return:
    """
    res = r.hgetall("node")
    return res
