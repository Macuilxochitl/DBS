from flask import make_response, jsonify


def make_ok_response():
    """
    :return: return a default ok response
    """
    return make_response(jsonify({"result": "ok"}), 200)


def make_json_response(data):
    """
    :return: return an ok response with some data
    """
    return make_response(jsonify({"result": "ok", "data": data}), 200)
