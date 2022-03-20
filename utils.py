from flask import Flask, make_response, jsonify


def make_ok_response() -> Flask.response_class:
    """
    :return: return a default ok response
    """
    return make_response(jsonify({"result": "ok"}), 200)


def make_json_response(data) -> Flask.response_class:
    """
    :return: return an ok response with some data
    """
    return make_response(jsonify({"result": "ok", "data": data}), 200)


def make_error_response(msg) -> Flask.response_class:
    """
    :return: return an ok response with some data
    """
    return make_response(jsonify({"result": "error", "msg": msg}), 200)
