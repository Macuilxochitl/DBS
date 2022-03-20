from flask import Flask, request
from db import insert_data, get_all_data

from utils import make_ok_response, make_json_response

app = Flask(__name__)


@app.route('/data/', methods=['GET', 'PUT'])
def data_handler():
    """
    main handler, action determine by http method
    If http method is "GET", will return all data that storing in this node.
    If http method is "PUT", will receive a data and save it into db.
    :return: Flask response
    """
    if request.method == 'GET':
        return make_json_response(get_all_data())
    if request.method == 'PUT':
        req = request.json
        insert_data(req)
        return make_ok_response()


if __name__ == '__main__':
    app.run()
