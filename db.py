from peewee import *
from config import db_config

mysql_db = MySQLDatabase(db_config["db_name"], user=db_config["db_user"], password=db_config["db_password"],
                         host=db_config["db_host"], port=db_config["db_port"])


class BaseModel(Model):
    class Meta:
        database = mysql_db


class PrepareData(BaseModel):
    data_id = CharField(index=True, unique=True)
    raw = CharField()
    signature = CharField()

    class Meta:
        table_name = "prepare_data"

    def __str__(self):
        return self.data_id


class Data(BaseModel):
    data_id = CharField(index=True, unique=True)
    raw = CharField()
    signature = CharField()

    class Meta:
        table_name = "data"

    def __str__(self):
        return self.data_id


def get_all_prepare() -> [dict]:
    """
    Get all prepare data from db.
    :return: result of query, e.g. [{"data_id": "1", "raw": "a", "signature": "aaa"}]
    """
    return [{"data_id": i.data_id, "raw": i.raw, "signature": i.signature} for i in PrepareData.select()]


def get_all_data_from_db() -> [dict]:
    """
    Get all data from db.
    :return: result of query, e.g. [{"data_id": "1", "raw": "a", "signature": "aaa"}]
    """
    return [{"data_id": i.data_id, "raw": i.raw, "signature": i.signature} for i in Data.select()]


def insert_data_to_prepare(data: [dict]) -> bool:
    """
    Insert data to prepare.
    :param data: datas, e.g. [{"data_id": "1", "raw": "a", "signature": "aaa"}]
    :return: result of query
    """
    return PrepareData.insert_many(data).execute()


def insert_data(data: [dict]) -> bool:
    """
    Insert data to data, pls be noticed that it will replace if conflict on data_id.
    :param data: datas, e.g. [{"data_id": "1", "raw": "a", "signature": "aaa"}]
    :return: result of query
    """
    return Data.insert_many(data).on_conflict_replace().execute()


def submit_prepare() -> bool:
    """
    Move all prepare data to data.
    :return: result of query
    """
    with mysql_db.atomic() as transaction:
        try:
            Data.insert_many(get_all_prepare()).execute()
            PrepareData.delete().execute()
            return True
        except Exception as e:
            print(e)
            transaction.rollback()
            return False


def del_prepare(data: dict) -> bool:
    """
    Del specific prepare data.
    :return: result of query
    """
    try:
        PrepareData.delete().where(PrepareData.data_id == data.get("data_id", "")).execute()
        return True
    except Exception as e:
        print(e)
        return False


def check_data_id_dup(data: dict) -> bool:
    """
    Check a data_id of data if is existed in db.
    :return: result of query
    """
    rows = Data.select(Data.data_id).where(Data.data_id == data.get("data_id", ""))
    print(len(rows))
    return False if rows else True
