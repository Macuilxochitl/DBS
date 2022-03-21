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
    return [{"data_id": i.data_id, "raw": i.raw, "signature": i.signature} for i in PrepareData.select()]


def get_all_data() -> [dict]:
    return [{"data_id": i.data_id, "raw": i.raw, "signature": i.signature} for i in Data.select()]


def insert_data_to_prepare(data: [dict]) -> bool:
    return PrepareData.insert_many(data).execute()


def submit_prepare() -> bool:
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
    try:
        PrepareData.delete().where(PrepareData.data_id == data.get("data_id", "")).execute()
        return True
    except Exception as e:
        print(e)
        return False


def check_data_id_dup(data: dict) -> bool:
    rows = Data.select(Data.data_id).where(Data.data_id == data.get("data_id", ""))
    print(len(rows))
    return False if rows else True
