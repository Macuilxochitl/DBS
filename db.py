from peewee import *
from config import db_config

mysql_db = MySQLDatabase(db_config["db_name"], user=db_config["db_user"], password=db_config["db_password"],
                         host=db_config["db_host"], port=db_config["db_port"])


class BaseModel(Model):
    class Meta:
        database = mysql_db


class Data(BaseModel):
    data_id = CharField(index=True)
    raw = CharField()
    signature = CharField()

    class Meta:
        table_name = "data"

    def __str__(self):
        return self.data_id


def get_all_data() -> [dict]:
    return [{"id": i.data_id, "raw": i.raw, "signature": i.signature} for i in Data.select()]


def insert_data(data: [dict]) -> bool:
    return Data.insert_many(data).execute()
