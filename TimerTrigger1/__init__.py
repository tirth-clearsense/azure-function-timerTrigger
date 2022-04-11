
import datetime
import logging
import databases
import azure.functions as func

from configparser import ConfigParser
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from base_schema import base_schema
from sqlalchemy import select
from sqlalchemy.sql import text
from azure.storage.queue import (
        QueueClient,
        BinaryBase64EncodePolicy,
        BinaryBase64DecodePolicy
)
import json
import os, uuid
config_object = ConfigParser()
config_object.read("config.ini")
DB_CONFIG = config_object["CREDENTIALS_DATABASE"]
DATABASE_URL = 'postgresql://{}:{}@{}/{}?sslmode={}'.format(DB_CONFIG['USERNAME'], DB_CONFIG['PASSWORD'],DB_CONFIG['HOST'],DB_CONFIG['NAME'], 'prefer')
# DATABASE_URL = 'postgresql://{}:{}@{}/{}?sslmode={}'.format("tirth", "password", "localhost", "personicletest", 'prefer')
engine = sqlalchemy.create_engine(
    DATABASE_URL, pool_size=3, max_overflow=0
)
Base = declarative_base(engine)

database = databases.Database(DATABASE_URL)
metadata = sqlalchemy.MetaData()


users = sqlalchemy.Table(
    "external_connections",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.INT, primary_key=True),
    sqlalchemy.Column("userId", sqlalchemy.String, primary_key=True,nullable=False),
    sqlalchemy.Column("service", sqlalchemy.String,nullable=False),
    sqlalchemy.Column("access_token", sqlalchemy.String,nullable=False),
    sqlalchemy.Column("expires_in", sqlalchemy.INT,nullable=False),
    sqlalchemy.Column("created_at", sqlalchemy.DATETIME,nullable=False, default=datetime.datetime.utcnow()),
    sqlalchemy.Column("external_user_id", sqlalchemy.String),
    sqlalchemy.Column("refresh_token", sqlalchemy.String),
    sqlalchemy.Column("last_accessed_at", sqlalchemy.DATETIME,nullable=True),
    sqlalchemy.Column("scope", sqlalchemy.String,nullable=True),
    )


async def main(mytimer: func.TimerRequest) -> None:
    await database.connect()
    connect_str = os.environ['AZURE_STORAGE_CONNECTION_STRING']
    queue_client = QueueClient.from_connection_string(connect_str, "personicle-data-download-queue")


    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    query = select(users).where(users.c.last_accessed_at < (datetime.datetime.utcnow() - datetime.timedelta(hours=1))) 
    rows = await database.fetch_all(query)
    fetch_data_for = []
    for r in rows:
        data= {}
        data["individual_id"] = tuple(r.values())[1]
        data["service_name"] = tuple(r.values())[2]
        data["service_token"] = tuple(r.values())[3]
        data["last_accessed_at"] = str(tuple(r.values())[8])
        fetch_data_for.append(data)

    json_object = json.dumps(fetch_data_for)

    logging.info("Adding data: " + json_object)
    queue_client.send_message(json_object)

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    await database.disconnect()

