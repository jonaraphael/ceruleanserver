import psycopg2
import pandas as pd
from configs import server_config
import json

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class DBConnection:
    """A class that knows how to connect to and manage connections to the DB
    """

    def __init__(
        self,
        host=server_config.DB_HOST,
        user=server_config.DB_USER,
        password=server_config.DB_PASSWORD,
        database=server_config.DB_DATABASE,
        port=server_config.DB_PORT,
        dbtype=server_config.DB_TYPE,
    ):
        self.engine = create_engine(
            f"{dbtype}://{user}:{password}@{host}:{port}/{database}", echo=server_config.ECHO_SQL,
        )
        self.sess = sessionmaker(bind=self.engine)()
