import psycopg2
import pandas as pd
from configs import server_config
import json

from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker, Query
from contextlib import contextmanager


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
            f"{dbtype}://{user}:{password}@{host}:{port}/{database}",
            echo=server_config.ECHO_SQL,
        )
        self.sess = sessionmaker(bind=self.engine)()


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    db = DBConnection()
    try:
        yield db.sess
        db.sess.commit()
    except:
        db.sess.rollback()
        raise
    finally:
        db.sess.close()


def unique_join(self, *props, **kwargs):
    """ This is a function added to the query object, that allows programmatic
    creation of queries by allowing repeated calling of any class that is
    already joined without causing an error.
    """
    if props[0] in [c.entity for c in self._join_entities]:
        return self
    return self.join(*props, **kwargs)


Query.unique_join = unique_join

# XXXHELP For some reason this returns None instead of 0, and 2 instead of 1!!!
# def get_count(q):
#     count_q = q.statement.with_only_columns([func.count()]).order_by(None)
#     count = q.session.execute(count_q).scalar()
#     return count
