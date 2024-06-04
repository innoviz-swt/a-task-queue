import re
from typing import NamedTuple, List
import sqlite3
from datetime import datetime

from ..imodel import IModel
from .handler import to_datetime, from_datetime
from .db_handler import DBHandler, transaction_decorator


class SqliteConnection(NamedTuple):
    path: str

    def __str__(self):
        return f"sqlite://{self.path}"


class SQLite3DBHandler(DBHandler):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @staticmethod
    def from_connection_str(conn):
        format = "sqlite://path"
        pattern = r"sqlite://(?P<path>.+)$"
        match = re.match(pattern, conn)

        if not match:
            raise Exception(f"conn must be in '{format}', ex: 'sqlite://ataskq.db.sqlite3'")

        path = match.group("path")
        ret = SqliteConnection(path=path)

        return ret

    @staticmethod
    def m2i_serialize():
        type_handlers = {
            datetime: lambda v: from_datetime(v),
        }

        return type_handlers

    @staticmethod
    def i2m_serialize():
        type_handlers = {
            datetime: lambda v: to_datetime(v),
        }

        return type_handlers

    @property
    def pragma_foreign_keys_on(self):
        return "PRAGMA foreign_keys = ON"

    @property
    def format_symbol(self):
        return "?"

    @property
    def connection(self):
        return self._connection

    @property
    def db_path(self):
        return self._connection.path

    @property
    def bytes_type(self):
        return "MEDIUMBLOB"

    @property
    def primary_key(self):
        return "INTEGER PRIMARY KEY AUTOINCREMENT"

    @property
    def timestamp_type(self):
        return "DATETIME"

    def timestamp(self, ts):
        return f"'{ts}'"

    @property
    def begin_exclusive(self):
        return "BEGIN EXCLUSIVE"

    @property
    def for_update(self):
        return ""

    def connect(self):
        conn = sqlite3.connect(self.db_path)
        return conn

    @transaction_decorator()
    def _create_bulk(self, c, model_cls: IModel, ikwargs: List[dict]) -> List[int]:
        # todo: consolidate all ikwargs with same keys to single insert command
        model_ids = []
        for v in ikwargs:
            d = {k: v for k, v in v.items() if model_cls.id_key() not in k}
            keys = list(d.keys())
            values = list(d.values())
            c.execute(
                f'INSERT INTO {model_cls.table_key()} ({", ".join(keys)}) VALUES ({", ".join([self.format_symbol] * len(keys))})',
                values,
            )
            model_id = c.lastrowid
            model_ids.append(model_id)

        return model_ids
