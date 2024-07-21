import re
from typing import NamedTuple, List
import sqlite3
from datetime import datetime

from ..model import Model, DateTime
from .handler import to_datetime, from_datetime
from .db_handler import DBHandler


class SqliteConnection(NamedTuple):
    path: str

    def __str__(self):
        return f"sqlite://{self.path}"


class SQLite3DBHandler(DBHandler):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @staticmethod
    def encode(obj, model_cls: Model, key, key_cls):
        if obj is None:
            return obj

        if issubclass(key_cls, (datetime, DateTime)):
            return from_datetime(obj)

        return obj

    # Custom decoding function
    def decode(obj, model_cls: Model, key, key_cls):
        if obj is None:
            return obj

        if issubclass(key_cls, (datetime, DateTime)):
            return to_datetime(obj)

        return obj

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
            DateTime: lambda v: from_datetime(v),
        }

        return type_handlers

    @staticmethod
    def i2m_serialize():
        type_handlers = {
            DateTime: lambda v: to_datetime(v),
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
        conn = sqlite3.connect(self.connection.path)
        return conn

    def _create(self, c: sqlite3.Connection, model: Model):
        d = self.to_interface(model)
        keys = list(d.keys())
        values = list(d.values())
        if keys:
            c.execute(
                f'INSERT INTO {model.table_key()} ({", ".join(keys)}) VALUES ({", ".join([self.format_symbol] * len(keys))})',
                values,
            )
        else:
            c.execute(f"INSERT INTO {model.table_key()} DEFAULT VALUES"),

        model_id = c.lastrowid
        setattr(model, model.id_key(), model_id)
