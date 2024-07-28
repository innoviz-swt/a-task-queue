import re
from typing import NamedTuple, Type
import sqlite3
from datetime import datetime

from ..model import Model, DateTime
from .handler import to_datetime, from_datetime
from .sql_handler import SQLHandler, SQLSession


class SqliteConnection(NamedTuple):
    path: str

    def __str__(self):
        return f"sqlite://{self.path}"


class SQLiteSession(SQLSession):
    def __init__(self, connection: SqliteConnection, exclusive) -> None:
        super().__init__()
        self.exclusive = exclusive
        self.connection = connection
        self.conn = None
        self.curso = None

    @property
    def lastrowid(self):
        return self.cursor.lastrowid

    def connect(self):
        self.conn = sqlite3.connect(self.connection.path)
        self.cursor = self.conn.cursor()
        # enable foreign keys for each connection (sqlite default is off)
        # https://www.sqlite.org/foreignkeys.html
        # Foreign key constraints are disabled by default (for backwards
        # compatibility), so must be enabled separately for each database
        # connection
        self.cursor.execute("PRAGMA foreign_keys = ON")

        if self.exclusive:
            self.cursor.execute("BEGIN EXCLUSIVE")
        else:
            self.cursor.execute("BEGIN")


class SQLite3DBHandler(SQLHandler):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @staticmethod
    def encode(obj, model_cls: Type[Model], key, key_cls):
        if obj is None:
            return obj

        if issubclass(key_cls, (datetime, DateTime)):
            return from_datetime(obj)

        return obj

    # Custom decoding function
    @staticmethod
    def decode(obj, model_cls: Type[Model], key, key_cls):
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
    def for_update(self):
        return ""

    def session(self, exclusive=False):
        session = SQLiteSession(self.connection, exclusive)
        return session

    def _create(self, s: SQLiteSession, model: Model):
        d = self._to_interface(model)
        keys = list(d.keys())
        values = list(d.values())
        if keys:
            s.execute(
                f'INSERT INTO {model.table_key()} ({", ".join(keys)}) VALUES ({", ".join([self.format_symbol] * len(keys))})',
                values,
            )
        else:
            s.execute(f"INSERT INTO {model.table_key()} DEFAULT VALUES"),

        model_id = s.lastrowid
        setattr(model, model.id_key(), model_id)
