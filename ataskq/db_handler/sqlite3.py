import re
from typing import NamedTuple
import sqlite3

from .db_handler import DBHandler


class SqliteConnection(NamedTuple):
    path: str

    def __str__(self):
        return f"sqlite://{self.path}"


def from_connection_str(db):
    format = 'sqlite://path'
    pattern = r'sqlite://(?P<path>.+)$'
    match = re.match(pattern, db)

    if not match:
        raise Exception(f"db must be in '{format}', ex: 'sqlite://ataskq.db.sqlite3'")

    path = match.group('path')
    ret = SqliteConnection(path=path)

    return ret


class SQLite3DBHandler(DBHandler):
    def __init__(self, conn=None, **kwargs) -> None:
        self._connection = from_connection_str(conn)
        super().__init__(**kwargs)

    @property
    def pragma_foreign_keys_on(self):
        return 'PRAGMA foreign_keys = ON'
        
    @property
    def format_symbol(self):
        return '?'

    @property
    def connection(self):
        return self._connection

    @property
    def db_path(self):
        return self._connection.path

    @property
    def bytes_type(self):
        return 'MEDIUMBLOB'

    @property
    def primary_key(self):
        return 'INTEGER PRIMARY KEY AUTOINCREMENT'

    @property
    def timestamp_type(self):
        return 'DATETIME'

    def timestamp(self, ts):
        return f"'{ts}'"

    @property
    def begin_exclusive(self):
        return 'BEGIN EXCLUSIVE'

    @property
    def db_path(self):
        return self._connection.path

    def connect(self):
        conn = sqlite3.connect(self.db_path)
        return conn
