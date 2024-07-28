import re
from typing import NamedTuple, Union, Type
from datetime import datetime

try:
    import psycopg2
except ModuleNotFoundError:
    raise Exception("'psycopg2' is reuiqred for using atasgq postgresql adapter.")

from .sql_handler import SQLHandler, SQLSession
from .handler import to_datetime, from_datetime, DateTime
from ..model import Model


class PostgreSQLConnection(NamedTuple):
    user: Union[None, str]
    password: Union[None, str]
    host: str
    port: int
    database: str

    def __str__(self):
        if self.user:
            userspec = f"{self.user}" + (self.password and f":{self.password}") + "@"

        return f"pg://{userspec}{self.host}:{self.port}/{self.database}"


class PostgreSQLSession(SQLSession):
    def __init__(self, connection: PostgreSQLConnection, exclusive) -> None:
        super().__init__()
        self.exclusive = exclusive
        self.connection = connection
        self.conn = None
        self.curso = None

    def connect(self):
        self.conn = psycopg2.connect(
            host=self.connection.host,
            database=self.connection.database,
            user=self.connection.user,
            password=self.connection.password,
        )
        self.cursor = self.conn.cursor()
        self.cursor.execute("BEGIN")


class PostgreSQLHandler(SQLHandler):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @staticmethod
    def from_connection_str(conn):
        # https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING-URIS
        # todo: add params spec support
        format = "pg://[user[:password]@][host][:port][/database]"
        pattern = r"pg://(?P<user>[^:@]+)(:(?P<password>[^@]+))?@?(?P<host>[^:/]+)(:(?P<port>\d+))?/(?P<database>.+)$"

        match = re.match(pattern, conn)

        if not match:
            raise Exception(f"db must be in '{format}', ex: 'pg://user:password@localhost:5432/mydb'")

        user = match.group("user")
        password = match.group("password")
        host = match.group("host")
        port = match.group("port")
        database = match.group("database")
        ret = PostgreSQLConnection(user=user, password=password, host=host, port=port, database=database)

        return ret

    @staticmethod
    def encode(obj, model_cls: Type[Model], key, key_cls):
        if obj is None:
            return obj

        return obj

    # Custom decoding function
    @staticmethod
    def decode(obj, model_cls: Type[Model], key, key_cls):
        if obj is None:
            return obj

        return obj

    @property
    def format_symbol(self):
        return "%s"

    @property
    def connection(self):
        return self._connection

    @property
    def bytes_type(self):
        return "BYTEA"

    @property
    def primary_key(self):
        return "SERIAL PRIMARY KEY"

    @property
    def timestamp_type(self):
        return "TIMESTAMP"

    def timestamp(self, ts):
        return f"'{ts}'::timestamp"

    @property
    def for_update(self):
        return "FOR UPDATE"

    def session(self, exclusive=False):
        session = PostgreSQLSession(self.connection, exclusive)
        return session
