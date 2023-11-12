import re
from typing import NamedTuple

try:
    import psycopg2
except ModuleNotFoundError:
    raise Exception("psycopg2 is reuiqred for using atasgq postgresql adapter.")

from .db_handler import DBHandler


class PostgresConnection(NamedTuple):
    user: None or str
    password: None or str
    host: str
    port: int
    database: str

    def __str__(self):
        if self.user:
            userspec = f"{self.user}" + (self.password and f':{self.password}') + '@'

        return f"postgresql://{userspec}{self.host}:{self.port}/{self.database}"


def from_connection_str(db):
    # https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING-URIS
    # todo: add params spec support
    format = 'postgresql://[user[:password]@][host][:port][/database]'
    pattern = r'postgresql://(?P<user>[^:@]+)(:(?P<password>[^@]+))?@?(?P<host>[^:/]+)(:(?P<port>\d+))?/(?P<database>.+)$'

    match = re.match(pattern, db)

    if not match:
        raise Exception(f"db must be in '{format}', ex: 'postgresql://user:password@localhost:5432/mydb'")

    user = match.group('user')
    password = match.group('password')
    host = match.group('host')
    port = match.group('port')
    database = match.group('database')
    ret = PostgresConnection(user=user, password=password, host=host, port=port, database=database)

    return ret


class PostgresqlDBHandler(DBHandler):
    def __init__(self, conn=None, **kwargs) -> None:
        self._connection = from_connection_str(conn)
        super().__init__(**kwargs)

    @property
    def format_symbol(self):
        return '%s'

    @property
    def connection(self):
        return self._connection

    @property
    def bytes_type(self):
        return 'MEDIUMBLOB'

    @property
    def primary_key(self):
        return 'SERIAL PRIMARY KEY'

    @property
    def timestamp_type(self):
        return 'DATETIME'

    def timestamp(self, ts):
        return f"'{ts}'::timestamp"

    @property
    def begin_exclusive(self):
        return 'BEGIN'

    def connect(self):
        conn = psycopg2.connect(
            host=self.connection.host,
            database=self.connection.database,
            user=self.connection.user,
            password=self.connection.password)
        return conn
