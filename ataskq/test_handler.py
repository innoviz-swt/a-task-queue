# todo: add queries order_by tests

from pathlib import Path
from copy import copy
from datetime import datetime

import pytest

from .handler import from_connection_str, EAction
from .handler import Handler
from .db_handler import DBHandler
from .db_handler.db_handler import transaction_decorator
from .models import Task, StateKWArg, Job, EStatus
from .register import register_handler


@pytest.fixture
def handler(conn) -> Handler:
    handler = from_connection_str(conn)
    register_handler("test_handler", handler)
    return handler


class ForTestDBHandler:

    def __init__(self, handler):
        self._handler = handler
        self.error_msg = []

    def connect(self):
        return self._handler.connect()

    def __getattr__(self, name):
        return getattr(self._handler, name)

    @transaction_decorator()
    def invalid(self, c):
        c.execute("SOME INVALID TRANSACTION")


@pytest.mark.parametrize("conn_type", ["sqlite", "pg", "http"])
def test_conn_type_check(conn_type, conn):
    if f"{conn_type}://" not in conn:
        pytest.skip()


def test_invalid_transaction(jhandler):
    if not isinstance(jhandler, DBHandler):
        pytest.skip()
        return

    handler = ForTestDBHandler(jhandler)
    with pytest.raises(Exception) as excinfo:
        handler.invalid()
    assert "syntax error" in str(excinfo.value)


def test_db_format(conn, handler):
    assert isinstance(handler, Handler)

    if "sqlite" in conn:
        from .db_handler.sqlite3 import SQLite3DBHandler

        assert isinstance(handler, SQLite3DBHandler)
        assert "ataskq.db" in handler.db_path
    elif "pg" in conn:
        from .db_handler.postgresql import PostgresqlDBHandler

        assert isinstance(handler, PostgresqlDBHandler)
    elif "http" in conn:
        from .rest_handler import RESTHandler

        assert isinstance(handler, RESTHandler)
    else:
        raise Exception(f"unknown handler type in connection string '{conn}'")


def test_db_invalid_format_no_sep():
    with pytest.raises(RuntimeError) as excinfo:
        from_connection_str(conn=f"sqlite")
    assert "connection must be of format <type>://<connection string>" == str(excinfo.value)


def test_db_invalid_format_no_type():
    with pytest.raises(RuntimeError) as excinfo:
        from_connection_str(conn=f"://ataskq.db")
    assert "missing handler type, connection must be of format <type>://<connection string>" == str(excinfo.value)


def test_db_invalid_format_no_connectino():
    with pytest.raises(RuntimeError) as excinfo:
        from_connection_str(conn=f"sqlite://")
    assert "missing connection string, connection must be of format <type>://<connection string>" == str(excinfo.value)


# def test_query(conn, jhandler):
#     if not isinstance(jhandler, DBHandler):
#         pytest.skip()

#     db_handler: Handler = from_connection_str(conn=conn).create_job()
#     for q in EQueryType.__members__.values():
#         try:
#             db_handler.query(q)
#         except Exception as ex:
#             pytest.fail(f"query '{q}' failed, exception: {ex}")
