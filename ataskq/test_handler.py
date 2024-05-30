# todo: add queries order_by tests

import pytest

from .config import load_config
from .handler import Handler, from_config
from .handler.db_handler import DBHandler, transaction_decorator
from .handler import register_handler


@pytest.fixture
def handler(config) -> Handler:
    handler = from_config(config)
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


def test_invalid_transaction(handler):
    if not isinstance(handler, DBHandler):
        pytest.skip()

    handler = ForTestDBHandler(handler)
    with pytest.raises(Exception) as excinfo:
        handler.invalid()
    assert "syntax error" in str(excinfo.value)


def test_db_format(config, handler):
    assert isinstance(handler, Handler)
    conn = config["connection"]

    if "sqlite" in conn:
        from .handler.sqlite3 import SQLite3DBHandler

        assert isinstance(handler, SQLite3DBHandler)
        assert "ataskq.db" in handler.db_path
    elif "pg" in conn:
        from .handler.postgresql import PostgresqlDBHandler

        assert isinstance(handler, PostgresqlDBHandler)
    elif "http" in conn:
        from .handler.rest_handler import RESTHandler

        assert isinstance(handler, RESTHandler)
    else:
        raise Exception(f"unknown handler type in connection string '{conn}'")


def test_db_invalid_format_no_sep():
    with pytest.raises(RuntimeError) as excinfo:
        from_config(config=load_config({"connection": "sqlite"}, environ=False))
    assert "connection must be of format <type>://<connection string>" == str(excinfo.value)


def test_db_invalid_format_no_type():
    with pytest.raises(RuntimeError) as excinfo:
        from_config(load_config({"connection": f"://ataskq.db"}, environ=False))
    assert "missing handler type, connection must be of format <type>://<connection string>" == str(excinfo.value)


def test_db_invalid_format_no_connectino():
    with pytest.raises(RuntimeError) as excinfo:
        from_config(load_config({"connection": "sqlite://"}, environ=False))
    assert "missing connection string, connection must be of format <type>://<connection string>" == str(excinfo.value)
