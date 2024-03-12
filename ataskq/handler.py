from abc import abstractmethod
from typing import Tuple, Union
from enum import Enum
from datetime import datetime

from .logger import Logger
from .models import Task
from .ihandler import IHandler


__STRTIME_FORMAT__ = "%Y-%m-%d %H:%M:%S.%f"


def to_datetime(string: Union[str, datetime, None]):
    if string is None:
        return None
    elif isinstance(string, datetime):
        return string

    return datetime.strptime(string, __STRTIME_FORMAT__)


def from_datetime(time: datetime):
    return time.strftime(__STRTIME_FORMAT__)


class EAction(str, Enum):
    RUN_TASK = "run_task"
    WAIT = "wait"
    STOP = "stop"


class Handler(IHandler, Logger):
    def __init__(self, logger: Logger = None):
        Logger.__init__(self, logger)

    @abstractmethod
    def _take_next_task(self, job_id, level: Union[int, None]) -> Tuple[EAction, Task]:
        pass


def from_connection_str(conn=None, **kwargs) -> Handler:
    if conn is None:
        conn = ""

    sep = "://"
    sep_index = conn.find(sep)
    if sep_index == -1:
        raise RuntimeError("connection must be of format <type>://<connection string>")
    handler_type = conn[:sep_index]

    # validate connectino
    if not handler_type:
        raise RuntimeError("missing handler type, connection must be of format <type>://<connection string>")

    connection_str = conn[sep_index + len(sep) :]
    if not connection_str:
        raise RuntimeError("missing connection string, connection must be of format <type>://<connection string>")

    # get db type handler
    if handler_type == "sqlite":
        from .db_handler.sqlite3 import SQLite3DBHandler

        handler = SQLite3DBHandler(conn, **kwargs)
        # register_ihandlers("sqlite", handler)
    elif handler_type == "pg":
        from .db_handler.postgresql import PostgresqlDBHandler

        handler = PostgresqlDBHandler(conn, **kwargs)
        # register_ihandlers("pg", handler)
    elif handler_type == "http" or handler_type == "https":
        from .rest_handler import RESTHandler

        handler = RESTHandler(conn, **kwargs)
        # register_ihandlers("http", handler)
    else:
        raise Exception(f"unsupported handler type '{handler_type}', type must be one of ['sqlite', 'pg', 'http']")

    return handler
