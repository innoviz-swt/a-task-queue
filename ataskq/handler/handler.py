from abc import ABC, abstractmethod
from typing import Union, List, Dict, Tuple, Type
from datetime import datetime
from enum import Enum

from ..env import CONFIG
from ..logger import Logger
from ..config import load_config

from ..model import Model, DateTime, Parent, State, EState
from ..models import Task

__STRTIME_FORMAT__ = "%Y-%m-%d %H:%M:%S.%f"


class Session:
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def close(self):
        pass

    def __enter__(self):
        # make a database connection and return it
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # make sure the dbconnection gets closed
        self.close()


def get_query_kwargs(kwargs):
    ret = {}
    where = ""
    if "where" in kwargs:
        where += kwargs["where"]
    for k, v in kwargs.items():
        if k == "where":
            continue
        if k in ["group_by", "order_by", "limit", "offset"]:
            ret[k] = v
            continue
        if v is None:
            continue
        where += f"{where and ' AND '}{k}={v}"

    where = where or None
    if where:
        ret["where"] = where

    return ret


def to_datetime(val: str) -> datetime:
    ret = datetime.strptime(val, __STRTIME_FORMAT__)
    ret = DateTime.fromtimestamp(ret.timestamp())
    return ret


def from_datetime(time: DateTime) -> str:
    return time.strftime(__STRTIME_FORMAT__)


class EAction(str, Enum):
    RUN_TASK = "run_task"
    WAIT = "wait"
    STOP = "stop"

    def __str__(self) -> str:
        return self.value


class Handler(ABC, Logger):
    def __init__(self, config=CONFIG, logger: Logger = None):
        Logger.__init__(self, logger)

        # init config
        self._config = load_config(config)
        self._connection = self.from_connection_str(self._config["connection"])

    @property
    def config(self):
        return self._config

    def session(exclusive=False):
        pass

    @staticmethod
    @abstractmethod
    def from_connection_str(conn):
        pass

    ########
    # CRUD #
    ########

    @abstractmethod
    def add(self, models: Union[Model, List[Model]]):
        pass

    @abstractmethod
    def delete_all(self, model_cls: Type[Model], **kwargs):
        pass

    @abstractmethod
    def delete(self, model: Model):
        pass

    @abstractmethod
    def count_all(self, model_cls: Type[Model], **kwargs) -> int:
        pass

    @abstractmethod
    def get_all(self, model_cls: Type[Model], relationships=None, **kwargs) -> List[Model]:
        pass

    @abstractmethod
    def get(self, model_cls: Type[Model], model_id: int) -> Model:
        pass

    @abstractmethod
    def update_all(self, model_cls: Type[Model], where: str = None, **ikwargs):
        pass

    ##########
    # Custom #
    ##########
    @abstractmethod
    def take_next_task(self, job_id=None, level_start: int = None, level_stop: int = None) -> Tuple[EAction, Task]:
        pass

    @abstractmethod
    def tasks_status(self, job_id=None, order_by: str = None, limit: int = None, offset: int = 0) -> List[dict]:
        pass

    @abstractmethod
    def jobs_status(self, order_by: str = None, limit: int = None, offset: int = 0) -> List[dict]:
        pass


__HANDLERS__: Dict[str, object] = dict()


def register_handler(name, handler: Handler):
    """register interface handlers"""
    __HANDLERS__[name] = handler


def unregister_handler(name):
    """register interface handlers"""
    return __HANDLERS__.pop(name)


def get_handler(name=None, assert_registered=False):
    """get registered interface handlers"""

    if len(__HANDLERS__) == 0:
        if assert_registered:
            raise RuntimeError("No registered interface handlers")
        return None
    elif len(__HANDLERS__) == 1:
        return list(__HANDLERS__.values())[0]
    else:
        assert (
            name is not None
        ), f"more than 1 type hander registered, please specify handler name. registered handlers: {list(__HANDLERS__.keys())}"
        assert (
            name in __HANDLERS__
        ), f"no handler named '{name}' is registered. registered handlers: {list(__HANDLERS__.keys())}"
        return __HANDLERS__[name]


def from_config(config=CONFIG, **kwargs) -> Handler:
    # expand config in factory and not inside handler
    config = load_config(config)
    conn = config["connection"]

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
    kwargs["config"] = config
    if handler_type == "sqlite":
        from .sqlite3 import SQLite3DBHandler

        handler = SQLite3DBHandler(**kwargs)
    elif handler_type == "pg":
        from .postgresql import PostgreSQLHandler

        handler = PostgreSQLHandler(**kwargs)
    elif handler_type == "http" or handler_type == "https":
        from .rest_handler import RESTHandler

        handler = RESTHandler(**kwargs)
    else:
        raise Exception(f"unsupported handler type '{handler_type}', type must be one of ['sqlite', 'pg', 'http']")

    return handler
