from abc import abstractmethod
from typing import Union, List, Dict
from datetime import datetime
from enum import Enum

from ..logger import Logger
from ..imodel import IModel, IModelSerializer

__STRTIME_FORMAT__ = "%Y-%m-%d %H:%M:%S.%f"


def get_query_kwargs(kwargs):
    _where = ""
    _order_by = ""
    if "_where" in kwargs:
        _where += kwargs["_where"]

    if "_order_by" in kwargs:
        _order_by += kwargs["_order_by"]

    # todo: the k=v for the kwargs should be interface dependent similar to insert
    for k, v in kwargs.items():
        if k in ["_where", "_order_by"]:
            continue
        _where += f"{_where and ' AND '}{k}={v}"

    _where = _where or None
    _order_by = _order_by or None

    ret = dict()
    if _where:
        ret["_where"] = _where
    if _order_by:
        ret["_order_by"] = _order_by

    ret["_offset"] = kwargs.get("_offset")
    ret["_limit"] = kwargs.get("_limit")

    return ret


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

    def __str__(self) -> str:
        return self.value


class Handler(IModelSerializer, Logger):
    def __init__(self, logger: Logger = None):
        Logger.__init__(self, logger)

    ######################
    # interface handlers #
    ######################

    @classmethod
    def i2m(cls, model_cls, kwargs: Union[dict, List[dict]]) -> Union[dict, List[dict]]:
        """interface to model"""
        return model_cls.i2m(kwargs, cls)

    @classmethod
    def from_interface(cls, model_cls: IModel, kwargs: Union[dict, List[dict]]) -> Union[IModel, List[IModel]]:
        return model_cls.from_interface(kwargs, cls)

    @classmethod
    def m2i(cls, model_cls: IModel, kwargs: Union[dict, List[dict]]) -> Union[dict, List[dict]]:
        """modle to interface"""
        return model_cls.m2i(kwargs, cls)

    @classmethod
    def to_interface(cls, model: IModel) -> IModel:
        return model.to_interface(cls)

    ########
    # CRUD #
    ########

    @abstractmethod
    def _create(self, model_cls: IModel, **ikwargs: dict):
        pass

    @abstractmethod
    def _create_bulk(self, model_cls: IModel, ikwargs: List[dict]):
        pass

    @abstractmethod
    def delete_all(self, model_cls: IModel, **kwargs):
        pass

    @abstractmethod
    def delete(self, model_cls: IModel, model_id: int):
        pass

    @abstractmethod
    def count_all(self, model_cls: IModel, **kwargs) -> int:
        pass

    @abstractmethod
    def get_all(self, model_cls: IModel, **kwargs) -> List[dict]:
        pass

    @abstractmethod
    def get(self, model_cls: IModel, model_id: int) -> dict:
        pass

    def create(self, model_cls: IModel, **mkwargs) -> int:
        assert (
            model_cls.id_key() not in mkwargs
        ), f"id '{model_cls.id_key()}' can't be passed to create '{model_cls.__name__}({model_cls.table_key()})'"
        ikwargs = self.m2i(model_cls, mkwargs)
        model_id = self._create(model_cls, **ikwargs)

        return model_id

    def create_bulk(self, model_cls: IModel, mkwargs: List[dict]) -> List[int]:
        for i, v in enumerate(mkwargs):
            assert (
                model_cls.id_key() not in v
            ), f"item [{i}]: id '{model_cls.id_key()}' can't be passed to create '{model_cls.__name__}({model_cls.table_key()})'"
        ikwargs = self.m2i(model_cls, mkwargs)
        model_ids = self._create_bulk(model_cls, ikwargs)

        return model_ids

    @abstractmethod
    def _update(self, model_cls: IModel, model_id: int, **ikwargs):
        pass

    @abstractmethod
    def update_all(self, model_cls: IModel, where: str = None, **ikwargs):
        pass

    def update(self, model_cls: IModel, model_id: int, **mkwargs):
        assert model_id is not None, f"{model_cls} must have assigned '{model_cls.id_key()}' for update"
        ikwargs = self.m2i(model_cls, mkwargs)
        self._update(model_cls, model_id, **ikwargs)

    ##########
    # Custom #
    ##########
    @abstractmethod
    def take_next_task(self, job_id, level: Union[int, None]) -> tuple:
        pass

    @abstractmethod
    def tasks_status(self, job_id) -> List[dict]:
        pass

    @abstractmethod
    def jobs_status(self) -> List[dict]:
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
        ), f"more than 1 type hander registered, please specify hanlder name. registered handlers: {list(__HANDLERS__.keys())}"
        assert (
            name in __HANDLERS__
        ), f"no handler named '{name}' is registered. registered handlers: {list(__HANDLERS__.keys())}"
        return __HANDLERS__[name]


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
        from .sqlite3 import SQLite3DBHandler

        handler = SQLite3DBHandler(conn, **kwargs)
    elif handler_type == "pg":
        from .postgresql import PostgresqlDBHandler

        handler = PostgresqlDBHandler(conn, **kwargs)
    elif handler_type == "http" or handler_type == "https":
        from .rest_handler import RESTHandler

        handler = RESTHandler(conn, **kwargs)
    else:
        raise Exception(f"unsupported handler type '{handler_type}', type must be one of ['sqlite', 'pg', 'http']")

    return handler
