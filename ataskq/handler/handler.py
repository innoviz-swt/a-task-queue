from abc import abstractmethod
from typing import Union, List, Dict
from datetime import datetime
from enum import Enum
import copy

from ..env import ATASKQ_CONFIG
from ..logger import Logger
from ..config import load_config
from ..imodel import IModel, IModelSerializer, DateTime

__STRTIME_FORMAT__ = "%Y-%m-%d %H:%M:%S.%f"


def get_query_kwargs(kwargs):
    # todo: the k=v for the kwargs should be interface dependent similar to insert
    ret = {}
    _where = ""
    if "_where" in kwargs:
        _where += kwargs["_where"]
    for k, v in kwargs.items():
        if k == "_where":
            continue
        if k in ["_group_by", "_order_by", "_limit", "_offset"]:
            ret[k] = v
            continue
        if v is None:
            continue
        _where += f"{_where and ' AND '}{k}={v}"

    _where = _where or None
    if _where:
        ret["_where"] = _where

    return ret


def to_datetime(val: Union[str, DateTime, datetime, None]):
    if val is None:
        return None
    elif isinstance(val, (DateTime)):
        return val
    elif isinstance(val, datetime):
        return DateTime.fromtimestamp(val.timestamp())

    ret = datetime.strptime(val, __STRTIME_FORMAT__)
    ret = DateTime.fromtimestamp(ret.timestamp())
    return ret


def from_datetime(time: DateTime):
    return time.strftime(__STRTIME_FORMAT__)


class EAction(str, Enum):
    RUN_TASK = "run_task"
    WAIT = "wait"
    STOP = "stop"

    def __str__(self) -> str:
        return self.value


class Handler(IModelSerializer, Logger):
    def __init__(self, config=ATASKQ_CONFIG, logger: Logger = None):
        Logger.__init__(self, logger)

        # init config
        self._config = load_config(config)
        self._connection = self.from_connection_str(self._config["connection"])

    @property
    def config(self):
        return self._config

    @staticmethod
    @abstractmethod
    def from_connection_str(conn):
        pass

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
    def delete(self, modl_cls: IModel, model_id: int):
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
        ret = self.create_bulk(model_cls, mkwargs)[0]
        return ret

    @staticmethod
    def members_attrs(model_cls: IModel, models: Union[IModel, List[IModel]], primary=False):
        if isinstance(models, list):
            is_list = True
        else:
            is_list = False
            models = [models]

        ret = [{k: v for k, v in m.__dict__.items() if k in model_cls.members(primary=primary)} for m in models]

        if not is_list:
            ret = ret[0]

        return ret

    def create_models_bulk(self, model_cls: IModel, models: List[IModel]) -> List[IModel]:
        models_attrs = self.members_attrs(model_cls, models)
        ikwargs = self.m2i(model_cls, models_attrs)
        model_ids = self._create_bulk(model_cls, ikwargs)
        for i, mi in enumerate(model_ids):
            setattr(models[i], model_cls.id_key(), mi)

        return models

    def create_dict_bulk(self, model_cls: IModel, models_attrs: List[dict]) -> List[int]:
        for i, v in enumerate(models_attrs):
            assert (
                model_cls.id_key() not in v
            ), f"item [{i}]: id '{model_cls.id_key()}' can't be passed to create '{model_cls.__name__}({model_cls.table_key()})'"
        interface_models_attrs = self.m2i(model_cls, models_attrs)
        model_ids = self._create_bulk(model_cls, interface_models_attrs)

        return model_ids

    def create_bulk(self, model_cls: IModel, models: Union[List[dict], List[IModel]]) -> List[int]:
        if not models:
            ret = []
        elif isinstance(models[0], IModel):
            ret = self.create_models_bulk(model_cls, models)
        elif isinstance(models[0], dict):
            ret = self.create_dict_bulk(model_cls, models)
        else:
            raise RuntimeError("Unexpected models type, expected List[IModel] or List[dict]")

        return ret

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
    def take_next_task(self, job_id=None, level_start: int = None, level_stop: int = None) -> tuple:
        pass

    @abstractmethod
    def tasks_status(self, job_id=None, _order_by: str = None, _limit: int = None, _offset: int = 0) -> List[dict]:
        pass

    @abstractmethod
    def jobs_status(self, _order_by: str = None, _limit: int = None, _offset: int = 0) -> List[dict]:
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


def from_config(config=ATASKQ_CONFIG, **kwargs) -> Handler:
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
        from .postgresql import PostgresqlDBHandler

        handler = PostgresqlDBHandler(**kwargs)
    elif handler_type == "http" or handler_type == "https":
        from .rest_handler import RESTHandler

        handler = RESTHandler(**kwargs)
    else:
        raise Exception(f"unsupported handler type '{handler_type}', type must be one of ['sqlite', 'pg', 'http']")

    return handler
