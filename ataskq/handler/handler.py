from abc import abstractmethod
from typing import Union, List, Dict
from datetime import datetime
from enum import Enum

from ..env import CONFIG
from ..logger import Logger
from ..config import load_config

from ..model import Model, DateTime, Parent, EState

__STRTIME_FORMAT__ = "%Y-%m-%d %H:%M:%S.%f"


def get_query_kwargs(kwargs):
    # todo: the k=v for the kwargs should be interface dependent similar to insert
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


class Handler(Logger):
    def __init__(self, config=CONFIG, logger: Logger = None):
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
    def from_interface(cls, model_cls: Model, ikwargs: dict) -> Model:
        for k in ikwargs.keys():
            ikwargs[k] = cls.encode(ikwargs[k], model_cls, k, model_cls.__annotations__[k])

        ret = model_cls(**ikwargs)
        ret._state.value = EState.Fetched

        return ret

    @classmethod
    def to_interface(cls, model: Model) -> dict:
        model_cls = model.__class__
        ret = dict()
        for k in model.members():
            ret[k] = cls.encode(getattr(model, k), model_cls, k, model_cls.__annotations__[k])

        if model.state.value == EState.NEW:
            ret._state.value = EState.Fetched
        elif model._state.value == EState.Fetched:
            raise RuntimeError(f"trying to push to db model in fetched state. {model}")
        elif model._state.value == EState.Modified:
            pass
        else:
            raise RuntimeError(f"unsupported db state '{model._state}'")

        return ret

    ########
    # CRUD #
    ########

    def _create(self, model: Model) -> int:
        pass

    def _update(self, model: Model) -> int:
        pass

    @abstractmethod
    def delete_all(self, model_cls: Model, **kwargs):
        pass

    @abstractmethod
    def delete(self, modl_cls: Model, model_id: int):
        pass

    @abstractmethod
    def count_all(self, model_cls: Model, **kwargs) -> int:
        pass

    @abstractmethod
    def _get_all(self, model_cls: Model, **kwargs) -> List[dict]:
        pass

    def get_all(self, model_cls: Model, **kwargs) -> List[Model]:
        iret = self._get_all(model_cls, **kwargs)
        ret = [self.from_interface(model_cls, ir) for ir in iret]

        return ret

    @abstractmethod
    def _get(self, model_cls: Model, model_id: int) -> dict:
        pass

    def get(self, model_cls: Model, model_id: int) -> Model:
        iret = self._get(model_cls, model_id)
        ret = self.from_interface(model_cls, iret)

        return ret

    @staticmethod
    def members_attrs(model_cls: Model, models: Union[Model, List[Model]], primary=False):
        if isinstance(models, list):
            is_list = True
        else:
            is_list = False
            models = [models]

        ret = [{k: v for k, v in m.__dict__.items() if k in model_cls.members(primary=primary)} for m in models]

        if not is_list:
            ret = ret[0]

        return ret

    def create_models_bulk(self, model_cls: Model, models: List[Model]) -> List[Model]:
        # parents mapping
        parents = {}
        for mi, m in enumerate(models):
            for p_key in m.parents():
                if (parent := getattr(m, p_key)) is not None:
                    # parent create is required
                    parent_mapping: Parent = getattr(m.__class__, p_key)
                    p_id_key = parent_mapping.key
                    assert getattr(m, p_id_key) is None, ""
                    parent_class = m.__annotations__[p_key]
                    if parent_class not in parents:
                        parents[parent_class] = {"models": [], "indices": [], "id_keys": []}
                    parents[parent_class]["models"].append(parent)
                    parents[parent_class]["indices"].append(mi)
                    parents[parent_class]["id_keys"].append(p_id_key)

        # parents bulk create
        for p_cls, parent_models_data in parents.items():
            parent_models = parent_models_data["models"]
            self.create_bulk(p_cls, parent_models)
            for p_m, p_i, p_id_key in zip(
                parent_models_data["models"], parent_models_data["indices"], parent_models_data["id_keys"]
            ):
                setattr(models[p_i], p_id_key, getattr(p_m, p_m.id_key()))

        # model create
        models_attrs = self.members_attrs(model_cls, models)
        ikwargs = self.m2i(model_cls, models_attrs)
        model_ids = self._create_bulk(model_cls, ikwargs)
        for i, mi in enumerate(model_ids):
            setattr(models[i], model_cls.id_key(), mi)

        return models

    @abstractmethod
    def update_all(self, model_cls: Model, where: str = None, **ikwargs):
        pass

    ##########
    # Custom #
    ##########
    @abstractmethod
    def take_next_task(self, job_id=None, level_start: int = None, level_stop: int = None) -> tuple:
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
        from .postgresql import PostgresqlDBHandler

        handler = PostgresqlDBHandler(**kwargs)
    elif handler_type == "http" or handler_type == "https":
        from .rest_handler import RESTHandler

        handler = RESTHandler(**kwargs)
    else:
        raise Exception(f"unsupported handler type '{handler_type}', type must be one of ['sqlite', 'pg', 'http']")

    return handler
