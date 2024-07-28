from abc import abstractmethod
from typing import Union, List, Dict, Tuple, Type, Set, _GenericAlias
from datetime import datetime
from enum import Enum

from ..env import CONFIG
from ..logger import Logger
from ..config import load_config

from ..model import Model, DateTime, Parent, Parents, Child, Children, State, EState
from ..models import Task

__STRTIME_FORMAT__ = "%Y-%m-%d %H:%M:%S.%f"


class Session:
    @abstractmethod
    def connect():
        pass

    @abstractmethod
    def close():
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


class Handler(Logger):
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

    ######################
    # interface handlers #
    ######################
    @staticmethod
    @abstractmethod
    def encode(obj, model_cls: Type[Model], key: str, key_cls):
        pass

    # Custom decoding function
    @staticmethod
    @abstractmethod
    def decode(obj, model_cls: Type[Model], key: str, key_cls):
        pass

    def _from_interface(self, model_cls: Type[Model], ikwargs: dict) -> Model:
        iret = dict()
        for k in ikwargs.keys():
            iret[k] = self.decode(ikwargs[k], model_cls, k, model_cls.__annotations__[k])

        ret = model_cls(**iret)

        ret._state = State(value=EState.Fetched)

        return ret

    def _to_interface(self, model: Model) -> dict:
        model_cls = model.__class__
        ret = dict()
        for k in model._state.columns:
            ret[k] = self.encode(getattr(model, k), model_cls, k, model_cls.__annotations__[k])

        # set state
        if model._state.value == EState.New:
            model._state = State(value=EState.Modified)
        elif model._state.value == EState.Fetched:
            raise RuntimeError(f"trying to push to db model in fetched state. {model}")
        elif model._state.value == EState.Modified:
            model._state = State(value=EState.Modified)
        else:
            raise RuntimeError(f"unsupported db state '{model._state}'")

        return ret

    ########
    # CRUD #
    ########

    @abstractmethod
    def _create(self, model: Model) -> int:
        pass

    @abstractmethod
    def _update(self, model: Model) -> int:
        pass

    def _add_parent(self, s, model, p_key, handled):
        if (parent := getattr(model, p_key)) is None:
            return

        parent: Model
        parent_mapping: Parent = getattr(model.__class__, p_key)
        p_id_key = parent_mapping.key
        if parent._state.value == EState.New:
            self._add(s, parent, handled)
            setattr(model, p_id_key, parent.id_val)
        else:
            assert getattr(model, p_id_key) == parent.id_val
            self._add(s, parent, handled)

    def _add_parents(self, s, model, p_key, handled):
        if (parents := getattr(model, p_key)) is None:
            return

        parents: List[Model]
        parents_mapping: Parents = getattr(model.__class__, p_key)
        p_id_key = parents_mapping.key

        for parent in parents:
            if parent._state.value == EState.New:
                self._add(s, parent, handled)
                setattr(model, p_id_key, parent.id_val)
            else:
                assert getattr(model, p_id_key) == parent.id_val
                self._add(s, parent, handled)

    def _add_child(self, s, model, c_key, handled):
        if (child := getattr(model, c_key)) is None:
            return

        child: Model
        child_mapping: Child = getattr(model.__class__, c_key)
        c_id_key = child_mapping.key

        if child._state.value == EState.New:
            setattr(child, c_id_key, model.id_val)
        else:
            assert getattr(child, c_id_key) == model.id_val
        self._add(s, child, handled)

    def _add_children(self, s, model, c_key, handled):
        if (children := getattr(model, c_key)) is None:
            return

        children: List[Model]
        child_mapping: Child = getattr(model.__class__, c_key)
        c_id_key = child_mapping.key

        for child in children:
            if child._state.value == EState.New:
                setattr(child, c_id_key, model.id_val)
            else:
                assert getattr(child, c_id_key) == model.id_val
            self._add(s, child, handled)

    def _add(self, s, model: Model, handled: Set[int]):
        # check if model already handled
        if id(model) in handled:
            return

        # handle parents
        for p_key in model.parent_keys():
            self._add_parent(s, model, p_key, handled)
        for p_key in model.parents_keys():
            self._add_parents(s, model, p_key, handled)

        if model._state.value == EState.New:
            self._create(s, model)
        elif model._state.value == EState.Modified and model._state.columns:
            self._update(s, model)

        # handle childs
        for c_key in model.child_keys():
            self._add_child(s, model, c_key, handled)
        for c_key in model.children_keys():
            self._add_children(s, model, c_key, handled)

        handled.add(id(model))

    def add(self, models: Union[Model, List[Model]]):
        handled = set()
        with self.session() as s:
            if not isinstance(models, list):
                models = [models]

            for model in models:
                self._add(s, model, handled)

    @abstractmethod
    def delete_all(self, model_cls: Type[Model], **kwargs):
        pass

    def delete(self, model: Model):
        self._delete(model)
        model._state = State(value=EState.Deleted)

    @abstractmethod
    def count_all(self, model_cls: Type[Model], **kwargs) -> int:
        pass

    @abstractmethod
    def _get_all(self, model_cls: Type[Model], **kwargs) -> List[dict]:
        pass

    # todo: pass session and use self._get(session,...)
    def _get_parent(self, p_key, model_cls, models):
        parent: Parent = getattr(model_cls, p_key)
        for model in models:
            parent_class = model.__annotations__[p_key]
            if (parent_id := getattr(model, parent.key)) is not None:
                rel_model = self.get(parent_class, parent_id)
                setattr(model, p_key, rel_model)

    def get_all(self, model_cls: Type[Model], relationships=None, **kwargs) -> List[Model]:
        assert issubclass(model_cls, Model)

        if relationships is None:
            relationships = []

        imodels = self._get_all(model_cls, **kwargs)
        models = [self._from_interface(model_cls, imodel) for imodel in imodels]

        for rel_key in relationships:
            if rel_key in model_cls.parent_keys():
                self._get_parent(rel_key, model_cls, models)
            elif rel_key in model_cls.parents_keys():
                raise NotImplementedError()
            elif rel_key in model_cls.child_keys():
                raise NotImplementedError()
            elif rel_key in model_cls.children_keys():
                raise NotImplementedError()
            else:
                raise RuntimeError(f"relationship '{model_cls.__name__}.{rel_key}' is not a relationship")

        return models

    @abstractmethod
    def _get(self, model_cls: Type[Model], model_id: int) -> dict:
        pass

    def get(self, model_cls: Type[Model], model_id: int) -> Model:
        iret = self._get(model_cls, model_id)
        ret = self._from_interface(model_cls, iret)

        return ret

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
