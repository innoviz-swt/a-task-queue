from typing import Union, List, Dict
from enum import Enum
import pickle
from importlib import import_module
from datetime import datetime
from abc import abstractmethod
from copy import copy

from .imodel import IModel
from .register import get_handlers, IHandler


class EntryPointRuntimeError(RuntimeError):
    pass


class TARGSLoadRuntimeError(EntryPointRuntimeError):
    pass


class EntrypointLoadRuntimeError(EntryPointRuntimeError):
    pass


class EntrypointCallRuntimeError(EntryPointRuntimeError):
    pass


class EStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILURE = "failure"


class EntryPoint:
    @staticmethod
    def init(kwargs) -> None:
        entrypoint = kwargs.get("entrypoint")
        if callable(entrypoint):
            kwargs["entrypoint"] = f"{entrypoint.__module__}.{entrypoint.__name__}"

        targs = kwargs.get("targs")
        if targs is not None and isinstance(targs, tuple):
            assert len(targs) == 2
            assert isinstance(targs[0], tuple)
            assert isinstance(targs[1], dict)
            kwargs["targs"] = pickle.dumps(targs)

    def get_targs(self):
        if self.targs is not None:
            try:
                targs = pickle.loads(self.targs)
                assert len(targs) == 2, "targs must be tuple of 2 elements"
                assert isinstance(targs[0], tuple), "targs[0] must be args tuple"
                assert isinstance(targs[1], dict), "targs[0] must be kwargs dict"
            except Exception as ex:
                raise TARGSLoadRuntimeError() from ex

        else:
            targs = ((), {})

        return targs[0], targs[1]

    def get_entrypoint(self):
        ep = self.entrypoint

        try:
            assert "." in ep, "entry point must be inside a module."
            module_name, func_name = ep.rsplit(".", 1)
            m = import_module(module_name)
            assert hasattr(
                m, func_name
            ), f"failed to load entry point, module '{module_name}' doen't have func named '{func_name}'."
            func = getattr(m, func_name)
            assert callable(func), f"entry point is not callable, '{module_name}.{func}'."
        except ImportError as ex:
            raise EntrypointLoadRuntimeError(f"Failed to load module '{module_name}'. Exception: '{ex}'")
        except Exception as ex:
            raise EntrypointLoadRuntimeError(f"Failed to load entry point '{ep}'. Exception: '{ex}'") from ex

        return func

    def call(self):
        args, kwargs = self.get_targs()
        entrypoint = self.get_entrypoint()

        try:
            ret = entrypoint(*args, **kwargs)
        except Exception as ex:
            raise EntrypointCallRuntimeError(
                f"Failed while call entrypoint function '{self.entrypoint}'. Exception: '{ex}'"
            ) from ex

        return ret


def _handle_union(cls_name, member, annotations, value, type_handlers=None):
    if type_handlers is None:
        type_handlers = dict()

    # check if value is of supported types
    for ann in annotations:
        if isinstance(value, ann):
            return value

    # attemp cast value
    success = False
    value = None
    for ann in annotations:
        try:
            if ann in type_handlers:
                value = type_handlers[ann](value)
            else:
                value = ann(value)
            success = True
            break
        except Exception:
            continue

    if not success:
        raise Exception(f"{cls_name}::{member}({annotations}) failed casting {type(value)} - '{value}'.")

    return value


class Model(IModel):
    def __init__(self, _serialize=True, **kwargs) -> None:
        cls_annotations = self.__annotations__
        defaults = getattr(self, "__DEFAULTS__", dict())

        # check a kwargs are class members
        for k in kwargs.keys():
            if k not in cls_annotations.keys():
                raise Exception(f"'{k}' not a possible class '{self.__class__.__name__}' member.")

        # set defaults
        for member in cls_annotations.keys():
            # default None to members not passed
            if member not in kwargs:
                kwargs[member] = defaults.get(member)

        # annotate kwargs
        if _serialize:
            kwargs = self._serialize(kwargs, dict())  # flag passed on constructor with no interface handlers

        # set kwargs as class members
        for k, v in kwargs.items():
            setattr(self, k, v)

    @classmethod
    def _serialize(cls, kwargs: dict, type_handlers: dict):
        ret = dict()
        cls_annotations = cls.__annotations__
        cls_name = cls.__name__
        for k, v in kwargs.items():
            if k not in cls_annotations:
                raise Exception(f"interface key '{k}' not in model annotations.")

            # allow None values (no handling)
            if v is None:
                ret[k] = None
                continue

            # get member annotation
            annotation = cls_annotations[k]

            # handle union
            if getattr(annotation, "__origin__", None) is Union:
                ret[k] = _handle_union(cls_name, k, annotation.__args__, v, type_handlers)
                continue

            # Single annotation cast
            ann = cls_annotations[k]

            ann_name = None
            if ann in type_handlers:
                ann_name = f"type_handler[{ann.__name__}]"
                ann = type_handlers[ann]
            elif issubclass(ann, str) and str in type_handlers:
                # string subclasses
                ann_name = f"type_handler[{ann.__name__} - str sublcass]"
                ann = type_handlers[str]
            elif issubclass(ann, Enum) and Enum in type_handlers:
                # string subclasses
                ann_name = f"type_handler[{ann.__name__} - Enum sublcass]"
                ann = type_handlers[Enum]
            else:
                # check if already in relevant type
                if isinstance(v, ann):
                    ret[k] = v
                    continue

                ann_name = f"{ann.__name__}"
                ann = ann

            try:
                ret[k] = ann(v)
            except Exception as ex:
                raise Exception(f"{cls_name}::{k}({ann_name}) failed cast from '{v}'({type(v).__name__})") from ex

        return ret

    @classmethod
    def i2m(cls, kwargs: Union[dict, List[dict]], type_handlers) -> Union[dict, List[dict]]:
        """interface to model"""
        if isinstance(kwargs, list):
            ret = [cls._serialize(kw, type_handlers) for kw in kwargs]
        else:
            ret = cls._serialize(kwargs, type_handlers)

        return ret

    @classmethod
    def from_interface(cls, kwargs: Union[dict, List[dict]], type_handlers):
        """interface to model"""
        mkwargs = cls.i2m(kwargs, type_handlers)
        if isinstance(kwargs, list):
            ret = [cls(_serialize=False, **kw) for kw in mkwargs]
        else:
            ret = cls(_serialize=False, **mkwargs)

        return ret

    @classmethod
    def m2i(cls, kwargs: Union[dict, List[dict]], type_handlers) -> Union[dict, List[dict]]:
        """model to interface"""
        if isinstance(kwargs, list):
            ret = [cls._serialize(kw, type_handlers) for kw in kwargs]
        else:
            ret = cls._serialize(kwargs, type_handlers)

        return ret

    def to_interface(self, type_handlers) -> dict:
        """model to interface"""
        ret = self.m2i(self.__dict__, type_handlers)

        return ret

    @classmethod
    def get_all_dict(cls, _handler: IHandler = None):
        if _handler is None:
            _handler = get_handlers()

        ret = _handler.get_all(cls)
        ret = self.i2m(model_cls, ret)

        return ret

    @classmethod
    def get_dict(cls, model_id: int, _handler: IHandler = None):
        if _handler is None:
            _handler = get_handlers()

        ret = _handler.get(model_id, cls)
        ret = self.i2m(model_cls, ret)

        return ret

    def create(self, _handler: IHandler = None, **mkwargs):
        assert (
            self.id_key() not in mkwargs
        ), f"id '{self.id_key()}' can't be passed to create '{self.__class__.__name__}({self.table_key()})'"

        if _handler is None:
            _handler = get_handlers()

        if not mkwargs:
            mkwargs = copy(self.__dict__)
            mkwargs.pop(self.id_key())

        ikwargs = self.m2i(mkwargs, _handler.to_interface_hanlders())
        model_id = _handler._create(self.__class__, **ikwargs)

        setattr(self, self.id_key(), model_id)

        return self


class Task(Model, EntryPoint):
    task_id: int
    name: str
    level: float
    entrypoint: str
    targs: bytes
    status: EStatus
    take_time: datetime
    start_time: datetime
    done_time: datetime
    pulse_time: datetime
    description: str
    # summary_cookie = None,
    job_id: int

    __DEFAULTS__ = dict(status=EStatus.PENDING, entrypoint="", level=0.0)

    @staticmethod
    def id_key():
        return "task_id"

    @staticmethod
    def table_key():
        return "tasks"

    def __init__(self, **kwargs) -> None:
        EntryPoint.init(kwargs)
        Model.__init__(self, **kwargs)


class StateKWArg(Model, EntryPoint):
    state_kwargs_id: int
    name: str
    entrypoint: str
    targs: bytes
    description: str
    job_id: int

    @staticmethod
    def id_key():
        return "state_kwargs_id"

    @staticmethod
    def table_key():
        return "state_kwargs"

    def __init__(self, **kwargs) -> None:
        EntryPoint.init(kwargs)
        Model.__init__(self, **kwargs)


class Job(Model):
    job_id: int
    name: str
    priority: float
    description: str

    @staticmethod
    def id_key():
        return "job_id"

    @staticmethod
    def table_key():
        return "jobs"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


__MODELS__: Dict[str, Model] = {m.table_key(): m for m in [Task, StateKWArg, Job]}
