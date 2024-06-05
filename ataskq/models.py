from typing import Union, List, Dict
from enum import Enum
import pickle
from importlib import import_module
from datetime import datetime
from copy import copy

from .imodel import IModel, IModelSerializer
from .handler import get_handler, Handler


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

    def __str__(self) -> str:
        return self.value


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
    def i2m(cls, kwargs: Union[dict, List[dict]], serializer: IModelSerializer) -> Union[dict, List[dict]]:
        """interface to model"""
        if isinstance(kwargs, list):
            ret = [cls._serialize(kw, serializer.i2m_serialize()) for kw in kwargs]
        else:
            ret = cls._serialize(kwargs, serializer.i2m_serialize())

        return ret

    @classmethod
    def from_interface(cls, kwargs: Union[dict, List[dict]], serializer: IModelSerializer):
        """interface to model"""
        mkwargs = cls.i2m(kwargs, serializer)
        if isinstance(kwargs, list):
            ret = [cls(_serialize=False, **kw) for kw in mkwargs]
        else:
            ret = cls(_serialize=False, **mkwargs)

        return ret

    @classmethod
    def m2i(cls, kwargs: Union[dict, List[dict]], serializer: IModelSerializer) -> Union[dict, List[dict]]:
        """model to interface"""
        if isinstance(kwargs, list):
            ret = [cls._serialize(kw, serializer.m2i_serialize()) for kw in kwargs]
        else:
            ret = cls._serialize(kwargs, serializer.m2i_serialize())

        return ret

    def to_interface(self, serializer: IModelSerializer) -> dict:
        """model to interface"""
        ret = self.m2i(self.__dict__, serializer)

        return ret

    @classmethod
    def count_all(cls, _handler: Handler = None, **kwargs):
        if _handler is None:
            _handler = get_handler(assert_registered=True)

        ret = _handler.count_all(cls, **kwargs)
        return ret

    @classmethod
    def get_all_dict(cls, _handler: Handler = None, **kwargs):
        if _handler is None:
            _handler = get_handler(assert_registered=True)

        ret = _handler.get_all(cls, **kwargs)
        ret = cls.i2m(ret, _handler)

        return ret

    @classmethod
    def get_all(cls, _handler: Handler = None, **kwargs):
        ret = cls.get_all_dict(_handler=_handler, **kwargs)
        ret = [cls(**r, _serialize=False) for r in ret]

        return ret

    @classmethod
    def get_dict(cls, model_id: int, _handler: Handler = None):
        if _handler is None:
            _handler = get_handler(assert_registered=True)

        ikwargs = _handler.get(cls, model_id)
        mkwargs = cls.i2m(ikwargs, _handler)

        return mkwargs

    @classmethod
    def get(cls, model_id: int, _handler: Handler = None):
        mkwargs = cls.get_dict(model_id, _handler)
        ret = cls(**mkwargs, _serialize=False)

        return ret

    def create(self, _handler: Handler = None, **mkwargs):
        if not mkwargs:
            assert (
                getattr(self, self.id_key()) is None
            ), f"id '{self.id_key()}' can't be assigned when creating '{self.__class__.__name__}({self.table_key()})'"
            mkwargs = copy(self.__dict__)
            mkwargs.pop(self.id_key())

        assert (
            self.id_key() not in mkwargs
        ), f"id '{self.id_key()}' can't be passed to create '{self.__class__.__name__}({self.table_key()})'"

        if _handler is None:
            _handler = get_handler(assert_registered=True)

        ikwargs = self.m2i(mkwargs, _handler)
        model_id = _handler._create(self.__class__, **ikwargs)

        setattr(self, self.id_key(), model_id)

        return self

    def update(self, _handler: Handler = None, **mkwargs):
        if not mkwargs:
            assert (
                getattr(self, self.id_key()) is not None
            ), f"id '{self.id_key()}' must be assigned when updating '{self.__class__.__name__}({self.table_key()})'"
            mkwargs = copy(self.__dict__)
            mkwargs.pop(self.id_key())

        assert (
            self.id_key() not in mkwargs
        ), f"id '{self.id_key()}' can't be passed to update '{self.__class__.__name__}({self.table_key()})'"

        if _handler is None:
            _handler = get_handler(assert_registered=True)

        model_id = getattr(self, self.id_key())
        ikwargs = self.m2i(mkwargs, _handler)
        _handler._update(self.__class__, model_id, **ikwargs)

        for k, v in mkwargs.items():
            setattr(self, k, v)

        return self

    @classmethod
    def delete_all(cls, _handler: Handler = None, **kwargs):
        if _handler is None:
            _handler = get_handler(assert_registered=True)

        ret = _handler.delete_all(cls, **kwargs)
        return ret

    def delete(self, _handler: Handler = None):
        model_id = getattr(self, self.id_key())
        assert (
            model_id is not None
        ), f"id '{self.__class__.__name__}({self.table_key()} -> {self.id_key()})' required for delete"

        if _handler is None:
            _handler = get_handler(assert_registered=True)

        _handler.delete(self.__class__, model_id)
        setattr(self, self.id_key(), None)

        return self

    def add_children(
        self,
        child_cls: IModel,
        children: Union[Union[IModel, dict], List[Union[IModel, dict]]],
        _handler: Handler = None,
    ):
        if not children:
            return

        if not isinstance(children, list):
            children = [children]

        assert child_cls in self.children(), f"no children association defined for '{child_cls}'"
        parent_key = self.children()[child_cls]
        parent_key_val = getattr(self, self.id_key())

        children_mkwargs = []
        for i, c in enumerate(children):
            if isinstance(c, child_cls):
                assert (
                    getattr(c, c.id_key()) is None
                ), f"id '{child_cls.id_key()}' can't be assigned when creating '{child_cls.__name__}({child_cls.table_key()})'"
                mkwargs = copy(c.__dict__)
                mkwargs.pop(c.id_key())
            elif isinstance(c, dict):
                mkwargs = c
            else:
                raise Exception(f"item [{i}]: Unsupported child type '{type(c)}'")

            # assign parent key to child
            assert (
                mkwargs.get(parent_key) is None
            ), f"child '{child_cls.__name__}[{i}]' can't have parent key '{self.__class__.__name__}->{parent_key}' assigned"
            mkwargs[parent_key] = parent_key_val
            children_mkwargs.append(mkwargs)

        if _handler is None:
            _handler = get_handler(assert_registered=True)

        child_ids = _handler.create_bulk(child_cls, children_mkwargs)
        for cid, c in zip(child_ids, children):
            setattr(c, parent_key, parent_key_val)
            setattr(c, child_cls.id_key(), cid)

        return children

    def get_children_dict(self, child_cls: IModel, _handler: Handler = None):
        assert child_cls in self.children(), f"no children association defined for '{child_cls}'"
        parent_key = self.children()[child_cls]
        primary_key_val = getattr(self, self.id_key())

        if _handler is None:
            _handler = get_handler(assert_registered=True)

        ikwargs = _handler.get_all(child_cls, **{f"{parent_key}": primary_key_val})
        mkwargs = child_cls.i2m(ikwargs, _handler)

        return mkwargs

    def get_children(self, child_cls: IModel, _handler: Handler = None):
        mkwargs = self.get_children_dict(child_cls, _handler)
        ret = [child_cls(**kw, _serialize=False) for kw in mkwargs]

        return ret


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

    def __str__(self):
        return f"{self.name}({self.task_id})" if self.name else f"{self.task_id}"


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

    @staticmethod
    def children():
        return {
            Task: "job_id",
        }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_tasks(self, _handler=None) -> List[Task]:
        return self.get_children(Task, _handler=_handler)

    def add_tasks(self, tasks: List[Task], _handler=None):
        return self.add_children(Task, tasks, _handler=_handler)


__MODELS__: Dict[str, Model] = {m.table_key(): m for m in [Task, Job]}
