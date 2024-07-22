from typing import Union, List, _GenericAlias
from datetime import datetime
from enum import Enum


class DBEnum(Enum):
    pass


class Int(int):
    pass


class Float(float):
    pass


class Str(str):
    pass


class DateTime(datetime):
    pass


class Bytes(bytes):
    pass


class PrimaryKey(int):
    pass


__DBFields__ = (Int, Float, Str, DateTime, Bytes)


class Parent:
    def __init__(self, key) -> None:
        self.key = key


class Child:
    def __init__(self, key) -> None:
        self.key = key


class EState(Enum):
    New = 0
    Fetched = 1
    Modified = 2
    Deleted = 3


class State:
    def __init__(self, value=EState.New) -> None:
        self.columns = set()
        self.value = value


class Model:
    _state: State

    @classmethod
    def id_key(cls):
        return cls.__name__.lower() + "_id"

    @property
    def id_val(self):
        return getattr(self, self.id_key())

    @classmethod
    def table_key(cls):
        return cls.__name__.lower() + "s"

    @classmethod
    def primary_keys(cls) -> str:
        ret = [
            ann
            for ann, klass in cls.__annotations__.items()
            if not isinstance(klass, _GenericAlias) and issubclass(klass, PrimaryKey)
        ]
        return ret

    @classmethod
    def members(cls, primary=False) -> str:
        if primary:
            ret = [
                ann
                for ann, klass in cls.__annotations__.items()
                if not isinstance(klass, _GenericAlias) and issubclass(klass, (*__DBFields__, PrimaryKey))
            ]
        else:
            ret = [
                ann
                for ann, klass in cls.__annotations__.items()
                if not isinstance(klass, _GenericAlias) and issubclass(klass, __DBFields__)
            ]
        return ret

    @classmethod
    def parents(cls) -> str:
        ret = [ann for ann in cls.__annotations__.keys() if isinstance(getattr(cls, ann, None), Parent)]
        return ret

    @classmethod
    def childs(cls) -> str:
        ret = [ann for ann in cls.__annotations__.keys() if isinstance(getattr(cls, ann, None), Child)]
        return ret

    def to_dict(self):
        ret = {getattr(self, k) for k in self.members()}

        return ret

    def __init__(self, **kwargs) -> None:
        self._state = State()
        cls_annotations = self.__annotations__

        # check a kwargs are class members
        for k in kwargs.keys():
            if k not in cls_annotations.keys():
                raise Exception(f"'{k}' is not annotated for class '{self.__class__.__name__}'.")

        # set defaults
        for member in self.members(primary=True):
            # default None to members not passed
            if member not in kwargs:
                kwargs[member] = getattr(self.__class__, member, None)

        for rel in self.parents():
            if rel not in kwargs:
                kwargs[rel] = None

        for rel in self.childs():
            if rel not in kwargs:
                kwargs[rel] = None

        # annotate kwargs
        kwargs = self._serialize(kwargs)  # flag passed on constructor with no interface handlers

        # set kwargs as class members
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __setattr__(self, name, value):
        if name not in self.members():
            super().__setattr__(name, value)
            return

        self._state.columns.add(name)
        if self._state.value == EState.Fetched:
            self._state.value = EState.Modified

        super().__setattr__(name, value)

    @classmethod
    def _serialize(cls, kwargs: dict):
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

            if k in cls.parents():
                # assert isinstance(v, IModel), "relationship members must be Models instances."
                ret[k] = v
                continue

            if k in cls.childs():
                # assert isinstance(v, IModel), "relationship members must be Models instances."
                ret[k] = v
                continue

            # get member annotation
            ann = cls_annotations[k]

            if isinstance(v, ann):
                ret[k] = v
                continue

            # handle union
            if getattr(ann, "__origin__", None) is Union:
                raise RuntimeError(f"{cls.__name__}::{k} Union type member not supported.")

            # serializer
            ann_name = None
            # check if already in relevant type
            if isinstance(v, ann):
                ret[k] = v
                continue

            ann_name = f"{ann.__name__}"
            serializer = ann

            try:
                ret[k] = serializer(v)
            except Exception as ex:
                raise Exception(f"{cls_name}::{k}({ann_name}) failed to serilize '{v}'({type(v).__name__})") from ex

        return ret
