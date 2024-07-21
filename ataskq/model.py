from typing import Union, List, _GenericAlias

from enum import Enum

from .imodel import *
from .imodel import __DBFields__


class EState(Enum):
    NEW = 0
    Fetched = 1
    Modified = 2


class State:
    def __init__(self) -> None:
        self.columns = set()
        self.value = EState.NEW


class Model(IModel):
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
        ret = [ann for ann, klass in cls.__annotations__.items() if isinstance(getattr(cls, ann, None), Parent)]
        return ret

    @classmethod
    def childs(cls) -> str:
        ret = [ann for ann, klass in cls.__annotations__.items() if isinstance(getattr(cls, ann, None), Child)]
        return ret

    def __init__(self, **kwargs) -> None:
        self._state = State()
        cls_annotations = self.__annotations__

        # check a kwargs are class members
        for k in kwargs.keys():
            if k not in cls_annotations.keys():
                raise Exception(f"'{k}' not a possible class '{self.__class__.__name__}' member.")

        # set defaults
        for member in self.members(primary=True):
            # default None to members not passed
            if member not in kwargs:
                kwargs[member] = getattr(self.__class__, member, None)

        for rel in self.parents():
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
            ret = [cls(**kw) for kw in mkwargs]
        else:
            ret = cls(**mkwargs)

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
