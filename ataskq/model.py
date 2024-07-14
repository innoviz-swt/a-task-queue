from copy import copy
from typing import Union, List, overload

from enum import Enum

from .imodel import *
from .handler import get_handler, Handler


class Model(IModel):
    @classmethod
    def id_key(cls):
        return cls.__name__.lower() + "_id"

    @classmethod
    def table_key(cls):
        return cls.__name__.lower() + "s"

    def __init__(self, **kwargs) -> None:
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
        kwargs = self._serialize(kwargs, dict())  # flag passed on constructor with no interface handlers
        # set kwargs as class members
        for k, v in kwargs.items():
            setattr(self, k, v)

    @classmethod
    def _serialize(cls, kwargs: dict, serializers: dict):
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
                assert isinstance(v, IModel), "relationship members must be Models instances."
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
            ann_serailizers = {k: v for k, v in serializers.items() if issubclass(ann, k)}
            if ann_serailizers:
                serializer = next(v for v in ann_serailizers.values())
                ann_name = f"serializers[{ann.__name__}]"
            else:
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
        ret = [cls(**r) for r in ret]

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
        ret = cls(**mkwargs)

        return ret

    @classmethod
    def create_bulk(cls, models: List[IModel], _handler: Handler = None) -> List[int]:
        if _handler is None:
            _handler = get_handler(assert_registered=True)
        if models is None:
            models = []

        mkwargs = [m.__dict__ for m in models]
        for i in range(len(mkwargs)):
            assert (
                mkwargs[i][cls.id_key()] is None
            ), f"id '{cls.id_key()}' can't be assigned when creating '{cls.__name__}({cls.table_key()})'"
            mkwargs[i] = copy(mkwargs[i])
            mkwargs[i].pop(cls.id_key())

        ikwargs = cls.m2i(mkwargs, serializer=_handler)
        ids = _handler.create_bulk(cls, ikwargs)

        for mid, m in zip(ids, models):
            setattr(m, cls.id_key(), mid)

        return models

    @classmethod
    def create(cls, _handler: Handler = None, **mkwargs):
        assert (
            cls.id_key() not in mkwargs
        ), f"id '{cls.id_key()}' can't be passed to create '{cls.__class__.__name__}({cls.table_key()})'"

        if _handler is None:
            _handler = get_handler(assert_registered=True)

        ikwargs = cls.m2i(mkwargs, _handler)
        model_id = _handler._create(cls, **ikwargs)

        return model_id

    def screate(self, _handler: Handler = None):
        assert (
            getattr(self, self.id_key()) is None
        ), f"id '{self.id_key()}' can't be assigned when creating '{self.__class__.__name__}({self.table_key()})'"
        mkwargs = {k: v for k, v in self.__dict__.items() if k in self.members()}

        model_id = self.create(_handler=_handler, **mkwargs)
        setattr(self, self.id_key(), model_id)

        return self

    def update(self, _handler: Handler = None, **mkwargs):
        if not mkwargs:
            assert (
                getattr(self, self.id_key()) is not None
            ), f"id '{self.id_key()}' must be assigned when updating '{self.__class__.__name__}({self.table_key()})'"
            mkwargs = {k: v for k, v in mkwargs.items() if k in self.__class__.members()}
        else:
            pk = [k for k in mkwargs.keys() if k in self.__class__.primary_keys()]
            assert len(pk) == 0, f"primary keys {pk} found in update mwargs."

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
