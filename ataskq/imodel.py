from abc import ABC, abstractmethod
from datetime import datetime
from typing import Union, List, Dict, Callable
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


class IModelSerializer(ABC):
    @staticmethod
    @abstractmethod
    def i2m_serialize() -> Dict[type, Callable]:
        pass

    @staticmethod
    @abstractmethod
    def m2i_serialize() -> Dict[type, Callable]:
        pass


class IModel(ABC):
    @staticmethod
    @abstractmethod
    def i2m(cls, kwargs: Union[dict, List[dict]], serializer: IModelSerializer) -> Union[dict, List[dict]]:
        pass

    @staticmethod
    @abstractmethod
    def from_interface(cls, kwargs: Union[dict, List[dict]], serializer: IModelSerializer):
        pass

    @staticmethod
    @abstractmethod
    def m2i(cls, kwargs: Union[dict, List[dict]], serializer: IModelSerializer) -> Union[dict, List[dict]]:
        pass

    @abstractmethod
    def to_interface(self, serializer: IModelSerializer) -> dict:
        pass
