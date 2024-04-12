from abc import ABC, abstractmethod
from typing import Union, List
from .ihandler import IHandler


class IModel(ABC):
    @staticmethod
    @abstractmethod
    def id_key():
        pass

    @staticmethod
    @abstractmethod
    def table_key():
        pass

    @staticmethod
    def children():
        return dict()

    @staticmethod
    @abstractmethod
    def i2m(cls, kwargs: Union[dict, List[dict]], handler: IHandler) -> Union[dict, List[dict]]:
        pass

    @staticmethod
    @abstractmethod
    def from_interface(cls, kwargs: Union[dict, List[dict]], handler: IHandler):
        pass

    @staticmethod
    @abstractmethod
    def m2i(cls, kwargs: Union[dict, List[dict]], handler: IHandler) -> Union[dict, List[dict]]:
        pass

    @abstractmethod
    def to_interface(self, handler: IHandler) -> dict:
        pass
