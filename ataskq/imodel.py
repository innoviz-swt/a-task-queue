from abc import ABC, abstractmethod
from typing import Union, List, Dict, Callable


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
