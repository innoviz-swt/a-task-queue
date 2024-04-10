from abc import ABC, abstractmethod


class IModel(ABC):
    @staticmethod
    @abstractmethod
    def id_key():
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def table_key():
        raise NotImplementedError()

    @staticmethod
    def children():
        return dict()
