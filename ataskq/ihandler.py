from abc import ABC, abstractmethod
from typing import Dict, Callable


class IHandler(ABC):
    @staticmethod
    @abstractmethod
    def i2m_serialize() -> Dict[type, Callable]:
        pass

    @staticmethod
    @abstractmethod
    def m2i_serialize() -> Dict[type, Callable]:
        pass
