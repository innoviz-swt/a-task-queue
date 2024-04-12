from abc import ABC, abstractmethod
from typing import Dict, Callable


class IHandler(ABC):
    @staticmethod
    @abstractmethod
    def from_interface_hanlders() -> Dict[type, Callable]:
        pass

    @staticmethod
    @abstractmethod
    def to_interface_hanlders() -> Dict[type, Callable]:
        pass
