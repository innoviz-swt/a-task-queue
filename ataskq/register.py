from abc import ABC, abstractmethod
from typing import Union, Callable, List, Dict
from .imodel import IModel


class IHandler(ABC):
    @staticmethod
    @abstractmethod
    def from_interface_hanlders() -> Dict[type, Callable]:
        pass

    @staticmethod
    @abstractmethod
    def to_interface_hanlders() -> Dict[type, Callable]:
        pass

    @classmethod
    def i2m(cls, model_cls, kwargs: Union[dict, List[dict]]) -> Union[dict, List[dict]]:
        """interface to model"""
        return model_cls.i2m(kwargs, cls.from_interface_hanlders())

    @classmethod
    def from_interface(cls, model_cls: IModel, kwargs: Union[dict, List[dict]]) -> Union[IModel, List[IModel]]:
        return model_cls.from_interface(kwargs, cls.from_interface_hanlders())

    @classmethod
    def m2i(cls, model_cls: IModel, kwargs: Union[dict, List[dict]]) -> Union[dict, List[dict]]:
        """modle to interface"""
        return model_cls.m2i(kwargs, cls.to_interface_hanlders())

    @classmethod
    def to_interface(cls, model: IModel) -> IModel:
        return model.to_interface(cls.to_interface_hanlders())

    @abstractmethod
    def _create(self, model_cls: IModel, **ikwargs: dict):
        pass

    @abstractmethod
    def delete(self, model_cls: IModel, model_id: int):
        pass

    @abstractmethod
    def get(self, model_cls: IModel, model_id: int) -> IModel:
        pass

    @abstractmethod
    def get_all(self, model_cls: IModel, model_id: int) -> List[IModel]:
        pass

    def create(self, model_cls: IModel, **mkwargs):
        assert model_cls.id_key() not in mkwargs, \
            f"id '{model_cls.id_key()}' can't be passed to create '{model_cls.__name__}({model_cls.table_key()})'"
        ikwargs = self.m2i(model_cls, mkwargs)
        model_id = self._create(model_cls, **ikwargs)

        return model_id

    @abstractmethod
    def delete(self, model_cls: IModel, model_id: int):
        pass

    @abstractmethod
    def _update(self, model_cls: IModel, model_id: int, **ikwargs):
        pass

    def update(self, model_cls: IModel, model_id: int, **mkwargs):
        assert model_id is not None, f"{model_cls} must have assigned '{model_cls.id_key()}' for update"
        ikwargs = self.m2i(model_cls, mkwargs)
        self._update(model_cls, model_id, **ikwargs)


__HANDLERS__: Dict[str, object] = dict()


def register_ihandlers(name, handler: IHandler):
    """register interface handlers"""
    __HANDLERS__[name] = handler


def get_handlers(name=None):
    """get registered interface handlers"""

    if len(__HANDLERS__) == 0:
        return None
    elif len(__HANDLERS__) == 1:
        return list(__HANDLERS__.values())[0]
    else:
        assert name is not None, f"more than 1 type hander registered, please specify hanlder name. registered handlers: {list(__HANDLERS__.keys())}"
        assert name in __HANDLERS__, f"no handler named '{name}' is registered. registered handlers: {list(__HANDLERS__.keys())}"
        return __HANDLERS__[name]
