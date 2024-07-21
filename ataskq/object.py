from typing import Any
from .model import Model, Str, Bytes, PrimaryKey
from .utils.dynamic_import import import_callable


def pickle_dict(**obj):
    ret = Object.serialize(obj, serializer="pickle.dumps", desrializer="pickle.loads")
    return ret


class Object(Model):
    object_id: PrimaryKey
    blob: Bytes
    serializer: Str
    desrializer: Str

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._cache = None

    def deserialize(self):
        if self._cache is not None:
            return self._cache

        deserializer_func = import_callable(self.desrializer)
        obj = deserializer_func(self.blob)
        self._cache = obj
        return obj

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        return self.deserialize()

    @staticmethod
    def serialize(obj, serializer="pickle.dumps", desrializer="pickle.loads"):
        if obj is None:
            return None
        serializer_func = import_callable(serializer)
        blob = serializer_func(obj)
        ret = Object(
            blob=blob,
            serializer=serializer,
            desrializer=desrializer,
        )

        return ret
