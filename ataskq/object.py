from typing import Any
from .model import Model, Str, Bytes, PrimaryKey
from .utils.dynamic_import import import_callable


def pickle_iter(*obj):
    ret = Object.serialize(obj, encoder="pickle")
    return ret


def pickle_dict(**obj):
    ret = Object.serialize(obj, encoder="pickle")
    return ret


DEFAULT_ENCODER = "pickle"
ENCODE_MAP = {
    "pickle": ("pickle.dumps", "pickle.loads"),
    "json": ("json.dumps", "json.loads"),
}


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

    def __call__(self) -> Any:
        return self.deserialize()

    @staticmethod
    def serialize(obj, encoder=DEFAULT_ENCODER, decoder=None):
        if obj is None:
            return None

        if encoder is None and decoder is None:
            encoder = ENCODE_MAP[DEFAULT_ENCODER]
        elif encoder is not None and decoder is None:
            encoder, decoder = ENCODE_MAP[encoder]

        serializer_func = import_callable(encoder)
        blob = serializer_func(obj)
        ret = Object(
            blob=blob,
            serializer=encoder,
            desrializer=decoder,
        )

        return ret
