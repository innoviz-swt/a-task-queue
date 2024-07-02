from .models import Model
from .utils.dynamic_import import import_callable


def pickle_dict(**obj):
    ret = Object.serialize(obj, serializer="pickle.dumps", desrializer="pickle.loads")
    return ret


class Object(Model):
    object_id: int
    blob: bytes
    serializer: str
    desrializer: str

    def deserialize(self):
        deserializer_func = import_callable(self.desrializer)
        obj = deserializer_func(self.blob)
        return obj

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

    # created_at: datetime
    # updated_at: datetime
