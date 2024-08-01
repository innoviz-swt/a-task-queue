from typing import List, NamedTuple, Tuple, Union
from ataskq.model import Model, State
import msgpack
from io import BytesIO
from enum import Enum
from ..utils.dynamic_import import import_class


try:
    import requests
except ImportError:
    raise Exception("'requests' is required to use ataskq REST handler.")

from .handler import Handler, EAction, get_query_kwargs, Session


class RESTConnection(NamedTuple):
    url: Union[None, str]

    def __str__(self):
        return {self.url}


class RESTSession(Session):
    def __init__(self, connection: RESTConnection) -> None:
        super().__init__()
        self.connection = connection

    def connect(self):
        pass

    def close(self):
        pass


class RESTHandler(Handler):
    # todo: remove max jobs
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @staticmethod
    def from_connection_str(conn):
        ret = RESTConnection(url=conn)

        return ret

    @property
    def connection(self):
        return self._connection

    @property
    def api_url(self):
        return f"{self._connection.url}/api"

    def session(self, exclusive=False):
        return RESTSession(self._connection)

    def rest_get(self, url, *args, **kwargs):
        url = f"{self.api_url}/{url}"
        res = requests.get(url, *args, **kwargs)
        assert res.ok, f"get url '{url}' failed. message: {res.text}"

        return res.json()

    def rest_delete(self, url, *args, **kwargs):
        url = f"{self.api_url}/{url}"
        res = requests.delete(url, *args, **kwargs)
        assert res.ok, f"delete url '{url}' failed. message: {res.text}"

        return res.json()

    ###########
    # msgpack #
    ###########
    @classmethod
    def encode(cls, obj):
        if isinstance(obj, set):
            data = list(obj)
            data_bytes = msgpack.packb(data, default=cls.encode)
            ret = msgpack.ExtType(1, data_bytes)
        elif isinstance(obj, Enum):
            data = {
                "class": f"{obj.__module__}.{obj.__class__.__name__}",
                "data": obj.value,
            }
            data_bytes = msgpack.packb(data, default=cls.encode)
            ret = msgpack.ExtType(2, data_bytes)
        elif isinstance(obj, State):
            data = {
                "class": f"{obj.__module__}.{obj.__class__.__name__}",
                "data": obj.__dict__,
            }
            data_bytes = msgpack.packb(data, default=cls.encode)
            ret = msgpack.ExtType(3, data_bytes)
        elif isinstance(obj, Model):
            data = {
                "class": f"{obj.__module__}.{obj.__class__.__name__}",
                "data": {**obj.members(primary=True), **obj.relationships()},
                "state": obj._state,
            }
            data_bytes = msgpack.packb(data, default=cls.encode)
            ret = msgpack.ExtType(4, data_bytes)
        else:
            ret = obj

        return ret

    @classmethod
    def ext_hook(cls, code, data_bytes):
        if code == 1:
            with BytesIO(data_bytes) as b:
                data = msgpack.unpack(b, ext_hook=cls.ext_hook)
            ret = set(data)
        elif code == 2:
            with BytesIO(data_bytes) as b:
                data = msgpack.unpack(b, ext_hook=cls.ext_hook)
            cls = import_class(data["class"], Enum)
            ret = cls(data["data"])
        elif code == 3:
            with BytesIO(data_bytes) as b:
                data = msgpack.unpack(b, ext_hook=cls.ext_hook)
            cls = import_class(data["class"], State)
            ret = cls(**data["data"])
        elif code == 4:
            with BytesIO(data_bytes) as b:
                data = msgpack.unpack(b, ext_hook=cls.ext_hook)
            cls = import_class(data["class"], Model)
            ret = cls(**data["data"])
            ret._state = data["state"]
        else:
            ret = msgpack.ExtType(code, data_bytes)

        return ret

    ########
    # CRUD #
    ########

    def add(self, models: Union[Model, List[Model]]):
        packed = msgpack.packb(models, default=self.encode)
        print("Serialized data:", packed)

        # Decode the packed data
        unpacked = msgpack.unpackb(packed, ext_hook=self.ext_hook)
        print("Deserialized data:", unpacked)

    def get_all(self, model_cls: Model, **kwargs) -> List[dict]:
        query_kwargs = get_query_kwargs(kwargs)
        res = self.rest_get(model_cls.table_key(), params=query_kwargs)
        return res

    def get(self, model_cls: Model, model_id: int) -> dict:
        res = self.rest_get(f"{model_cls.table_key()}/{model_id}")
        return res

    def count_all(self, model_cls: Model, **kwargs) -> int:
        query_kwargs = get_query_kwargs(kwargs)
        res = self.rest_get(f"{model_cls.table_key()}/count", params=query_kwargs)
        return res

    def _create(self, model_cls: Model, model: dict) -> int:
        res = self.rest_post(model_cls.table_key(), json=model)

        return res

    def _create_bulk(self, model_cls: Model, ikwargs: List[dict]) -> List[int]:
        res = self.rest_post(f"{model_cls.table_key()}/bulk", json=ikwargs)

        return res

    def delete_all(self, model_cls: Model, **kwargs):
        query_kwargs = get_query_kwargs(kwargs)
        self.rest_delete(f"{model_cls.table_key()}", json=query_kwargs)

    def delete(self, model_cls: Model, model_id: int):
        self.rest_delete(f"{model_cls.table_key()}/{model_id}")

    def _update(self, model_cls: Model, model_id, **ikwargs):
        self.rest_put(f"{model_cls.table_key()}/{model_id}", json=ikwargs)

    def update_all(self, model_cls: Model, **ikwargs):
        self.rest_put(f"{model_cls.table_key()}", json=ikwargs)

    ##################
    # Custom Queries #
    ##################

    def take_next_task(self, **kwargs) -> Tuple:
        from ..models import Task

        res = self.rest_get("custom_query/take_next_task", params=kwargs)

        action = EAction(res["action"])
        task = self._from_interface(Task, res["task"]) if res["task"] is not None else None

        return (action, task)

    def tasks_status(self, **kwargs):
        res = self.rest_get(f"custom_query/tasks_status", params=kwargs)

        return res

    def jobs_status(self, **kwargs):
        res = self.rest_get(f"custom_query/jobs_status", params=kwargs)

        return res
