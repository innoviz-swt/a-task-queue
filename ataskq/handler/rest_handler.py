from typing import List, NamedTuple, Tuple, Union
from enum import Enum
from datetime import datetime
import base64

from ataskq.imodel import IModel

try:
    import requests
except ImportError:
    raise Exception("'requests' is required to use ataskq REST handler.")

from .handler import Handler, EAction, from_datetime, get_query_kwargs, to_datetime


class RESTConnection(NamedTuple):
    url: Union[None, str]

    def __str__(self):
        return {self.url}


def from_connection_str(conn):
    ret = RESTConnection(url=conn)

    return ret


class RESTHandler(Handler):
    # todo: remove max jobs
    def __init__(self, conn=None, logger=None) -> None:
        self._connection = from_connection_str(conn)
        super().__init__(logger)

    @staticmethod
    def m2i_serialize():
        type_handlers = {
            datetime: lambda v: from_datetime(v),
            Enum: lambda v: v.value,
            bytes: lambda v: base64.b64encode(v).decode("ascii"),
        }

        return type_handlers

    @staticmethod
    def i2m_serialize():
        type_handlers = {datetime: lambda v: to_datetime(v), bytes: lambda v: base64.b64decode(v.encode("ascii"))}

        return type_handlers

    @property
    def api_url(self):
        return f"{self._connection.url}/api"

    def rest_get(self, url, *args, **kwargs):
        url = f"{self.api_url}/{url}"
        res = requests.get(url, *args, **kwargs)
        assert res.ok, f"get url '{url}' failed. message: {res.text}"

        return res.json()

    def rest_post(self, url, *args, **kwargs):
        url = f"{self.api_url}/{url}"
        res = requests.post(url, *args, **kwargs)
        assert res.ok, f"post url '{url}' failed. message: {res.text}"

        return res.json()

    def rest_put(self, url, *args, **kwargs):
        url = f"{self.api_url}/{url}"
        res = requests.put(url, *args, **kwargs)
        assert res.ok, f"put url '{url}' failed. message: {res.text}"

        return res.json()

    def rest_delete(self, url, *args, **kwargs):
        url = f"{self.api_url}/{url}"
        res = requests.delete(url, *args, **kwargs)
        assert res.ok, f"delete url '{url}' failed. message: {res.text}"

        return res.json()

    #########
    # Model #
    #########
    def get_all(self, model_cls: IModel, **kwargs) -> List[dict]:
        query_kwargs = get_query_kwargs(kwargs)
        res = self.rest_get(model_cls.table_key(), params=query_kwargs)
        return res

    def get(self, model_cls: IModel, model_id: int) -> dict:
        res = self.rest_get(f"{model_cls.table_key()}/{model_id}")
        return res

    def count_all(self, model_cls: IModel, **kwargs) -> int:
        query_kwargs = get_query_kwargs(kwargs)
        res = self.rest_get(f"{model_cls.table_key()}/count", params=query_kwargs)
        return res

    def _create(self, model_cls: IModel, **ikwargs: dict) -> int:
        res = self.rest_post(model_cls.table_key(), json=ikwargs)

        return res

    def _create_bulk(self, model_cls: IModel, ikwargs: List[dict]) -> List[int]:
        res = self.rest_post(f"{model_cls.table_key()}/bulk", json=ikwargs)

        return res

    def delete_all(self, model_cls: IModel, **kwargs):
        query_kwargs = get_query_kwargs(kwargs)
        self.rest_delete(f"{model_cls.table_key()}", json=query_kwargs)

    def delete(self, model_cls: IModel, model_id: int):
        self.rest_delete(f"{model_cls.table_key()}/{model_id}")

    def _update(self, model_cls: IModel, model_id, **ikwargs):
        self.rest_put(f"{model_cls.table_key()}/{model_id}", json=ikwargs)

    def update_all(self, model_cls: IModel, **ikwargs):
        self.rest_put(f"{model_cls.table_key()}", json=ikwargs)

    ##################
    # Custom Queries #
    ##################

    def take_next_task(self, job_id, level_start, level_stop) -> Tuple:
        from ..models import Task

        res = self.rest_get(
            "custom_query/take_next_task", params=dict(job_id=job_id, level_start=level_start, level_stop=level_stop)
        )

        action = EAction(res["action"])
        task = self.from_interface(Task, res["task"]) if res["task"] is not None else None

        return (action, task)

    def tasks_status(self, job_id):
        res = self.rest_get(f"custom_query/tasks_status/{job_id}")

        return res

    def jobs_status(self):
        res = self.rest_get(f"custom_query/jobs_status")

        return res
