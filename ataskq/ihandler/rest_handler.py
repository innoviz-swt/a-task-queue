from typing import List, NamedTuple, Tuple
from enum import Enum
from datetime import datetime
import base64

from ataskq.models import Job, Model

try:
    import requests
except ImportError:
    raise Exception("'requests' is required to use ataskq REST handler.")

from .models import StateKWArg, Task
from .handler import Handler, EAction, from_datetime, to_datetime
from . import __schema_version__


class RESTConnection(NamedTuple):
    url: None or str

    def __str__(self):
        return {self.url}


def from_connection_str(conn):
    ret = RESTConnection(url=conn)

    return ret


class RESTHandler(Handler):
    # todo: remove max jobs
    def __init__(self, conn=None, logger=None) -> None:
        assert max_jobs is None, "RESTHandler doesn't support max jobs handling."
        self._connection = from_connection_str(conn)
        super().__init__(job_id, logger)

    @staticmethod
    def to_interface_hanlders():
        type_handlers = {
            datetime: lambda v: from_datetime(v),
            Enum: lambda v: v.value,
            bytes: lambda v: base64.b64encode(v).decode("ascii"),
        }

        return type_handlers

    @staticmethod
    def from_interface_hanlders():
        type_handlers = {datetime: lambda v: to_datetime(v), bytes: lambda v: base64.b64decode(v.encode("ascii"))}

        return type_handlers

    @property
    def api_url(self):
        return f"{self._connection.url}/api"

    def get(self, url, *args, **kwargs):
        url = f"{self.api_url}/{url}"
        res = requests.get(url, *args, **kwargs)
        assert res.ok, f"get url '{url}' failed. message: {res.text}"

        return res.json()

    def post(self, url, *args, **kwargs):
        url = f"{self.api_url}/{url}"
        res = requests.post(url, *args, **kwargs)
        assert res.ok, f"post url '{url}' failed. message: {res.text}"

        return res.json()

    def put(self, url, *args, **kwargs):
        url = f"{self.api_url}/{url}"
        res = requests.put(url, *args, **kwargs)
        assert res.ok, f"put url '{url}' failed. message: {res.text}"

        return res.json()

    def rest_delete(self, url, *args, **kwargs):
        url = f"{self.api_url}/{url}"
        res = requests.delete(url, *args, **kwargs)
        assert res.ok, f"delete url '{url}' failed. message: {res.text}"

        return res.json()

    def _create(self, model_cls: Model, **ikwargs: dict):
        res = self.post(model_cls.table_key(), json=ikwargs)
        return res[model_cls.id_key()]

    def delete(self, model_cls: Model, model_id: int):
        self.rest_delete(f"{model_cls.table_key()}/{model_id}")

    def _update(self, model_cls: Model, model_id, **ikwargs):
        self.put(f"{model_cls.table_key()}/{model_id}", json=ikwargs)

    ##################
    # Custom Queries #
    ##################

    def take_next_task(self, job_id, level_start, level_stop) -> Tuple[EAction, Task]:
        res = self.get(
            f"custom_query/take_next_task", params=dict(job_id=job_id, level_start=level_start, level_stop=level_stop)
        )

        action = EAction(res["action"])
        task = self.from_interface(Task, res["task"]) if res["task"] is not None else None

        return (action, task)

    def tasks_status(self, job_id):
        res = self.get(f"custom_query/tasks_status", params=dict(job_id=job_id))

        return res

    def jobs_status(self, c):
        res = self.get(f"custom_query/jobs_status", params=dict(job_id=job_id))

        return res
