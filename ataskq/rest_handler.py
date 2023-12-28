from typing import List, NamedTuple, Tuple
from io import BytesIO
from enum import Enum
from datetime import datetime
import base64

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
    def __init__(self, conn=None, max_jobs=None, job_id=None, logger=None) -> None:
        self._connection = from_connection_str(conn)
        self._job_id = job_id
        super().__init__(job_id, logger)

    @staticmethod
    def to_interface_type_hanlders():
        type_handlers = {
            datetime: lambda v: f"{from_datetime(v)}",
            Enum: lambda v: v.value,
            bytes: lambda v: base64.b64encode(v).decode('ascii')
        }

        return type_handlers

    @staticmethod
    def from_interface_type_hanlders():
        type_handlers = {
            datetime: lambda v: to_datetime(v),
            bytes: lambda v: base64.b64decode(v.encode('ascii'))
        }

        return type_handlers

    @property
    def api_url(self):
        return f'{self._connection.url}/api'

    def get(self, url, *args, **kwargs):
        url = f'{self.api_url}/{url}'
        res = requests.get(url, *args, **kwargs)
        assert res.ok, f"get url '{url}' failed. message: {res.text}"

        return res.json()

    def post(self, url, *args, **kwargs):
        url = f'{self.api_url}/{url}'
        res = requests.post(url, *args, **kwargs)
        assert res.ok, f"post url '{url}' failed. message: {res.text}"

        return res.json()

    def put(self, url, *args, **kwargs):
        url = f'{self.api_url}/{url}'
        res = requests.put(url, *args, **kwargs)
        assert res.ok, f"put url '{url}' failed. message: {res.text}"

        return res.json()

    def create_job(self, name='', description='') -> Handler:
        res = self.post('jobs', data=dict(
            name=name,
            description=description
        ))
        self._job_id = res['job_id']

        return self

    # # form based implementation
    # def _add_tasks(self, tasks: List[Task]):
    #     files = []
    #     data = []
    #     for i, t in enumerate(tasks):
    #         for k, v in t.items():
    #             if k == Task.id_key():
    #                 continue
    #             if isinstance(v, bytes):
    #                 files.append((f'{i}.{k}', BytesIO(v)))
    #             else:
    #                 data.append((f'{i}.{k}', v))

    #     self.post(f'jobs/{self._job_id}/tasks', files=files, data=data)

    def _add_tasks(self, itasks: List[Task]):
        self.post(f'jobs/{self._job_id}/tasks', json=itasks)

    def _add_state_kwargs(self, i_state_kwargs: List[dict]):
        self.post(f'jobs/{self._job_id}/state_kwargs', json=i_state_kwargs)

    def get_state_kwargs(self):
        res = self.get(f'jobs/{self._job_id}/state_kwargs')
        ret = [self.from_interface(StateKWArg, skw) for skw in res]

        return ret

    def get_tasks(self, order_by=None):
        res = self.get(f'jobs/{self._job_id}/tasks')
        ret = [self.from_interface(Task, t) for t in res]

        return ret

    def _take_next_task(self, level) -> Tuple[EAction, Task]:
        level_start = level.start if level is not None else None
        level_stop = level.stop if level is not None else None
        res = self.get(f'jobs/{self._job_id}/next_task', params=dict(level_start=level_start, level_stop=level_stop))

        action = EAction(res['action'])
        task = self.from_interface(Task, res['task']) if res['task'] is not None else None

        return (action, task)

    def _update_task(self, task_id, **ikwargs):
        self.put(f'tasks/{task_id}', json=ikwargs)
