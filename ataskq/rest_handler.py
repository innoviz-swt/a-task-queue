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
    def __init__(self, conn=None, max_jobs=None, job_id=None, logger=None) -> None:
        assert max_jobs is None, "RESTHandler doesn't support max jobs handling."
        self._max_jobs = max_jobs
        self._connection = from_connection_str(conn)
        self._job_id = job_id
        super().__init__(job_id, logger)

    @staticmethod
    def to_interface_hanlders():
        type_handlers = {
            datetime: lambda v: from_datetime(v),
            Enum: lambda v: v.value,
            bytes: lambda v: base64.b64encode(v).decode('ascii')
        }

        return type_handlers

    @staticmethod
    def from_interface_hanlders():
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

    def rdelete(self, url, *args, **kwargs):
        url = f'{self.api_url}/{url}'
        res = requests.delete(url, *args, **kwargs)
        assert res.ok, f"delete url '{url}' failed. message: {res.text}"

        return res.json()

    def _create(self, model_cls: Model, **ikwargs: dict):
        res = self.post(model_cls.table_key(), json=ikwargs)
        return res[model_cls.id_key()]

    def delete(self, model_cls: Model, model_id: int):
        self.rdelete(f'{model_cls.table_key()}/{model_id}')

    def _update(self, model_cls: Model, model_id, **ikwargs):
        self.put(f'{model_cls.table_key()}/{model_id}', json=ikwargs)

    def keep_max_jobs(self):
        raise NotImplementedError(f"{self.__class__.__name__} doesn't implement keep_max_jobs")

    def get_jobs(self) -> List[Job]:
        ijobs = self.get('jobs')
        jobs = [self.from_interface(Job, j) for j in ijobs]

        return jobs

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

    def count_pending_tasks_below_level(self, level: int):
        res = self.get(f'jobs/{self._job_id}/count_pending_tasks_below_level', params=dict(level=level))
        return res['count']
