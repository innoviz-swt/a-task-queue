from typing import List, NamedTuple, Tuple
from io import BytesIO
from enum import Enum

try:
    import requests
except ImportError:
    raise Exception("'requests' is required to use ataskq REST handler.")

from .models import Job, StateKWArg, Task, EStatus
from .handler import Handler, EAction
from . import __schema_version__


class RESTConnection(NamedTuple):
    url: None or str

    def __str__(self):
        return f"rest://{self.url}"


def from_connection_str(conn):
    ret = RESTConnection(url=conn)

    return ret


class RESTHandler(Handler):
    # todo: remove max jobs
    def __init__(self, conn=None, max_jobs=None, job_id=None, logger=None) -> None:
        self._connection = from_connection_str(conn)
        self._job_id = job_id
        super().__init__(job_id, logger)

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

    def create_job(self, name='', description='') -> Handler:
        res = self.post('jobs', data=dict(
            name=name,
            description=description
        ))
        self._job_id = res['job_id']

        return self

    def _add_tasks(self, tasks: List[Task]):
        files = []
        data = []
        for i, t in enumerate(tasks):
            for k, v in t.__dict__.items():
                if k == Task.id_key():
                    continue
                if isinstance(v, bytes):
                    files.append((f'{i}.{k}', BytesIO(v)))
                elif isinstance(v, Enum):
                    data.append((f'{i}.{k}', f'{v}'))
                else:
                    data.append((f'{i}.{k}', v))

        self.post(f'jobs/{self._job_id}/tasks', files=files, data=data)

    def get_state_kwargs(self):
        res = self.get(f'jobs/{self._job_id}/state_kwargs')
        ret = [StateKWArg(**skw) for skw in res]

        return ret

    def _take_next_task(self, level) -> Tuple[EAction, Task]:
        res = self.get(f'jobs/{self._job_id}/next_task', params=dict(level=1))

        action = EAction(res['action'])
        task = Task(**res['task']) if res['task'] is not None else None

        return (action, task)
