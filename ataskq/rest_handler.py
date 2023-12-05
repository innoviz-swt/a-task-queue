from enum import Enum
from datetime import datetime, timedelta
import pickle
from typing import List, Tuple, NamedTuple
from pathlib import Path
from io import TextIOWrapper
import re

try:
    import requests
except ImportError:
    raise Exception("'requests' is required to use ataskq REST handler.")

from .models import Job, StateKWArg, Task, EStatus
from .handler import Handler
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
