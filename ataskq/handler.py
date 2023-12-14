from abc import ABC, abstractmethod
import pickle
from typing import Tuple, Union
from enum import Enum

from .logger import Logger
from .models import Task, EStatus


class EAction(str, Enum):
    RUN_TASK = 'run_task'
    WAIT = 'wait'
    STOP = 'stop'


class Handler(ABC, Logger):
    def __init__(self, job_id=None, logger: Logger = None):
        Logger.__init__(self, logger)

        self._job_id = job_id

    @property
    def job_id(self):
        return self._job_id

    @abstractmethod
    def create_job(self, c, name='', description=''):
        pass

    @abstractmethod
    def _add_tasks(self, tasks):
        pass

    @abstractmethod
    def get_state_kwargs(self):
        pass

    @abstractmethod
    def _take_next_task(self, level: Union[int, None]) -> Tuple[EAction, Task]:
        pass

    def add_tasks(self, tasks):
        if self._job_id is None:
            raise RuntimeError(f"Job not assigned, pass job_id in __init__ or use create_job() first.")

        if isinstance(tasks, (Task)):
            tasks = [tasks]

        # Insert data into a table
        # todo use some sql batch operation
        for t in tasks:
            # set \ validate job id
            assert t.job_id is None or t.job_id == self._job_id
            t.job_id = self._job_id

            # assert status as pending
            assert t.status == EStatus.PENDING

            if callable(t.entrypoint):
                t.entrypoint = f"{t.entrypoint.__module__}.{t.entrypoint.__name__}"

            if t.targs is not None:
                assert len(t.targs) == 2
                assert isinstance(t.targs[0], tuple)
                assert isinstance(t.targs[1], dict)
                t.targs = pickle.dumps(t.targs)

        self._add_tasks(tasks)


def from_connection_str(conn=None, **kwargs) -> Handler:
    if conn is None:
        conn = ''

    sep = '://'
    sep_index = conn.find(sep)
    if sep_index == -1:
        raise RuntimeError(f'connection must be of format <db type>://<connection string>')
    handler_type = conn[:sep_index]

    # validate connectino
    if not handler_type:
        raise RuntimeError(f'missing db type, connection must be of format <db type>://<connection string>')

    connection_str = conn[sep_index + len(sep):]
    if not connection_str:
        raise RuntimeError(f'missing connection string, connection must be of format <db type>://<connection string>')

    # get db type handler
    if handler_type == 'sqlite':
        from .db_handler.sqlite3 import SQLite3DBHandler
        handler = SQLite3DBHandler(conn, **kwargs)
    elif handler_type == 'postgresql':
        from .db_handler.postgresql import PostgresqlDBHandler
        handler = PostgresqlDBHandler(conn, **kwargs)
    elif handler_type == 'http' or handler_type == 'https':
        from .rest_handler import RESTHandler
        handler = RESTHandler(conn, **kwargs)
    else:
        raise Exception(f"unsupported db type '{handler_type}', db type must be one of ['sqlite', 'postgresql']")

    return handler
