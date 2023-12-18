from abc import ABC, abstractmethod
import pickle
from typing import Tuple, Union, Dict, Callable, List
from enum import Enum
from datetime import datetime

from .logger import Logger
from .models import Task, EStatus, Model


__STRTIME_FORMAT__ = '%Y-%m-%d %H:%M:%S.%f'


def to_datetime(string: datetime):
    if string is None:
        return None

    return datetime.strptime(string, __STRTIME_FORMAT__)


def from_datetime(time: datetime):
    return time.strftime(__STRTIME_FORMAT__)


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

    @staticmethod
    @abstractmethod
    def from_interface_type_hanlders() -> Dict[type, Callable]:
        pass

    @staticmethod
    @abstractmethod
    def to_interface_type_hanlders() -> Dict[type, Callable]:
        pass

    @classmethod
    def i2m(cls, model_cls: Model, kwargs: dict) -> dict:
        """interface to model"""
        return model_cls.i2m(kwargs, cls.from_interface_type_hanlders())

    @classmethod
    def m2i(cls, model_cls: Model, kwargs: dict) -> dict:
        """modle to interface"""
        return model_cls.m2i(kwargs, cls.to_interface_type_hanlders())

    @classmethod
    def from_interface(cls, model_cls: Model, kwargs: dict) -> Model:
        return model_cls.from_interface(kwargs, cls.from_interface_type_hanlders())

    @abstractmethod
    def create_job(self, c, name='', description=''):
        pass

    @abstractmethod
    def _add_tasks(self, tasks: dict):
        pass

    @abstractmethod
    def get_state_kwargs(self):
        pass

    @abstractmethod
    def _update_task(self, task_id: int, **kwargs):
        pass

    def update_task_start_time(self, task: Task, start_time: datetime = None):
        if start_time is None:
            start_time = datetime.now()

        kwargs = dict(start_time=start_time)
        kwargs = self.m2i(Task, kwargs)

        self._update_task(task.task_id, **kwargs)
        task.start_time = start_time

    @abstractmethod
    def _take_next_task(self, level: Union[int, None]) -> Tuple[EAction, Task]:
        pass

    def add_tasks(self, tasks: Union[Task, List[Task]]):
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

        itask = [t.to_interface(self.from_interface_type_hanlders()) for t in tasks]
        self._add_tasks(itask)


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
