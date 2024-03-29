from abc import ABC, abstractmethod
import pickle
from typing import Tuple, Union, Dict, Callable, List
from enum import Enum
from datetime import datetime

from .logger import Logger
from .models import Model, Job, Task, EStatus, StateKWArg


__STRTIME_FORMAT__ = '%Y-%m-%d %H:%M:%S.%f'


def to_datetime(string: Union[str, datetime, None]):
    if string is None:
        return None
    elif isinstance(string, datetime):
        return string

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

    def set_job_id(self, job_id):
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
    def from_interface(cls, model_cls: Model, kwargs: dict) -> Model:
        return model_cls.from_interface(kwargs, cls.from_interface_type_hanlders())

    @classmethod
    def m2i(cls, model_cls: Model, kwargs: dict) -> dict:
        """modle to interface"""
        return model_cls.m2i(kwargs, cls.to_interface_type_hanlders())

    @classmethod
    def to_interface(cls, model: Model) -> Model:
        return model.to_interface(cls.to_interface_type_hanlders())

    @abstractmethod
    def get_jobs(self) -> List[Job]:
        pass

    @abstractmethod
    def create_job(self, c, name=None, description=None):
        pass

    @abstractmethod
    def _delete_job(self):
        pass

    def delete_job(self):
        self._delete_job()
        self._job_id = None

    @abstractmethod
    def _add_tasks(self, itasks: List[dict]):
        pass

    @abstractmethod
    def _add_state_kwargs(self, i_state_kwargs: List[dict]):
        pass

    @abstractmethod
    def get_tasks(self, order_by=None):
        pass

    @abstractmethod
    def get_state_kwargs(self):
        pass

    @abstractmethod
    def _update_task(self, task_id: int, **ikwargs):
        pass

    def update_task(self, task_id: int, **mkwargs):
        assert task_id is not None, "task must have assigned 'task_id' for update"
        ikwargs = self.m2i(Task, mkwargs)
        self._update_task(task_id, **ikwargs)

    def update_task_start_time(self, task: Task, start_time: datetime = None):
        if start_time is None:
            start_time = datetime.now()

        mkwargs = dict(start_time=start_time)

        self.update_task(task.task_id, **mkwargs)
        task.start_time = start_time

    def update_task_status(self, task: Task, status: EStatus, timestamp: datetime = None):
        if timestamp is None:
            timestamp = datetime.now()

        if status == EStatus.RUNNING:
            # for running task update pulse_time
            self.update_task(task.task_id, status=status, pulse_time=timestamp)
            task.status = status
            task.pulse_time = timestamp
        elif status == EStatus.SUCCESS or status == EStatus.FAILURE:
            # for done task update pulse_time and done_time time as well
            self.update_task(task.task_id, status=status, pulse_time=timestamp, done_time=timestamp)
            task.status = status
            task.pulse_time = timestamp
            task.done_time = timestamp
        else:
            raise RuntimeError(
                f"Unsupported status '{status}' for status update")

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

        itask = [self.to_interface(t) for t in tasks]
        self._add_tasks(itask)

        return self

    def add_state_kwargs(self, state_kwargs: Union[StateKWArg, List[StateKWArg]]):
        if self._job_id is None:
            raise RuntimeError(f"Job not assigned, pass job_id in __init__, set_job_id or use create_job() first.")

        if isinstance(state_kwargs, StateKWArg):
            state_kwargs = [state_kwargs]

        # Insert data into a table
        # todo use some sql batch operation
        for skw in state_kwargs:
            assert skw.job_id is None or skw.job_id == self._job_id
            skw.job_id = self._job_id

            if callable(skw.entrypoint):
                skw.entrypoint = f"{skw.entrypoint.__module__}.{skw.entrypoint.__name__}"

            if skw.targs is not None:
                assert len(skw.targs) == 2
                assert isinstance(skw.targs[0], tuple)
                assert isinstance(skw.targs[1], dict)
                skw.targs = pickle.dumps(skw.targs)

        i_state_kwargs = [self.to_interface(t) for t in state_kwargs]
        self._add_state_kwargs(i_state_kwargs)

        return self

    @abstractmethod
    def count_pending_tasks_below_level(self, level: int) -> int:
        pass


def from_connection_str(conn=None, **kwargs) -> Handler:
    if conn is None:
        conn = ''

    sep = '://'
    sep_index = conn.find(sep)
    if sep_index == -1:
        raise RuntimeError(f'connection must be of format <type>://<connection string>')
    handler_type = conn[:sep_index]

    # validate connectino
    if not handler_type:
        raise RuntimeError(f'missing handler type, connection must be of format <type>://<connection string>')

    connection_str = conn[sep_index + len(sep):]
    if not connection_str:
        raise RuntimeError(f'missing connection string, connection must be of format <type>://<connection string>')

    # get db type handler
    if handler_type == 'sqlite':
        from .db_handler.sqlite3 import SQLite3DBHandler
        handler = SQLite3DBHandler(conn, **kwargs)
    elif handler_type == 'pg':
        from .db_handler.postgresql import PostgresqlDBHandler
        handler = PostgresqlDBHandler(conn, **kwargs)
    elif handler_type == 'http' or handler_type == 'https':
        from .rest_handler import RESTHandler
        handler = RESTHandler(conn, **kwargs)
    else:
        raise Exception(f"unsupported handler type '{handler_type}', type must be one of ['sqlite', 'pg', 'http']")

    return handler
