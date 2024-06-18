from typing import List, Dict
from enum import Enum
import pickle
from datetime import datetime
from .utils.dynamic_import import import_callable
from .model import Model


class EntryPointRuntimeError(RuntimeError):
    pass


class TARGSLoadRuntimeError(EntryPointRuntimeError):
    pass


class EntrypointLoadRuntimeError(EntryPointRuntimeError):
    pass


class EntrypointCallRuntimeError(EntryPointRuntimeError):
    pass


class EStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILURE = "failure"

    def __str__(self) -> str:
        return self.value


class EntryPoint:
    @staticmethod
    def init(kwargs) -> None:
        entrypoint = kwargs.get("entrypoint")
        if callable(entrypoint):
            kwargs["entrypoint"] = f"{entrypoint.__module__}.{entrypoint.__name__}"

        targs = kwargs.get("targs")
        if targs is not None and isinstance(targs, tuple):
            assert len(targs) == 2
            assert isinstance(targs[0], tuple)
            assert isinstance(targs[1], dict)
            kwargs["targs"] = pickle.dumps(targs)

    def get_entrypoint(self):
        try:
            func = import_callable(self.entrypoint)
        except Exception as ex:
            raise EntrypointLoadRuntimeError from ex

        return func


class Object(Model):
    object_id: int
    blob: bytes
    serializer: str
    desrializer: str
    # created_at: datetime
    # updated_at: datetime


class Task(Model, EntryPoint):
    task_id: int
    name: str
    description: str
    level: float
    entrypoint: str
    kwargs_oid: int
    status: EStatus
    take_time: datetime
    start_time: datetime
    done_time: datetime
    pulse_time: datetime
    job_id: int

    __DEFAULTS__ = dict(status=EStatus.PENDING, entrypoint="", level=0.0)

    def __init__(self, **kwargs) -> None:
        EntryPoint.init(kwargs)
        Model.__init__(self, **kwargs)

    def __str__(self):
        return f"{self.name}({self.task_id})" if self.name else f"{self.task_id}"


class Job(Model):
    job_id: int
    name: str
    priority: float
    description: str

    @staticmethod
    def id_key():
        return "job_id"

    @staticmethod
    def table_key():
        return "jobs"

    @staticmethod
    def children():
        return {
            Task: "job_id",
        }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_tasks(self, _handler=None) -> List[Task]:
        return self.get_children(Task, _handler=_handler)

    def add_tasks(self, tasks: List[Task], _handler=None):
        return self.add_children(Task, tasks, _handler=_handler)


__MODELS__: Dict[str, Model] = {m.table_key(): m for m in [Object, Task, Job]}
