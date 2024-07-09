from typing import List
from .model import Model, PrimaryKey, Str, Int, Float, DateTime, Child, Bytes
from .object import Object
from enum import Enum


class EStatus(Str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILURE = "failure"

    def __str__(self) -> str:
        return self.value


class Task(Model):
    task_id: PrimaryKey
    name: Str
    description: Str
    level: Float = 0.0
    entrypoint: Str = ""
    kwargs_id: Int
    status: EStatus = EStatus.PENDING
    start_time: DateTime
    take_time: DateTime
    done_time: DateTime
    pulse_time: DateTime
    job_id: Int
    kwargs: Object = Child(key="kwargs_id")

    def __init__(self, **kwargs) -> None:
        self._kwargs = kwargs.pop("kwargs", None)
        entrypoint = kwargs.get("entrypoint")
        if callable(entrypoint):
            kwargs["entrypoint"] = f"{entrypoint.__module__}.{entrypoint.__name__}"
        Model.__init__(self, **kwargs)

    def get_kwargs(self, _handler=None):
        if self._kwargs is not None:
            return self._kwargs

        ret = Object.get(self.kwargs_id, _handler=_handler)
        self._kwargs = ret

        return ret

    def set_kwargs(self, v: Object):
        if self.kwargs_id is not None:
            raise Exception(f"kwargs already assign to task, object id - {self.kwargs_id}")
        if v.object_id:
            self.kwargs_id = v.object_id
        self._kwargs = v

    def __str__(self):
        return f"{self.name}({self.task_id})" if self.name else f"{self.task_id}"


class Job(Model):
    job_id: PrimaryKey
    name: Str
    priority: Float
    description: Str

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_tasks(self, _handler=None) -> List[Task]:
        ret = Task.get_all(job_id=self.job_id, _handler=_handler)
        return ret

    def add_tasks(self, tasks: List[Task], _handler=None):
        for t in tasks:
            t.job_id = self.job_id
        # create bulk objects
        tasks_kwargs = [t._kwargs for t in tasks if t._kwargs and t._kwargs.object_id is None]
        indices = [i for i, t in enumerate(tasks) if t._kwargs and t._kwargs.object_id is None]
        Object.create_bulk(tasks_kwargs, _handler=_handler)
        for i in indices:
            tasks[i].kwargs_id = tasks[i]._kwargs.object_id
        Task.create_bulk(tasks, _handler=_handler)

        return tasks
