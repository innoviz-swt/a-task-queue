from typing import List
from .model import Model, PrimaryKey, Str, Int, Float, DateTime, Parent, Bytes
from .object import Object, pickle_dict
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
    kwargs: Object = Parent(key="kwargs_id")

    def __init__(self, **kwargs) -> None:
        entrypoint = kwargs.get("entrypoint")
        if callable(entrypoint):
            kwargs["entrypoint"] = f"{entrypoint.__module__}.{entrypoint.__name__}"
        if isinstance(kwargs.get("kwargs"), dict):
            kwargs["kwargs"] = pickle_dict(**kwargs["kwargs"])
        Model.__init__(self, **kwargs)

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
        tasks_kwargs = [t.kwargs for t in tasks if t.kwargs and t.kwargs.object_id is None]
        indices = [i for i, t in enumerate(tasks) if t.kwargs and t.kwargs.object_id is None]
        Object.create_bulk(tasks_kwargs, _handler=_handler)
        for i in indices:
            tasks[i].kwargs_id = tasks[i].kwargs.object_id
        Task.create_bulk(tasks, _handler=_handler)

        return tasks
