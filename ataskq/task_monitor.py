import socket
from threading import Thread, Event
import time

from .task import Task, EStatus


class MonitorThread(Thread):
    # task runner is .task_runner TaskRunner, avoiding circular import
    def __init__(self, task: Task, task_runner, pulse_interval: float = 60) -> None:
        super().__init__(daemon=True)
        self._stop_event = Event()
        self._task = task
        self._task_runner = task_runner
        self._pulse_interval = pulse_interval

    def run(self) -> None:
        self._task_runner.info(f"Running monitor thread for task id '{self._task.tid}'")
        while not self._stop_event.is_set():
            self._task_runner.update_task_status(self._task, EStatus.RUNNING)
            self._stop_event.wait(self._pulse_interval)

    def stop(self):
        self._stop_event.set()
         