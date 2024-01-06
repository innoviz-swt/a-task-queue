import socket
from threading import Thread, Event
import time

from .models import Task, EStatus


class MonitorThread(Thread):
    # task runner is .task_runner TaskRunner, avoiding circular import
    def __init__(self, task: Task, ataskq, pulse_interval: float = 60) -> None:
        super().__init__(daemon=True)
        self._stop_event = Event()
        self._task = task
        self._ataskq = ataskq
        self._pulse_interval = pulse_interval

    def run(self) -> None:
        self._ataskq.info(f"Running monitor thread for task id '{self._task.task_id}'")
        while not self._stop_event.is_set():
            self._ataskq.update_task_status(self._task, EStatus.RUNNING)
            self._stop_event.wait(self._pulse_interval)

    def stop(self):
        self._stop_event.set()
