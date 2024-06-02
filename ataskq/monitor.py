from threading import Thread, Event

from .models import Task, EStatus


class MonitorThread(Thread):
    # task runner is .task_runner TaskRunner, avoiding circular import
    def __init__(self, task: Task, ataskq, pulse_interval: float = 60) -> None:
        from .taskq import TaskQ  # here to avoid circular dependency

        super().__init__(daemon=True)
        self._stop_event = Event()
        self._task = task
        self._ataskq: TaskQ = ataskq
        self._pulse_interval = pulse_interval

    def run(self) -> None:
        self._ataskq.info(f"Running monitor thread for task '{self._task}'")
        while not self._stop_event.is_set():
            self._ataskq.update_task_status(self._task, EStatus.RUNNING)
            self._stop_event.wait(self._pulse_interval)

    def stop(self):
        self._stop_event.set()
