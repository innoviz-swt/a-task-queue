import multiprocessing
import os
import pickle
import logging
from importlib import import_module
from multiprocessing import Process
import time

from .logger import Logger
from .task import EStatus, Task
from .monitor import MonitorThread
from .db_handler import DBHandler, EQueryType, EAction


def targs(*args, **kwargs):
    return (args, kwargs)


def keyval_store_retry(retries=1000, polling_delta=0.1):
    def decorator(func):
        def wrapper(self, *args, **kwargs):
            for i in range(retries):
                try:
                    ret = func(self, *args, **kwargs)
                    # self.info(f' success in {i} iteration.')
                    return ret
                except Exception:
                    if (i != 0 and i % 100 == 0):
                        self.warning(f'keyval store retry {i} iteration.')
                    time.sleep(polling_delta)
                    continue
            raise RuntimeError(f"Failed keyval store retry retry. retries: {retries}, polling_delta: {polling_delta}.")
        return wrapper
    return decorator


class TaskQ(Logger):
    def __init__(self, db="sqlite://ataskq.sqlite.db", run_task_raise_exception=False, task_pull_intervnal=0.2, monitor_pulse_interval=60, monitor_timeout_internal=60 * 5, logger: logging.Logger or None = None) -> None:
        """
        Args:
        task_pull_intervnal: pulling interval for task to complete in seconds.
        monitor_pulse_interval: update interval for pulse in seconds while taks is running.
        monitor_timeout_internal: timeout for task last monitor pulse in seconds, if passed before getting next task, set to Failure.
        run_task_raise_exception: if True, run_task will raise exception when task fails. This is for debugging purpose only and will fail production flow.
        """
        super().__init__(logger)

        # init db handler
        self._db_handler = DBHandler(db=db)

        self._run_task_raise_exception = run_task_raise_exception
        self._task_pull_interval = task_pull_intervnal
        self._monitor_pulse_interval = monitor_pulse_interval
        self._monitor_timeout_internal = monitor_timeout_internal

        self._running = False

    @property
    def db_handler(self):
        return self._db_handler

    @property
    def task_wait_interval(self):
        return self._task_pull_interval

    @property
    def monitor_pulse_interval(self):
        return self._monitor_pulse_interval

    def create_job(self, overwrite=False):
        if overwrite and self._db_handler.db_path and os.path.exists(self._db_handler.db_path):
            os.remove(self._db_handler.db_path)
        self._db_handler.create_job()

        return self

    def add_tasks(self, tasks):
        self._db_handler.add_tasks(tasks)

        return self

    def count_pending_tasks_below_level(self, level):
        return self._db_handler.count_pending_tasks_below_level(level)

    def log_tasks(self):
        rows, _ = self._db_handler.query(query_type=EQueryType.TASKS)

        self.info("# tasks:")
        for row in rows:
            self.info(row)

    def get_tasks(self):
        rows, col_names = self._db_handler.query(query_type=EQueryType.TASKS)
        tasks = [Task(**dict(zip(col_names, row))) for row in rows]

        return tasks

    def update_task_start_time(self, task):
        self._db_handler.update_task_start_time(task)

    def update_task_status(self, task, status):
        self._db_handler.update_task_status(task, status)

    def _run_task(self, task):
        # get entry point func to execute
        ep = task.entrypoint
        if ep == 'ataskq.skip_run_task':
            self.info(f"task '{task.tid}' is marked as 'skip_run_task', skipping run task.")
            return

        assert '.' in ep, 'entry point must be inside a module.'
        module_name, func_name = ep.rsplit('.', 1)
        try:
            m = import_module(module_name)
        except ImportError as ex:
            raise RuntimeError(f"Failed to load module '{module_name}'. Exception: '{ex}'")
        assert hasattr(
            m, func_name), f"failed to load entry point, module '{module_name}' doen't have func named '{func_name}'."
        func = getattr(m, func_name)
        assert callable(func), f"entry point is not callable, '{module_name},{func}'."

        # get targs
        if task.targs is not None:
            try:
                targs = pickle.loads(task.targs)
            except Exception as ex:
                if self._run_task_raise_exception:  # for debug purposes only
                    self.warning("Getting tasks args failed.")
                    self.update_task_status(task, EStatus.FAILURE)
                    raise ex

                self.warning("Getting tasks args failed.", exc_info=True)
                self.update_task_status(task, EStatus.FAILURE)

                return
        else:
            targs = ((), {})

        # update task start time
        self.update_task_start_time(task)

        # run task
        monitor = MonitorThread(task, self, pulse_interval=self._monitor_pulse_interval)
        monitor.start()

        try:
            func(*targs[0], **targs[1])
            status = EStatus.SUCCESS
        except Exception as exc:
            if self._run_task_raise_exception:  # for debug purposes only
                self.warning("Running task entry point failed with exception.")
                self.update_task_status(task, EStatus.FAILURE)
                monitor.stop()
                monitor.join()
                raise exc

            self.warning("Running task entry point failed with exception.", exc_info=True)
            status = EStatus.FAILURE

        monitor.stop()
        monitor.join()
        self.update_task_status(task, status)

    def _run(self, level):
        # check for error code
        while True:
            # update tasks timeout
            self._db_handler._set_timeout_tasks(self._monitor_timeout_internal)
            # grab tasks and set them in Q
            action, task = self._db_handler._take_next_task(level)

            # handle no task available
            if action == EAction.STOP:
                break
            if action == EAction.RUN_TASK:
                self._run_task(task)
            elif action == EAction.WAIT:
                self.debug(f"waiting for {self._task_pull_interval} sec before taking next task")
                time.sleep(self._task_pull_interval)

    def assert_level(self, level):
        if isinstance(level, int):
            level = range(level, level+1)
        elif isinstance(level, (list, tuple)):
            assert len(level) == 2, 'level of type list or tuple must have length of 2'
            level = range(level[0], level[1])
        else:
            assert isinstance(level, range), 'level must be int, list, tuple or range'

        # check all task < level.start are done
        count = self.count_pending_tasks_below_level(level.start)
        assert count == 0, f'all tasks below level must be done before running tasks at levels {level}'

        return level

    def run(self, num_processes=None, level=None):
        if level is not None:
            level = self.assert_level(level)

        self._running = True

        # default to run in current process
        if num_processes is None:
            self._run(level)
            self._running = False
            return

        assert isinstance(num_processes, (int, float))

        if isinstance(num_processes, float):
            assert 0.0 <= num_processes <= 1.0
            nprocesses = int(multiprocessing.cpu_count() * num_processes)
        elif num_processes < 0:
            nprocesses = multiprocessing.cpu_count() - num_processes
        else:
            nprocesses = num_processes

        # set processes and Q
        processes = [Process(target=self._run, args=(level,), daemon=True) for i in range(nprocesses)]
        [p.start() for p in processes]

        # join all processes
        [p.join() for p in processes]

        # log failed processes
        for p in processes:
            if p.exitcode != 0:
                self.error(f"Process '{p.pid}' failed with exitcode '{p.exitcode}'")

        self._running = False

        return self
