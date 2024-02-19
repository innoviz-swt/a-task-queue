import multiprocessing
import os
import pickle
import logging
from importlib import import_module
from multiprocessing import Process
from inspect import signature
import time
from typing import Dict, List

from .env import ATASKQ_CONNECTION, ATASKQ_MONITOR_PULSE_INTERVAL, ATASKQ_TASK_PULSE_TIMEOUT, ATASKQ_TASK_PULL_INTERVAL
from .logger import Logger
from .models import EStatus, StateKWArg, Task, EntryPointRuntimeError
from .monitor import MonitorThread
from .db_handler import EQueryType, EAction, DBHandler
from .handler import Handler
from .handler import from_connection_str


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
    def __init__(
            self,
            job_id=None,
            conn=ATASKQ_CONNECTION,
            run_task_raise_exception=False,
            task_pull_intervnal=ATASKQ_TASK_PULL_INTERVAL,
            monitor_pulse_interval=ATASKQ_MONITOR_PULSE_INTERVAL,
            task_pulse_timeout=ATASKQ_TASK_PULSE_TIMEOUT,
            max_jobs=None,
            logger: logging.Logger or None = None) -> None:
        """
        Args:
        task_pull_intervnal: pulling interval for task to complete in seconds.
        task_pulse_timeout: update interval for pulse in seconds while taks is running.
        monitor_timeout_internal: timeout for task last monitor pulse in seconds, if passed before getting next task, set to Failure.
        run_task_raise_exception: if True, run_task will raise exception when task fails. This is for debugging purpose only and will fail production flow.
        """
        super().__init__(logger)

        # init db handler
        self._hanlder: Handler = from_connection_str(
            conn=conn, job_id=job_id, max_jobs=max_jobs, logger=self._logger)

        self._run_task_raise_exception = run_task_raise_exception
        self._task_pull_interval = task_pull_intervnal
        self._monitor_pulse_interval = monitor_pulse_interval
        self._task_pulse_timeout = task_pulse_timeout

        # state kwargs for jobs
        self._state_kwargs: Dict[str: object] = dict()

        self._running = False

    @property
    def handler(self):
        return self._hanlder

    @property
    def task_wait_interval(self):
        return self._task_pull_interval

    @property
    def monitor_pulse_interval(self):
        return self._monitor_pulse_interval

    def create_job(self, name=None, description=None):
        self._hanlder.create_job(name=name, description=description)

        return self

    def add_state_kwargs(self, state_kwargs):
        self._hanlder.add_state_kwargs(state_kwargs)

        return self

    def add_tasks(self, tasks: Task or List[Task]):
        self._hanlder.add_tasks(tasks)

        return self

    def count_pending_tasks_below_level(self, level):
        return self._hanlder.count_pending_tasks_below_level(level)

    def log_tasks(self):
        rows, _ = self._hanlder.query(query_type=EQueryType.TASKS)

        self.info("# tasks:")
        for row in rows:
            self.info(row)

    def get_tasks(self, order_by=None):
        return self._hanlder.get_tasks(order_by=order_by)

    def get_jobs(self):
        return self._hanlder.get_jobs()

    def update_task_start_time(self, task: Task):
        self._hanlder.update_task_start_time(task)

    def update_task_status(self, task: Task, status: EStatus):
        self._hanlder.update_task_status(task, status)

    def _run_task(self, task: Task):
        # get entry point func to execute
        ep = task.entrypoint
        if ep == 'ataskq.skip_run_task':
            self.info(f"task '{task.task_id}' is marked as 'skip_run_task', skipping run task.")
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

        # get state kwargs for kwarg in function signature
        ep_state_kwargs = {param.name: param.default for param in signature(
            func).parameters.values() if param.default is not param.empty and param.name in self._state_kwargs}

        for name, default in ep_state_kwargs.items():
            state_kwarg = self._state_kwargs.get(name)
            if isinstance(state_kwarg, StateKWArg):
                # state kwargs not initalized yet
                assert default is None, f"state kwarg '{name}' default value must be None"
                self._logger.info(f"Initializing state kwarg '{name}'")
                try:
                    self._state_kwargs[name] = state_kwarg.call()
                except EntryPointRuntimeError as ex:
                    self._logger.info(f"Failed to initialize state kwarg '{name}'")
                    self.update_task_status(task, EStatus.FAILURE)

                    if self._run_task_raise_exception:
                        raise ex
                    return
            # state kwargs already inistliazed
            self._logger.info(f"Update kwarg '{name}' with state kwargs")
            targs[1][name] = self._state_kwargs[name]

        # update task start time
        self.update_task_start_time(task)

        # run task
        monitor = MonitorThread(task, self, pulse_interval=self._monitor_pulse_interval)
        monitor.start()

        try:
            func(*targs[0], **targs[1])
            status = EStatus.SUCCESS
        except Exception as ex:
            if self._run_task_raise_exception:  # for debug purposes only
                self.warning("Running task entry point failed with exception.")
                self.update_task_status(task, EStatus.FAILURE)
                monitor.stop()
                monitor.join()
                raise ex

            self.warning("Running task entry point failed with exception.", exc_info=True)
            status = EStatus.FAILURE

        monitor.stop()
        monitor.join()
        self.update_task_status(task, status)

    @staticmethod
    def init_state_kwarg(self, state_kwarg: StateKWArg):
        return

    def _run(self, level):
        # make sure all state kwargs for job have key in self._state_kwargs, later to be used in _run_task
        state_kwargs_db = self._hanlder.get_state_kwargs()
        for skw in state_kwargs_db:
            if skw.name not in self._state_kwargs:
                self._state_kwargs[skw.name] = skw

        # check for error code
        while True:
            # if the taskq handler is db handler, the taskq performs background tasks before each run
            if isinstance(self._hanlder, DBHandler):
                self._hanlder.fail_pulse_timeout_tasks(self._task_pulse_timeout)
            # grab tasks and set them in Q
            action, task = self._hanlder._take_next_task(level)

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
            level = range(level, level + 1)
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
