import multiprocessing
import pickle
from pathlib import Path
import logging
from importlib import import_module
from multiprocessing import Process
import time
import shutil

from .logger import Logger
from .task import EStatus
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
    def __init__(self, job_path="./ataskqjob", run_task_raise_exception=False, task_wait_interval=0.2, monitor_pulse_interval = 60, logger: logging.Logger or None=None) -> None:
        """
        Args:
        task_wait_interval: pulling interval for task to complete in seconds.
        monitor_pulse_interval: update interval for pulse in seconds while taks is running.
        run_task_raise_exception: if True, run_task will raise exception when task fails. This is for debugging purpose only and will fail production flow.
        """
        super().__init__(logger)

        # init db handler
        self._job_path = Path(job_path)
        self._db_handler = DBHandler(f'sqlite://{self.job_path}/tasks.sqlite.db')
            
        self._run_task_raise_exception = run_task_raise_exception
        self._task_wait_interval = task_wait_interval
        self._monitor_pulse_interval = monitor_pulse_interval

        self._running = False        
    
    @property
    def job_path(self):
        return self._job_path

    @property
    def db_handler(self):
        return self._db_handler

    @property
    def task_wait_interval(self):
        return self._task_wait_interval

    @property
    def monitor_pulse_interval(self):
        return self._monitor_pulse_interval

    def create_job(self, parents=False, overwrite=False):
        job_path = self._job_path

        if job_path.exists() and overwrite:
            shutil.rmtree(job_path)
        elif job_path.exists():
            self.warning(f"job path '{job_path}' already exists.")
            return self

        job_path.mkdir(parents=parents)
        (job_path / '.ataskqjob').write_text('')

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

    def update_task_start_time(self, task):
        self._db_handler.update_task_start_time(task)

    def update_task_status(self, task, status):
        self._db_handler.update_task_status(task, status)

    def _run_task(self, task):
        # get entry point func to execute
        ep = task.entrypoint
        assert '.' in ep, 'entry point must be inside a module.'
        module_name, func_name = ep.rsplit('.', 1)
        try:
            m = import_module(module_name)
        except ImportError as ex:
            raise RuntimeError(f"Failed to load module '{module_name}'. Exception: '{ex}'")
        assert hasattr(m, func_name), f"failed to load entry point, module '{module_name}' doen't have func named '{func_name}'."
        func = getattr(m, func_name)
        assert callable(func), f"entry point is not callable, '{module_name},{func}'."
        
        # get targs
        if task.targs is not None:
            try:    
                targs = pickle.loads(task.targs)
            except Exception as ex:
                # failed to load targs, report task failure and return
                self.info("Getting tasks args failed.", exc_info=True)
                self.update_task_status(task, EStatus.FAILURE)

                if  self._run_task_raise_exception: # for debug purposes only
                    raise ex

                return
        else:
            targs = ((), {})


        # update task start time
        self.update_task_start_time(task)

        # run task
        monitor = MonitorThread(task, self, pulse_interval=self._monitor_pulse_interval)
        monitor.start()
        
        ex = None
        try:
            func(*targs[0], **targs[1])  
            status = EStatus.SUCCESS
        except Exception as e:
            self.info("Running task entry point failed with exception.", exc_info=True)
            ex = e
            status = EStatus.FAILURE
        
        monitor.stop()
        monitor.join()
        self.update_task_status(task, status)

        if ex is not None and self._run_task_raise_exception: # for debug purposes only
            raise ex

    def _run(self, level):
        # check for error code
        while True:
            # grab tasks and set them in Q
            action, task = self._db_handler._take_next_task(level)

            # handle no task available
            if action == EAction.STOP:
                break
            if action == EAction.RUN_TASK:
                self._run_task(task)
            elif action == EAction.WAIT:
                self.debug(f"waiting for {self._task_wait_interval} sec before taking next task")
                time.sleep(self._task_wait_interval)
    
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