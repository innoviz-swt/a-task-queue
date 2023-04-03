import multiprocessing
import pickle
from pathlib import Path
import shutil
import hashlib
from typing import List, Tuple
import sqlite3
import logging
from importlib import import_module
from enum import Enum
from datetime import datetime
from multiprocessing import Process
import time

import rocksdb3

from .logger import Logger


class EStatus(str, Enum):
    PENDING = 'pending'
    RUNNING = 'running'
    SUCCESS = 'success'
    FAILURE = 'failure'


class EAction(str, Enum):
    RUN_TASK = 'run_task'
    WAIT = 'wait'
    STOP = 'stop'


def keyval_store_retry(retries=1000, polling_delta=0.1):
    def decorator(func):
        def wrapper(self, *args, **kwargs):
            for i in range(retries):
                try:
                    ret = func(self, *args, **kwargs)
                    # self.info(f' success in {i} iteration.')
                    return ret
                # except plyvel.IOError as ex:
                except rocksdb3.RocksDBError:
                    if (i != 0 and i % 100 == 0):
                        self.warning(f'keyval store retry {i} iteration.')
                    time.sleep(polling_delta)
                    continue
            raise RuntimeError(f"Failed keyval store retry retry. retries: {retries}, polling_delta: {polling_delta}.")
        return wrapper
    return decorator



class Task:
    def __init__(self, tid = None, level: float = -1, entrypoint: str = "", targs: tuple or None = None, status: EStatus = EStatus.PENDING, take_time = None) -> None:
        self.tid = tid
        self.level = level
        self.entrypoint = entrypoint
        self.targs = targs
        self.status = status
        self.take_time = take_time

    def db_vals(self):
        return (self.level, self.entrypoint, self.targs, self.status.value)

class TaskRunner(Logger):
    def __init__(self, job_path="./ataskqjob", run_task_raise_exception=False, task_wait_delta=0.2, logger: logging.Logger or None=None) -> None:
        super().__init__(logger)
        self._job_path = Path(job_path)
        self._taskdb = self._job_path / 'tasks.db'
        self._keyvaldb = self._job_path / 'keyvalue.db'
        self._run_task_raise_exception = run_task_raise_exception
        self._task_wait_delta = task_wait_delta
        

    @property
    def job_path(self):
        return self._job_path

    def create_job(self, parents=False, overwrite=False):
        job_path = self._job_path

        if job_path.exists() and overwrite:
            shutil.rmtree(job_path)
        elif job_path.exists():
            self.warning(f"job path '{job_path}' already exists.")
            return self

        job_path.mkdir(parents=parents)
        (job_path / '.ataskqjob').write_text('')

        # task db
        self.info(f"Create db '{self._taskdb}'.")
        with sqlite3.connect(str(self._taskdb)) as conn:
            # Start a transaction
            with conn:
                # Create a cursor object
                c = conn.cursor()

                # Create tasks table
                statuses = ", ".join([f'\"{a}\"' for a in EStatus])
                c.execute(f"CREATE TABLE tasks ("\
                    "tid INTEGER PRIMARY KEY, "\
                    "level REAL, "\
                    "entrypoint TEXT NOT NULL, "\
                    "targs BLOB, "\
                    f"status TEXT CHECK(status in ({statuses})),"\
                    "take_time DATETIME"\
                ")")

        # key value store
        # keyvaldb = plyvel.DB(str(self._keyvaldb), create_if_missing=True)
        # keyvaldb.close()
        rocksdb3.open_default(str(self._keyvaldb))


        return self

    @keyval_store_retry()
    def _put_keyval(self, key, val):
        # keyvaldb = plyvel.DB(str(self._keyvaldb))
        # keyvaldb.put(key, val)
        # keyvaldb.close()
        # assert keyvaldb.closed

        db = rocksdb3.open_default(str(self._keyvaldb))
        db.put(key, val)

    
    @keyval_store_retry()
    def _get_keval(self, key):
        # keyvaldb = plyvel.DB(str(self._keyvaldb))
        # val = keyvaldb.get(key)
        # keyvaldb.close()
        # assert keyvaldb.closed

        db = rocksdb3.open_default(str(self._keyvaldb))
        val = db.get(key)

        return val

    def add_tasks(self, tasks: List[Task] or Task):
        if isinstance(tasks, (Task)):
            tasks = [tasks]

        with sqlite3.connect(str(self._taskdb)) as conn:
            # Start a transaction
            with conn:
                # Create a cursor object
                c = conn.cursor()

                # Insert data into a table
                # todo use some sql batch operation
                for t in tasks:
                    # hanlde task
                    if t.targs is not None:
                        assert len(t.targs) == 2
                        assert isinstance(t.targs[0], tuple)
                        assert isinstance(t.targs[1], dict)
                        data = pickle.dumps(t.targs)
                        key_hash = hashlib.md5(data).digest() 
                        self._put_keyval(key_hash, pickle.dumps(t.targs)) 
                        t.targs = key_hash
                    keys = list(t.__dict__.keys())
                    values = list(t.__dict__.values())
                    c.execute(f'INSERT INTO tasks ({", ".join(keys)}) VALUES ({", ".join(["?"] * len(keys))})', values)
        return self

    def log_tasks(self):
        with sqlite3.connect(str(self._taskdb)) as conn:
            # Start a transaction
            with conn:
                # Create a cursor object
                c = conn.cursor()

                c.execute('SELECT * FROM tasks')
                rows = c.fetchall()

        self.info("tasks:")
        for row in rows:
            self.info(row)
    
    def _take_next_task(self) -> Tuple[EAction, Task]:
        with sqlite3.connect(str(self._taskdb)) as conn:
            # Start a transaction
            with conn:
                # Create a cursor object
                c = conn.cursor()

                c.execute('BEGIN EXCLUSIVE;')

                # get pending task with minimum level
                c.execute(f'SELECT * FROM tasks WHERE status IN ("{EStatus.PENDING}") AND level = '
                          f'(SELECT MIN(level) FROM tasks WHERE status IN ("{EStatus.PENDING}"));')
                row = c.fetchone()
                ptask = row if row is None else Task(*row)

                # get running task with minimum level
                c.execute(f'SELECT * FROM tasks WHERE status IN ("{EStatus.RUNNING}") AND level = '
                          f'(SELECT MIN(level) FROM tasks WHERE status IN ("{EStatus.RUNNING}"));')
                row = c.fetchone()
                rtask = row if row is None else Task(*row)

                action = None
                if ptask is None and rtask is None:
                    # no more pending task, no more running tasks
                    action = EAction.STOP
                elif ptask is None and rtask is not None:
                    # no more pending tasks, tasks still running
                    action = EAction.WAIT
                elif ptask is not None and rtask is None:
                    # pending task next, no more running tasks 
                    action = EAction.RUN_TASK
                elif ptask is not None and rtask is not None:
                    if ptask.level > rtask.level:
                        # pending task with level higher than running (wait for running to end)
                        action = EAction.WAIT
                    elif rtask.level > ptask.level:
                        # should never happend
                        # running task with level higher than pending task (warn and take next task)
                        self.warning(f'Running task with level higher than pending detected, taking pending. running id: {rtask.tid}, pending id: {ptask.tid}.')
                        action = EAction.RUN_TASK
                    else:
                        action = EAction.RUN_TASK

                if action == EAction.RUN_TASK:
                    c.execute(f'UPDATE tasks SET status = "{EStatus.RUNNING}", take_time = "{datetime.now()}" WHERE tid = {ptask.tid};')
                    ptask.status = EStatus.RUNNING
                    task = ptask
                elif action == EAction.WAIT:
                    task = None
                elif action == EAction.STOP:
                    task = None
                else:
                    raise RuntimeError(f"Unsupported action '{EAction}'")

                # end execution
                conn.commit()
        
        # self.log_tasks()
        return action, task

    def update_task_status(self, task, status):
        # update status
        with sqlite3.connect(str(self._taskdb)) as conn:
            # Start a transaction
            with conn:
                # Create a cursor object
                c = conn.cursor()
                c.execute(f'UPDATE tasks SET status = "{status}" WHERE tid = {task.tid}')
        task.status = status

        
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
            targs_data = self._get_keval(task.targs)
            targs = pickle.loads(targs_data)
        else:
            targs = ((), {})

        try:
            func(*targs[0], **targs[1])  
            status = EStatus.SUCCESS
        except Exception as ex:
            self.info("running task entry point failed with exception.", exc_info=True)
            status = EStatus.FAILURE
            if self._run_task_raise_exception:
                raise ex

        self.update_task_status(task, status)


    def run_all_sequential(self):
        action, task = self._take_next_task()
        while action != EAction.STOP:
            if action == EAction.RUN_TASK:
                self._run_task(task)
            else:
                raise RuntimeError("Run sequential should never hit action either than run or stop")
            action, task = self._take_next_task()

    def _multiprocess_run(self):
        # check for error code
        while True:
            # grab tasks and set them in Q
            action, task = self._take_next_task()

            # handle no task available
            if action == EAction.STOP:
                break
            if action == EAction.RUN_TASK:
                self._run_task(task)
            elif action == EAction.WAIT:
                self.debug(f"waiting for {self._task_wait_delta} sec before taking next task")
                time.sleep(self._task_wait_delta)

    def run_all_multiprocess(self, num_processes=0.9):
        assert isinstance(num_processes, (int, float))

        if isinstance(num_processes, float):
            assert 0.0 <= num_processes <= 1.0
            nprocesses = int(multiprocessing.cpu_count() * num_processes)
        elif num_processes < 0:
            nprocesses = multiprocessing.cpu_count() - num_processes 
        else:
            nprocesses = num_processes

        # set processes and Q
        processes = [Process(target=self._multiprocess_run, daemon=True) for i in range(nprocesses)]
        [p.start() for p in processes]

        # join all processes
        [p.join() for p in processes]

        # log failed processes 
        for p in processes:
            if p.exitcode != 0:
                self.error(f"Process '{p.pid}' failed with exitcode '{p.exitcode}'")
