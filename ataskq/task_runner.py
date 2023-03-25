import pickle
from errno import ESTALE
from pathlib import Path
import shutil
import hashlib
from typing import List
import sqlite3
import logging
from importlib import import_module
from enum import Enum
from datetime import datetime

import plyvel

from .logger import Logger
from collections import namedtuple


class EStatus(str, Enum):
    PENDING = 'pending'
    RUNNING = 'running'
    SUCCESS = 'success'
    FAILURE = 'failure'


# python 3.6 default factory

class Task:
    def __init__(self, level: float, entrypoint: str, targs: tuple or None = None, status: EStatus = EStatus.PENDING, take_time = None, tid = None) -> None:
        self.tid = tid
        self.level = level
        self.entrypoint = entrypoint
        self.targs = targs
        self.status = status
        self.take_time = take_time

    def db_vals(self):
        return (self.level, self.entrypoint, self.targs, self.status.value)

class TaskRunner(Logger):
    def __init__(self, job_path="./ataskqjob", run_task_raise_exception=False, logger: logging.Logger or None=None) -> None:
        super().__init__(logger)
        self._job_path = Path(job_path)
        self._taskdb = self._job_path / 'tasks.db'
        self._keyvaldb = self._job_path / 'keyvalue.db'
        self._run_task_raise_exception = run_task_raise_exception
        

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
        plyvel.DB(str(self._keyvaldb), create_if_missing=True)

        return self

    def add_tasks(self, tasks: List[Task] or Task):
        if isinstance(tasks, (Task)):
            tasks = [tasks]

        keyvaldb = plyvel.DB(str(self._keyvaldb))
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
                        keyvaldb.put(key_hash, pickle.dumps(t.targs))
                        t.targs = key_hash
                    keys = list(t.__dict__.keys())
                    values = list(t.__dict__.values())
                    c.execute(f'INSERT INTO tasks ({", ".join(keys)}) VALUES ({", ".join(["?"] * len(keys))})', values)
        keyvaldb.close()
        return self

    def log_tasks(self):
        with sqlite3.connect(str(self._taskdb)) as conn:
            # Start a transaction
            with conn:
                # Create a cursor object
                c = conn.cursor()

                c.execute('SELECT * FROM tasks')
                rows = c.fetchall()

                for row in rows:
                    self.info(row)
    
    def _take_next_task(self, set_running=True):
        with sqlite3.connect(str(self._taskdb)) as conn:
            # Start a transaction
            with conn:
                # Create a cursor object
                c = conn.cursor()

                c.execute('BEGIN EXCLUSIVE')

                # get task with minimum level not Done or Running
                c.execute(f'SELECT * FROM tasks WHERE status IN ("{EStatus.PENDING}") AND level = '
                          f'(SELECT MIN(level) FROM tasks WHERE status IN ("{EStatus.PENDING}"))')
                row = c.fetchone()
                if row is None:
                    conn.commit()
                    return None

                tid = row[0]
                task = Task(*row[1:], tid = tid) # 0 is index

                c = conn.cursor()
                if set_running:
                    c.execute(f'UPDATE tasks SET status = "{EStatus.RUNNING}", take_time="{datetime.now()}" WHERE tid = {task.tid}')
                    task.status = EStatus.RUNNING
                
                # end execution
                conn.commit()
        
        return task

    def get_next_task(self):
        return self._take_next_task(set_running=False)

    def update_task_status(self, task, status):
        # update status
        with sqlite3.connect(str(self._taskdb)) as conn:
            # Start a transaction
            with conn:
                # Create a cursor object
                c = conn.cursor()
                c.execute(f'UPDATE tasks SET status = "{status}" WHERE tid = {task.tid}')
        task.status = status

        
    def run_task(self, task):
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
            keyvaldb = plyvel.DB(str(self._keyvaldb))
            targs = pickle.loads(keyvaldb.get(task.targs))
            keyvaldb.close()
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
        task = self._take_next_task()
        while task:
            # self.log_tasks()
            self.run_task(task)
            task = self._take_next_task()
