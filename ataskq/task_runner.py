
from pathlib import Path
import shutil
from typing import List
import sqlite3
import logging
from importlib import import_module
from enum import Enum

from .logger import Logger
from collections import namedtuple

Task = namedtuple('Task', ('level', 'entrypoint', 'status'))

class EStatus(str, Enum):
    NONE = 'none'
    CREATED = 'created'
    RUNNING = 'running'
    SUCCESS = 'success'
    FAILURE = 'failure'
    TIMEDOUT = 'timedout'

class TaskRunner(Logger):
    def __init__(self, job_path="./ataskqjob", logger: logging.Logger or None=None) -> None:
        super().__init__(logger)
        self._job_path = Path(job_path)
        self._taskdb = self._job_path / 'tasks.db'

    @property
    def job_path(self):
        return self._job_path

    def create_job(self, parents=False, overwrite=False):
        job_path = self._job_path

        if job_path.exists() and overwrite:
            shutil.rmtree(job_path)
        elif job_path.exists():
            self.warn(f"job path '{job_path}' already exists.")
            return

        job_path.mkdir(parents=parents)
        (job_path / '.ataskqjob').write_text('')

        self.info(f"Create db '{self._taskdb}'.")
        with sqlite3.connect(str(self._taskdb)) as conn:
            # Start a transaction
            with conn:
                # Create a cursor object
                c = conn.cursor()

                # Create tasks table
                statuses = ", ".join([f'\"{a}\"' for a in EStatus])
                c.execute(f"CREATE TABLE tasks ("\
                    "id INTEGER PRIMARY KEY, "\
                    "level REAL, "\
                    "entrypoint TEXT NOT NULL, "\
                    f"status TEXT CHECK(status in ({statuses}))"\
                ")")

        return self

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
                    c.execute(f'INSERT INTO tasks {t._fields} VALUES ({", ".join(["?"] * len(t._fields))})', t)
        
        return self

    def get_tasks(self):
        with sqlite3.connect(str(self._taskdb)) as conn:
            # Start a transaction
            with conn:
                # Create a cursor object
                c = conn.cursor()

                c.execute('SELECT * FROM tasks')
                rows = c.fetchall()

                for row in rows:
                    print(row)
        
    def run_next(self):
        with sqlite3.connect(str(self._taskdb)) as conn:
            # Start a transaction
            with conn:
                # Create a cursor object
                c = conn.cursor()

                # get task with minimum level not Done or Running
                c.execute('SELECT * FROM tasks WHERE level = '
                          f'(SELECT MIN(level) FROM tasks WHERE status IN ("{EStatus.CREATED}"))')
                row = c.fetchone()
                task_id = row[0]
                task = Task(*row[1:]) # 0 is index

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
        
        try:
            func()  
            status = EStatus.SUCCESS
        except Exception as ex:
            self.info("running task entry point failed with exception.", ex)
            status = EStatus.FAILURE

        # update status
        with sqlite3.connect(str(self._taskdb)) as conn:
            # Start a transaction
            with conn:
                # Create a cursor object
                c = conn.cursor()
                c.execute(f'UPDATE tasks SET status = "{status}" WHERE id = {task_id}')



    def run(self):
        self.run_next()
        self.run_next()
