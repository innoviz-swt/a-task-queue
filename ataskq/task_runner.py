from ast import Return
from errno import ESTALE
from io import TextIOWrapper
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

from .logger import Logger
from .task import Task, EStatus
from .task_monitor import MonitorThread


class EAction(str, Enum):
    RUN_TASK = 'run_task'
    WAIT = 'wait'
    STOP = 'stop'


class EQueryType(Enum):
    TASKS_SUMMARY = 1,
    NUM_UNITS_SUMMARY = 2,
    TASKS = 3,


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


class TaskRunner(Logger):
    def __init__(self, job_path="./ataskqjob", run_task_raise_exception=False, task_wait_interval=0.2, monitor_pulse_interval = 60, logger: logging.Logger or None=None) -> None:
        """
        task_wait_interval: pulling interval for task to complete in seconds.
        monitor_pulse_interval: update interval for pulse in seconds while taks is running.
        run_task_raise_exception: if True, run_task will raise exception when task fails. This is for debugging purpose only and will fail production flow.
        """
        super().__init__(logger)
        self._job_path = Path(job_path)
        self._taskdb = self._job_path / 'tasks.sqlite.db'
        self._run_task_raise_exception = run_task_raise_exception
        self._task_wait_interval = task_wait_interval
        self._monitor_pulse_interval = monitor_pulse_interval

        self._running = False
        self._templates_dir = Path(__file__).parent / 'templates'
        

    @property
    def job_path(self):
        return self._job_path

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

        # task db
        self.info(f"Create db '{self._taskdb}'.")
        with sqlite3.connect(str(self._taskdb)) as conn:
            # Start a transaction
            with conn:
                # Create a cursor object
                c = conn.cursor()

                # Create tasks table
                statuses = ", ".join([f'\"{a}\"' for a in EStatus])
                c.execute(f"CREATE TABLE tasks (" \
                    "tid INTEGER PRIMARY KEY, " \
                    "level REAL, " \
                    "name TEXT, " \
                    "entrypoint TEXT NOT NULL, " \
                    "targs MEDIUMBLOB, " \
                    f"status TEXT CHECK(status in ({statuses}))," \
                    "take_time DATETIME," \
                    "start_time DATETIME," \
                    "done_time DATETIME," \
                    "pulse_time DATETIME," \
                    "num_units INTEGER" \
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
                    # hanlde task
                    if t.targs is not None:
                        assert len(t.targs) == 2
                        assert isinstance(t.targs[0], tuple)
                        assert isinstance(t.targs[1], dict)
                        t.targs = pickle.dumps(t.targs)
                    keys = list(t.__dict__.keys())
                    values = list(t.__dict__.values())
                    c.execute(f'INSERT INTO tasks ({", ".join(keys)}) VALUES ({", ".join(["?"] * len(keys))})', values)
        
        return self

    def tasks_summary_query(self):
        query = "SELECT level, name," \
            "COUNT(*) as total, " + \
            ",".join(
                [f"SUM(CASE WHEN status = '{status}' THEN 1 ELSE 0 END) AS {status} " for status in  EStatus]
            ) + \
            "FROM tasks " \
            "GROUP BY level, name;"
        
        return query

    def num_units_summary_query(self):
        query = "SELECT level, name," \
                "SUM(num_units) as total, " + \
                ",".join(
                    [f"SUM(CASE WHEN status = '{status}' THEN num_units ELSE 0 END) AS {status} " for status in  EStatus]
                ) + \
                "FROM tasks " \
                "GROUP BY level, name;"
        
        return query

    def task_status_query(self):
        query = 'SELECT * FROM tasks'
        return query

    def query(self, query_type=EQueryType.TASKS_SUMMARY):
        with sqlite3.connect(str(self._taskdb)) as conn:
            # Start a transaction
            with conn:
                # Create a cursor object
                c = conn.cursor()
                if query_type == EQueryType.TASKS_SUMMARY:
                    query = self.tasks_summary_query()
                elif query_type == EQueryType.NUM_UNITS_SUMMARY:
                    query = self.num_units_summary_query()
                elif query_type == EQueryType.TASKS:
                    query = self.task_status_query()
                else:
                    # should never get here
                    raise RuntimeError(f"Unknown summary type: {query_type}")

                c.execute(query)
                rows = c.fetchall()
                col_names = [description[0] for description in c.description]
                # col_types = [description[1] for description in c.description]

        return rows, col_names

    @staticmethod
    def table(col_names, rows):
        """
        Return a html table
        """
        
        pad = '  '
        ret = [
            '<table>',
            pad + '<tr>',
            *[ pad + pad + '<th> ' + col + ' </th>' for col in col_names],
            pad + '</tr>',
        ]

        for row in rows:
            ret += [
                pad + '<tr>',
                *[ pad + pad + '<td> ' + f'{col}'+ ' </td>' for col in row],
                pad + '</tr>',
            ]

        ret += ['</table>']

        table = "\n".join(ret)
                    
        return table

    def html_table(self, query_type=EQueryType.TASKS_SUMMARY):
        """
        Return a html table of the summary
        """
        rows, col_names = self.query(query_type)
        table = self.table(col_names, rows)

        return table

    def html(self, query_type, file=None):
        """
        Return a html of the summary and write to file if given.
        """
        with open(self._templates_dir / 'base.html') as f:
            html = f.read()
        
        table = self.html_table(query_type)
        html = html.replace('{{title}}', query_type.name.lower().replace('_', ' '))
        html = html.replace('{{table}}', table)

        if file is not None:
            if isinstance(file, (str, Path)):
                with open(file, 'w') as f:
                    f.write(html)
            elif isinstance(file, TextIOWrapper):
                file.write(html)
            else:
                raise RuntimeError('file must by either path of file io')
        
        return html

    def log_tasks(self):
        rows, _ = self.query(query_type=EQueryType.TASKS)

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
                    now = datetime.now()
                    c.execute(f'UPDATE tasks SET status = "{EStatus.RUNNING}", take_time = "{now}", pulse_time = "{now}" WHERE tid = {ptask.tid};')
                    ptask.status = EStatus.RUNNING
                    ptask.take_time = now
                    ptask.pulse_time = now
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

    def update_task_start_time(self, task, time=None):
        if time is None:
            time = datetime.now()
        
        with sqlite3.connect(str(self._taskdb)) as conn:
            # Start a transaction
            with conn:
                # Create a cursor object
                c = conn.cursor()
                c.execute(f'UPDATE tasks SET start_time = "{time}" WHERE tid = {task.tid};')
                task.start_time = time

    def update_task_status(self, task, status):
        # update status
        with sqlite3.connect(str(self._taskdb)) as conn:
            # Start a transaction
            with conn:
                # Create a cursor object
                c = conn.cursor()
                now = datetime.now()
                if status == EStatus.RUNNING:
                    # for running task update pulse_time
                    c.execute(f'UPDATE tasks SET status = "{status}", pulse_time = "{now}" WHERE tid = {task.tid}')
                    task.status = status
                    task.pulse_time = now
                elif status == EStatus.SUCCESS or status == EStatus.FAILURE:
                    # for done task update pulse_time and done_time time as well
                    c.execute(f'UPDATE tasks SET status = "{status}", pulse_time = "{now}", done_time = "{now}" WHERE tid = {task.tid}')
                    task.status = status
                    task.pulse_time = now
                else:
                    raise RuntimeError(f"Unsupported status '{status}' for status update")

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

    def _run(self):
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
                self.debug(f"waiting for {self._task_wait_interval} sec before taking next task")
                time.sleep(self._task_wait_interval)

    def run(self, num_processes=None):
        self._running = True

        # default to run in current process
        if num_processes is None:
            self._run()
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
        processes = [Process(target=self._run, daemon=True) for i in range(nprocesses)]
        [p.start() for p in processes]

        # join all processes
        [p.join() for p in processes]

        # log failed processes 
        for p in processes:
            if p.exitcode != 0:
                self.error(f"Process '{p.pid}' failed with exitcode '{p.exitcode}'")

        self._running = False

        return self