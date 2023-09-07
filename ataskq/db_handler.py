from enum import Enum
from datetime import datetime, timedelta
import pickle
import sqlite3
from typing import List, Tuple
from pathlib import Path
from io import TextIOWrapper

from .task import Task, EStatus
from .logger import Logger
from . import __schema_version__

class EQueryType(Enum):
    TASKS_STATUS = 1,
    TASKS = 2,


class EAction(str, Enum):
    RUN_TASK = 'run_task'
    WAIT = 'wait'
    STOP = 'stop'


def transaction_decorator(func):
    def wrapper(self, *args, **kwargs):
        with self.connect() as conn:
            c = conn.cursor()
            try:
                ret = func(self, c, *args, **kwargs)
            except Exception as e:
                conn.commit()
                self.error(f"Failed to execute transaction: {e}")
                raise e

        conn.commit()
        return ret

    return wrapper


class DBHandler(Logger):
    def __init__(self, db='sqlite://', logger=None) -> None:
        super().__init__(logger)
        
        sep = '://'
        sep_index = db.find(sep)
        if sep_index == -1:
            raise RuntimeError(f'db must be of format <type>://<connection string>')
        self._db = db
        self._db_type = db[:sep_index]
        if not self._db_type:
            raise RuntimeError(f'missing db type, db must be of format <type>://<connection string>')
        self._db_conn = db[sep_index+len(sep):]
        if not self._db_conn:
            raise RuntimeError(f'missing db connection string, db must be of format <type>://<connection string>')

        self._templates_dir = Path(__file__).parent / 'templates'

    @property
    def db(self):
        return self._db

    @property
    def db_type(self):
        return self._db_type

    @property
    def db_conn(self):
        return self._db_conn

    @property
    def db_path(self):
        if self._db_type == "sqlite":
            return self._db_conn
        else:
            return None

    def connect(self):
        if self._db_type == "sqlite":
            conn = sqlite3.connect(self._db[9:])
        else:
            raise RuntimeError(f"Unsupported db '{self._db}'.")

        return conn

    @transaction_decorator
    def create_job(self, c):
        # Create schema version table if not exists
        c.execute("CREATE TABLE IF NOT EXISTS schema_version ("
                  "version INTEGER PRIMARY KEY"
                  ")")
        c.execute(f"INSERT INTO schema_version (version) VALUES ({__schema_version__})")

        # Create tasks table if not exists
        statuses = ", ".join([f'\"{a}\"' for a in EStatus])
        c.execute(f"CREATE TABLE IF NOT EXISTS tasks ("
                  "tid INTEGER PRIMARY KEY, "
                  "level REAL, "
                  "name TEXT, "
                  "entrypoint TEXT NOT NULL, "
                  "targs MEDIUMBLOB, "
                  f"status TEXT CHECK(status in ({statuses})),"
                  "take_time DATETIME,"
                  "start_time DATETIME,"
                  "done_time DATETIME,"
                  "pulse_time DATETIME,"
                  "description TEXT"
                  ")")

        return self

    @transaction_decorator
    def add_tasks(self, c, tasks: List[Task] or Task):
        if isinstance(tasks, (Task)):
            tasks = [tasks]

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
            c.execute(
                f'INSERT INTO tasks ({", ".join(keys)}) VALUES ({", ".join(["?"] * len(keys))})', values)

        return self

    def tasks_status_query(self):
        query = "SELECT level, name," \
            "COUNT(*) as total, " + \
            ",".join(
                [f"SUM(CASE WHEN status = '{status}' THEN 1 ELSE 0 END) AS {status} " for status in EStatus]
            ) + \
            "FROM tasks " \
            "GROUP BY level, name;"

        return query

    def task_query(self):
        query = 'SELECT * FROM tasks'
        return query

    @transaction_decorator
    def query(self, c, query_type=EQueryType.TASKS_STATUS):
        if query_type == EQueryType.TASKS_STATUS:
            query = self.tasks_status_query()
        elif query_type == EQueryType.TASKS:
            query = self.task_query()
        else:
            # should never get here
            raise RuntimeError(f"Unknown status type: {query_type}")

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

        # targs is byte array, so we need to limits its width
        targsi = col_names.index('targs') if 'targs' in col_names else -1
        def colstyle(i): return " class=targs-col" if i == targsi else ''
        ret = [
            '<table>',
            pad + '<tr>',
            *[pad + pad + f'<th{colstyle(i)}> ' + f'{col}' +
              ' </th>' for i, col in enumerate(col_names)],
            pad + '</tr>',
        ]

        for row in rows:
            ret += [
                pad + '<tr>',
                *[pad + pad + f"<td{colstyle(i)}> " + f'{col}' +
                  ' </td>' for i, col in enumerate(row)],
                pad + '</tr>',
            ]

        ret += ['</table>']

        table = "\n".join(ret)

        return table

    def html_table(self, query_type=EQueryType.TASKS_STATUS):
        """
        Return a html table of the status
        """
        rows, col_names = self.query(query_type)
        table = self.table(col_names, rows)

        return table

    def html(self, query_type, file=None):
        """
        Return a html of the status and write to file if given.
        """
        with open(self._templates_dir / 'base.html') as f:
            html = f.read()

        table = self.html_table(query_type)
        html = html.replace(
            '{{title}}', query_type.name.lower().replace('_', ' '))
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

    @transaction_decorator
    def count_pending_tasks_below_level(self, c, level) -> int:
        c.execute(
            f'SELECT COUNT(*) FROM tasks WHERE level < {level} AND status in ("{EStatus.PENDING}")')

        row = c.fetchone()
        return row[0]
    
    @transaction_decorator
    def _set_timeout_tasks(self, c, timeout_sec):
        # set timeout tasks
        last_valid_pulse = datetime.now() - timedelta(seconds=timeout_sec)
        c.execute(f'UPDATE tasks SET status = "{EStatus.FAILURE}" WHERE pulse_time < "{last_valid_pulse}" AND status NOT IN ("{EStatus.SUCCESS}", "{EStatus.FAILURE}");')

    @transaction_decorator
    def _take_next_task(self, c, level) -> Tuple[EAction, Task]:
        c.execute('BEGIN EXCLUSIVE;')

        level_query = f' AND level >= {level.start} AND level < {level.stop}' if level is not None else ''
        # get pending task with minimum level
        c.execute(f'SELECT * FROM tasks WHERE status IN ("{EStatus.PENDING}"){level_query} AND level = '
                  f'(SELECT MIN(level) FROM tasks WHERE status IN ("{EStatus.PENDING}"){level_query});')
        row = c.fetchone()
        if row is None:
            ptask = None
        else:
            col_names = [description[0] for description in c.description]
            ptask = Task(**dict(zip(col_names, row)))

        # get running task with minimum level
        c.execute(f'SELECT * FROM tasks WHERE status IN ("{EStatus.RUNNING}"){level_query} AND level = '
                  f'(SELECT MIN(level) FROM tasks WHERE status IN ("{EStatus.RUNNING}"){level_query});')
        row = c.fetchone()
        if row is None:
            rtask = None
        else:
            col_names = [description[0] for description in c.description]
            rtask = Task(**dict(zip(col_names, row)))

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
                self.warning(
                    f'Running task with level higher than pending detected, taking pending. running id: {rtask.tid}, pending id: {ptask.tid}.')
                action = EAction.RUN_TASK
            else:
                action = EAction.RUN_TASK

        if action == EAction.RUN_TASK:
            now = datetime.now()
            c.execute(
                f'UPDATE tasks SET status = "{EStatus.RUNNING}", take_time = "{now}", pulse_time = "{now}" WHERE tid = {ptask.tid};')
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
    
        # self.log_tasks()
        return action, task

    @transaction_decorator
    def update_task_start_time(self, c, task, time=None):
        if time is None:
            time = datetime.now()

        c.execute(
            f'UPDATE tasks SET start_time = "{time}" WHERE tid = {task.tid};')
        task.start_time = time

    @transaction_decorator
    def update_task_status(self, c, task, status):
        now = datetime.now()
        if status == EStatus.RUNNING:
            # for running task update pulse_time
            c.execute(
                f'UPDATE tasks SET status = "{status}", pulse_time = "{now}" WHERE tid = {task.tid}')
            task.status = status
            task.pulse_time = now
        elif status == EStatus.SUCCESS or status == EStatus.FAILURE:
            # for done task update pulse_time and done_time time as well
            c.execute(
                f'UPDATE tasks SET status = "{status}", pulse_time = "{now}", done_time = "{now}" WHERE tid = {task.tid}')
            task.status = status
            task.pulse_time = now
        else:
            raise RuntimeError(
                f"Unsupported status '{status}' for status update")
