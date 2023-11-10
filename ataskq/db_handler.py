from enum import Enum
from datetime import datetime, timedelta
import pickle
import sqlite3
from typing import List, Tuple
from pathlib import Path
from io import TextIOWrapper
import re
from typing import NamedTuple
import time

from .models import Job, StateKWArg, Task, EStatus
from .logger import Logger
from . import __schema_version__


class EQueryType(str, Enum):
    TASKS = 'tasks',
    TASKS_STATUS = 'tasks_status',
    STATE_KWARGS = 'state_kwargs',
    JOBS = 'jobs',
    JOBS_STATUS = 'jobs_status',


class EAction(str, Enum):
    RUN_TASK = 'run_task'
    WAIT = 'wait'
    STOP = 'stop'


class SqliteConnection(NamedTuple):
    path: str

    def __str__(self):
        return f"sqlite://{self.path}"

class PostgresConnection(NamedTuple):
    user: None or str 
    password: None or str
    host: str
    port: int
    database: str

    def __str__(self):
        if self.user:
            userspec = f"{self.user}" + (self.password and f':{self.password}') + '@'

        return f"postgresql://{userspec}{self.host}:{self.port}/{self.database}"


def from_sqlite_connection_str(db):
    format = 'sqlite://path'
    pattern = r'sqlite://(?P<path>.+)$'
    match = re.match(pattern, db)

    if not match:
        raise Exception(f"db must be in '{format}', ex: 'sqlite://ataskq.db.sqlite3'")

    path = match.group('path')
    ret = SqliteConnection(path=path)

    return ret



def from_postgres_connection_str(db):
    # https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING-URIS
    # todo: add params spec support
    format = 'postgresql://[user[:password]@][host][:port][/database]'
    pattern = r'postgresql://(?P<user>[^:@]+)(:(?P<password>[^@]+))?@?(?P<host>[^:/]+)(:(?P<port>\d+))?/(?P<database>.+)$'

    match = re.match(pattern, db)

    if not match:
        raise Exception(f"db must be in '{format}', ex: 'postgresql://user:password@localhost:5432/mydb'")

    user = match.group('user')
    password = match.group('password')
    host = match.group('host')
    port = match.group('port')
    database = match.group('database')
    ret = PostgresConnection(user=user, password=password, host=host, port=port, database=database)

    return ret


class SqliteConnection(NamedTuple):
    type: str
    path: str


def transaction_decorator(func):
    def wrapper(self, *args, **kwargs):
        with self.connect() as conn:
            c = conn.cursor()
            try:
                # enable foreign keys for each connection (sqlite default is off)
                # https://www.sqlite.org/foreignkeys.html
                # Foreign key constraints are disabled by default (for backwards
                # compatibility), so must be enabled separately for each database
                # connection
                # c.execute('PRAGMA foreign_keys = ON')
                ret = func(self, c, *args, **kwargs)
            except Exception as e:
                conn.commit()
                self.error(f"Failed to execute transaction: {e}")
                raise e

        conn.commit()
        return ret

    return wrapper


class DBHandler(Logger):
    def __init__(self, db='sqlite://', job_id=None, max_jobs=None, logger=None) -> None:
        super().__init__(logger)

        sep = '://'
        sep_index = db.find(sep)
        if sep_index == -1:
            raise RuntimeError(f'db must be of format <type>://<connection string>')
        self._db = db
        self._db_type = db[:sep_index]
        if self._db_type == 'sqlite':
            self._db_conn = from_sqlite_connection_str(db)
        elif self._db_type == 'postgresql':
            self._db_conn = from_postgres_connection_str(db)
        elif not self._db_type:
            raise Exception(f'missing db type, db must be of format <type>://<connection string>')

        self._max_jobs = max_jobs
        self._templates_dir = Path(__file__).parent / 'templates'
        self._job_id = job_id

    @property
    def job_id(self):
        return self._job_id
    
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
            conn = sqlite3.connect(self._db_conn)
        elif self._db_type == "postgresql":
            import psycopg2
            conn =  psycopg2.connect(
            host=self.db_conn.host,
            database=self.db_conn.database,
            user=self.db_conn.user,
            password=self.db_conn.password) 

        else:
            raise RuntimeError(f"Unsupported db '{self._db}'.")

        return conn

    @transaction_decorator
    def create_job(self, c, name='', description=''):
        if self._job_id is not None:
            raise RuntimeError(f"Job already assigned with job_id '{self._job_id}'.")

        bytes_types = 'BYTEA' if self._db_type == 'postgresql' else 'MEDIUMBLOB'
        primary_key = 'SERIAL PRIMARY KEY' if self._db_type == 'postgresql' else 'INTEGER PRIMARY KEY AUTOINCREMENT'

        # Create schema version table if not exists
        c.execute("CREATE TABLE IF NOT EXISTS schema_version ("
                  "version INTEGER PRIMARY KEY"
                  ")")
        c.execute("SELECT * FROM schema_version")
        current_schema_version = c.fetchone()
        if current_schema_version is None:
            c.execute(f"INSERT INTO schema_version (version) VALUES ({__schema_version__})")
        else:
            current_schema_version = current_schema_version[0]
            assert current_schema_version == __schema_version__, f"Schema version mismatch, current schema version is {current_schema_version} while code schema version is {__schema_version__}"
    
        # Create jobs table if not exists
        c.execute("CREATE TABLE IF NOT EXISTS jobs ("
                  f"job_id {primary_key}, "
                  "name TEXT, "
                  "description TEXT, "
                  "priority REAL DEFAULT 0"
                  #   "summary_cookie_keys JSON"
                  ")")

        # Create state arguments table if not exists
        c.execute("CREATE TABLE IF NOT EXISTS state_kwargs ("
                  f"state_kwargs_id {primary_key}, "
                  "name TEXT, "
                  "entrypoint TEXT NOT NULL, "
                  f"targs {bytes_types}, "
                  "description TEXT, "
                  "job_id INTEGER NOT NULL, "
                  "CONSTRAINT uq_name_job_id UNIQUE(name, job_id), "
                  "CONSTRAINT fk_job_id FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE"
                  ")")

        # Create tasks table if not exists
        statuses = ", ".join([f'\"{a}\"' for a in EStatus])
        c.execute(f"CREATE TABLE IF NOT EXISTS tasks ("
                  f"task_id {primary_key}, "
                  "name TEXT, "
                  "level REAL, "
                  "entrypoint TEXT NOT NULL, "
                  f"targs {bytes_types}, "
                  f"status TEXT ," # CHECK(status in ({statuses})), 
                  "take_time TIMESTAMP, "
                  "start_time TIMESTAMP, "
                  "done_time TIMESTAMP, "
                  "pulse_time TIMESTAMP, "
                  "description TEXT, "
                  #   "summary_cookie JSON, "
                  "job_id INTEGER NOT NULL, "
                  "CONSTRAINT fk_job_id FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE"
                  ")")

        # Create job and store job id
        c.execute(f"INSERT INTO jobs(name, description) VALUES ({name or 'NULL'}, {description or 'NULL'}) RETURNING job_id")
        # c.execute("SELECT last_insert_rowid()")
        self._job_id = c.fetchone()[0]

        if self._max_jobs is not None:
            c.execute(f"DELETE FROM jobs WHERE job_id NOT IN (SELECT job_id FROM jobs ORDER BY job_id DESC limit {self._max_jobs})")

        return self

    @transaction_decorator
    def delete_job(self, c, job_id=None):
        job_id = self._job_id if job_id is None else job_id
        c.execute(f"DELETE FROM jobs WHERE job_id = {job_id}")

    @transaction_decorator
    def add_state_kwargs(self, c, state_kwargs: List[StateKWArg] or StateKWArg):
        if self._job_id is None:
            raise RuntimeError(f"Job not assigned, pass job_id in __init__ or use create_job() first.")

        if isinstance(state_kwargs, StateKWArg):
            state_kwargs = [state_kwargs]

        # Insert data into a table
        # todo use some sql batch operation
        for skw in state_kwargs:
            assert skw.job_id is None
            skw.job_id = self._job_id

            if callable(skw.entrypoint):
                skw.entrypoint = f"{skw.entrypoint.__module__}.{skw.entrypoint.__name__}"

            if skw.targs is not None:
                assert len(skw.targs) == 2
                assert isinstance(skw.targs[0], tuple)
                assert isinstance(skw.targs[1], dict)
                skw.targs = pickle.dumps(skw.targs)
            d = {k: v for k, v in skw.__dict__.items() if 'state_kwargs_id' not in k}
            keys = list(d.keys())
            values = list(d.values())
            c.execute(
                f"INSERT INTO state_kwargs ({', '.join(keys)}) VALUES ({', '.join(['%s'] * len(keys))})", values)

        return self

    @transaction_decorator
    def add_tasks(self, c, tasks: List[Task] or Task):
        if self._job_id is None:
            raise RuntimeError(f"Job not assigned, pass job_id in __init__ or use create_job() first.")

        if isinstance(tasks, (Task)):
            tasks = [tasks]

        # Insert data into a table
        # todo use some sql batch operation
        for t in tasks:
            assert t.job_id is None
            t.job_id = self._job_id

            if callable(t.entrypoint):
                t.entrypoint = f"{t.entrypoint.__module__}.{t.entrypoint.__name__}"

            if t.targs is not None:
                assert len(t.targs) == 2
                assert isinstance(t.targs[0], tuple)
                assert isinstance(t.targs[1], dict)
                t.targs = pickle.dumps(t.targs)
            d = {k: v for k, v in t.__dict__.items() if 'task_id' not in k}
            keys = list(d.keys())
            values = list(d.values())
            c.execute(
                f'INSERT INTO tasks ({", ".join(keys)}) VALUES ({", ".join(["%s"] * len(keys))})', values)

        return self

    def task_query(self):
        query = f'SELECT * FROM tasks WHERE job_id = {self._job_id};'
        return query

    def state_kwargs_query(self):
        query = f'SELECT * FROM state_kwargs WHERE job_id = {self._job_id};'
        return query

    def tasks_status_query(self):
        query = "SELECT level, name," \
            "COUNT(*) as total, " + \
            ",".join(
                [f"SUM(CASE WHEN status = '{status}' THEN 1 ELSE 0 END) AS {status} " for status in EStatus]
            ) + \
            f"FROM tasks WHERE job_id = {self._job_id} " \
            "GROUP BY level, name;"

        return query

    def jobs_query(self):
        query = f'SELECT * FROM jobs;'
        return query

    def jobs_status_query(self):
        query = "SELECT jobs.job_id, jobs.name, jobs.description, jobs.priority, " \
            "COUNT(*) as tasks, " + \
            ",".join(
                [f"SUM(CASE WHEN status = '{status}' THEN 1 ELSE 0 END) AS {status} " for status in EStatus]
            ) + \
            f"FROM jobs " \
            "LEFT JOIN tasks ON jobs.job_id = tasks.job_id;"

        return query

    @transaction_decorator
    def query(self, c, query_type=EQueryType.JOBS_STATUS):
        queries = {
            EQueryType.TASKS: self.task_query,
            EQueryType.TASKS_STATUS: self.tasks_status_query,
            EQueryType.STATE_KWARGS: self.state_kwargs_query,
            EQueryType.JOBS: self.jobs_query,
            EQueryType.JOBS_STATUS: self.jobs_status_query,
        }
        query = queries.get(query_type)

        if query is None:
            # should never get here
            raise RuntimeError(f"Unknown status type: {query_type}")

        query_str = query()
        c.execute(query_str)
        rows = c.fetchall()
        col_names = [description[0] for description in c.description]
        # col_types = [description[1] for description in c.description]

        return rows, col_names

    def get_tasks(self):
        rows, col_names = self.query(query_type=EQueryType.TASKS)
        tasks = [Task(**dict(zip(col_names, row))) for row in rows]

        return tasks

    def get_state_kwargs(self):
        rows, col_names = self.query(query_type=EQueryType.STATE_KWARGS)
        tasks = [StateKWArg(**dict(zip(col_names, row))) for row in rows]

        return tasks

    def get_jobs(self):
        rows, col_names = self.query(query_type=EQueryType.JOBS)
        jobs = [Job(**dict(zip(col_names, row))) for row in rows]

        return jobs

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
            f"SELECT COUNT(*) FROM tasks WHERE level < {level} AND status in ('{EStatus.PENDING}')")

        row = c.fetchone()
        return row[0]

    @transaction_decorator
    def _set_timeout_tasks(self, c, timeout_sec):
        # set timeout tasks
        last_valid_pulse = datetime.now() - timedelta(seconds=timeout_sec)
        c.execute(
            f"UPDATE tasks SET status = '{EStatus.FAILURE}' WHERE pulse_time < '{last_valid_pulse}'::timestamp AND status NOT IN ('{EStatus.SUCCESS}', '{EStatus.FAILURE}');")

    @transaction_decorator
    def _take_next_task(self, c, level) -> Tuple[EAction, Task]:
        if self._db_type == 'sqlite':
            c.execute('BEGIN EXCLUSIVE;')
        else:
            # todo: add FOR UPDATE in the queries
            c.execute('BEGIN;')

        level_query = f' AND level >= {level.start} AND level < {level.stop}' if level is not None else ''
        # get pending task with minimum level
        c.execute(f"SELECT * FROM tasks WHERE status IN ('{EStatus.PENDING}'){level_query} AND level = "
                  f"(SELECT MIN(level) FROM tasks WHERE status IN ('{EStatus.PENDING}'){level_query});")
        row = c.fetchone()
        if row is None:
            ptask = None
        else:
            col_names = [description[0] for description in c.description]
            ptask = Task(**dict(zip(col_names, row)))

        # get running task with minimum level
        c.execute(f"SELECT * FROM tasks WHERE status IN ('{EStatus.RUNNING}'){level_query} AND level = "
                  f"(SELECT MIN(level) FROM tasks WHERE status IN ('{EStatus.RUNNING}'){level_query});")
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
                    f'Running task with level higher than pending detected, taking pending. running id: {rtask.task_id}, pending id: {ptask.task_id}.')
                action = EAction.RUN_TASK
            else:
                action = EAction.RUN_TASK

        if action == EAction.RUN_TASK:
            now = datetime.now()
            c.execute(
                f"UPDATE tasks SET status = '{EStatus.RUNNING}', take_time = '{now}'::timestamp, pulse_time = '{now}'::timestamp WHERE task_id = {ptask.task_id};")
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
            f"UPDATE tasks SET start_time = '{time}'::timestamp WHERE task_id = {task.task_id};")
        task.start_time = time

    @transaction_decorator
    def update_task_status(self, c, task, status):
        now = datetime.now()
        if status == EStatus.RUNNING:
            # for running task update pulse_time
            c.execute(
                f"UPDATE tasks SET status = '{status}', pulse_time = '{now}'::timestamp WHERE task_id = {task.task_id}")
            task.status = status
            task.pulse_time = now
        elif status == EStatus.SUCCESS or status == EStatus.FAILURE:
            # for done task update pulse_time and done_time time as well
            c.execute(
                f"UPDATE tasks SET status = '{status}', pulse_time = '{now}'::timestamp, done_time = '{now}'::timestamp WHERE task_id = {task.task_id}")
            task.status = status
            task.pulse_time = now
        else:
            raise RuntimeError(
                f"Unsupported status '{status}' for status update")
