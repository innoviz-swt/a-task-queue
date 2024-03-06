from datetime import datetime, timedelta
from typing import List, Tuple
from pathlib import Path
from io import TextIOWrapper
from abc import abstractmethod
from datetime import datetime

from ..imodel import IModel
from ..models import Model, StateKWArg, Task, EStatus
from ..handler import Handler, EAction
from .. import __schema_version__
from ..env import ATASKQ_DB_INIT_ON_HANDLER_INIT

CONNECTION_LOG = [None]


def set_connection_log(log):
    CONNECTION_LOG[0] = log


def transaction_decorator(exclusive=False):
    def decorator(func):
        def wrapper(self, *args, **kwargs):
            with self.connect() as conn:
                if CONNECTION_LOG[0] is not None:
                    conn.set_trace_callback(CONNECTION_LOG[0])

                c = conn.cursor()
                try:
                    if self.pragma_foreign_keys_on:
                        c.execute(self.pragma_foreign_keys_on)

                    if exclusive:
                        c.execute(self.begin_exclusive)
                    else:
                        c.execute("BEGIN")
                    # enable foreign keys for each connection (sqlite default is off)
                    # https://www.sqlite.org/foreignkeys.html
                    # Foreign key constraints are disabled by default (for backwards
                    # compatibility), so must be enabled separately for each database
                    # connection
                    ret = func(self, c, *args, **kwargs)
                    conn.commit()
                except Exception as e:
                    self.error(f"Failed to execute transaction '{type(e)}:{e}'. Rolling back")
                    conn.rollback()
                    raise e

            return ret

        return wrapper

    return decorator


def _field_with_order(f):
    if isinstance(f, (tuple, list)):
        if len(f) == 1:
            return f"{f[0]} ASC"
        assert len(f) == 2, "order_by tuple must be of format (field, order) where order default is ASC"

    else:
        return f"{f} ASC"
    return f"{f[0]} {f[1]}"


def order_query(order_by):
    if isinstance(order_by, str):
        return order_by

    assert isinstance(order_by, (tuple, list)), "order_by must be of type str, tuple or list"

    order_by = [_field_with_order(f) for f in order_by]
    order_by = ", ".join(order_by)

    return order_by


class DBHandler(Handler):
    def __init__(self, init_db=ATASKQ_DB_INIT_ON_HANDLER_INIT, logger=None) -> None:
        super().__init__(logger)

        if init_db:
            self.init_db()

    @property
    def db_path(self):
        raise Exception(f"'{self.__class__.__name__}' db doesn't support db path property'")

    def get_all(self, model_cls: IModel, where=None) -> List[dict]:
        rows, col_names, _ = self.select_query(model_cls, where=where)
        ret = [dict(zip(col_names, row)) for row in rows]

        return ret

    def get(self, model_cls: IModel, model_id) -> dict:
        rows, col_names, query_str = self.select_query(model_cls, where=f"{model_cls.id_key()} == {model_id}")
        assert len(rows) != 0, f"no match found for '{model_cls.__name__}', query: '{query_str}'."
        assert len(rows) == 1, f"more than 1 row found for '{model_cls.__name__}', query: '{query_str}'."
        ret = [dict(zip(col_names, row)) for row in rows][0]

        return ret

    @property
    def pragma_foreign_keys_on(self):
        return None

    @property
    @abstractmethod
    def format_symbol(self):
        pass

    @property
    @abstractmethod
    def connection(self):
        pass

    @property
    @abstractmethod
    def bytes_type(self):
        pass

    @property
    @abstractmethod
    def primary_key(self):
        pass

    @property
    @abstractmethod
    def timestamp_type(self):
        pass

    @abstractmethod
    def timestamp(self, ts):
        pass

    @property
    @abstractmethod
    def begin_exclusive(self):
        pass

    @property
    @abstractmethod
    def for_update(self):
        pass

    @abstractmethod
    def connect(self):
        pass

    @transaction_decorator()
    def _create(self, c, model_cls: Model, **ikwargs) -> Handler:
        d = {k: v for k, v in ikwargs.items() if model_cls.id_key() not in k}
        keys = list(d.keys())
        values = list(d.values())
        c.execute(
            f'INSERT INTO {model_cls.table_key()} ({", ".join(keys)}) VALUES ({", ".join([self.format_symbol] * len(keys))}) RETURNING {model_cls.id_key()}',
            values,
        )

        model_id = c.fetchone()[0]

        return model_id

    @transaction_decorator()
    def _create_bulk(self, c, model_cls: Model, ikwargs: List[dict]) -> Handler:
        # todo: consolidate all ikwargs with same keys to single insert command
        model_ids = []
        for v in ikwargs:
            d = {k: v for k, v in v.items() if model_cls.id_key() not in k}
            keys = list(d.keys())
            values = list(d.values())
            c.execute(
                f'INSERT INTO {model_cls.table_key()} ({", ".join(keys)}) VALUES ({", ".join([self.format_symbol] * len(keys))}) RETURNING {model_cls.id_key()}',
                values,
            )
            model_id = c.fetchone()[0]
            model_ids.append(model_id)

        return model_ids

    @transaction_decorator()
    def _update(self, c, model_cls: Model, model_id, **ikwargs):
        if len(ikwargs) == 0:
            return

        insert = ", ".join([f"{k} = {self.format_symbol}" for k in ikwargs.keys()])
        values = list(ikwargs.values())
        c.execute(f"UPDATE {model_cls.table_key()} SET {insert} WHERE {model_cls.id_key()} = {model_id};", values)

    @transaction_decorator()
    def delete(self, c, model_cls: Model, model_id: int):
        c.execute(f"DELETE FROM {model_cls.table_key()} WHERE {model_cls.id_key()} = {model_id}")

    @transaction_decorator(exclusive=True)
    def init_db(self, c):
        # Create schema version table if not exists
        c.execute("CREATE TABLE IF NOT EXISTS schema_version (" "version INTEGER PRIMARY KEY" ")")
        c.execute("SELECT * FROM schema_version")
        current_schema_version = c.fetchone()
        if current_schema_version is None:
            c.execute(f"INSERT INTO schema_version (version) VALUES ({__schema_version__})")
        else:
            current_schema_version = current_schema_version[0]
            assert (
                current_schema_version == __schema_version__
            ), f"Schema version mismatch, current schema version is {current_schema_version} while code schema version is {__schema_version__}"

        # Create jobs table if not exists
        c.execute(
            "CREATE TABLE IF NOT EXISTS jobs ("
            f"job_id {self.primary_key}, "
            "name TEXT, "
            "description TEXT, "
            "priority REAL DEFAULT 0"
            #   "summary_cookie_keys JSON"
            ")"
        )

        # Create state arguments table if not exists
        c.execute(
            "CREATE TABLE IF NOT EXISTS state_kwargs ("
            f"state_kwargs_id {self.primary_key}, "
            "name TEXT, "
            "entrypoint TEXT NOT NULL, "
            f"targs {self.bytes_type}, "
            "description TEXT, "
            "job_id INTEGER NOT NULL, "
            "CONSTRAINT uq_name_job_id UNIQUE(name, job_id), "
            "CONSTRAINT fk_job_id FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE"
            ")"
        )

        # Create tasks table if not exists
        statuses = ", ".join([f'"{a}"' for a in EStatus])
        c.execute(
            f"CREATE TABLE IF NOT EXISTS tasks ("
            f"task_id {self.primary_key}, "
            "name TEXT, "
            "level REAL, "
            "entrypoint TEXT NOT NULL, "
            f"targs {self.bytes_type}, "
            f"status TEXT ,"  # CHECK(status in ({statuses})),
            f"take_time {self.timestamp_type}, "
            f"start_time {self.timestamp_type}, "
            f"done_time {self.timestamp_type}, "
            f"pulse_time {self.timestamp_type}, "
            "description TEXT, "
            #   "summary_cookie JSON, "
            "job_id INTEGER NOT NULL, "
            "CONSTRAINT fk_job_id FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE"
            ")"
        )

    @transaction_decorator()
    def keep_max_jobs(self, c, max_jobs: int):
        c.execute(
            f"DELETE FROM jobs WHERE job_id NOT IN (SELECT job_id FROM jobs ORDER BY job_id DESC limit {max_jobs})"
        )

    @transaction_decorator()
    def _add_tasks(self, c, tasks: List[dict]):
        for t in tasks:
            d = {k: v for k, v in t.items() if Task.id_key() not in k}
            keys = list(d.keys())
            values = list(d.values())
            c.execute(
                f'INSERT INTO tasks ({", ".join(keys)}) VALUES ({", ".join([self.format_symbol] * len(keys))})', values
            )

        return self

    @transaction_decorator()
    def _add_state_kwargs(self, c, i_state_kwargs: List[dict]):
        for t in i_state_kwargs:
            d = {k: v for k, v in t.items() if StateKWArg.id_key() not in k}
            keys = list(d.keys())
            values = list(d.values())
            c.execute(
                f'INSERT INTO state_kwargs ({", ".join(keys)}) VALUES ({", ".join([self.format_symbol] * len(keys))})',
                values,
            )

        return self

    @transaction_decorator()
    def select_query(self, c, model_cls: Model, where: str = None, order_by=None):
        if where is None:
            where = ""

        query_str = f"SELECT * FROM {model_cls.table_key()}"

        if where:
            query_str += f" WHERE {where}"

        if order_by:
            order_str = order_query(order_by)
        else:
            default_order_by = f"{model_cls.table_key()}.{model_cls.id_key()} ASC"
            order_str = order_query(default_order_by)
        query_str += f" ORDER BY {order_str}"

        c.execute(query_str)
        rows = c.fetchall()
        col_names = [description[0] for description in c.description]

        return rows, col_names, query_str

    def tasks_status_query(self):
        query = (
            "SELECT level, name,"
            "COUNT(*) as total, "
            + ",".join([f"SUM(CASE WHEN status = '{status}' THEN 1 ELSE 0 END) AS {status} " for status in EStatus])
            + f"FROM tasks WHERE job_id = {self._job_id} "
            "GROUP BY level, name"
        )

        return query

    def task_query(self):
        query = f"SELECT * FROM tasks WHERE job_id = {self._job_id}"
        return query

    def state_kwargs_query(self):
        query = f"SELECT * FROM state_kwargs WHERE job_id = {self._job_id}"
        return query

    def jobs_query(self):
        query = f"SELECT * FROM jobs"
        return query

    def jobs_status_query(self):
        query = (
            "SELECT jobs.job_id, jobs.name, jobs.description, jobs.priority, "
            "COUNT(*) as tasks, "
            + ", ".join([f"SUM(CASE WHEN status = '{status}' THEN 1 ELSE 0 END) AS {status}" for status in EStatus])
            + f" FROM jobs "
            "LEFT JOIN tasks ON jobs.job_id = tasks.job_id GROUP BY jobs.job_id"
        )

        return query

    # @transaction_decorator()
    # def query(self, c, query_type=EQueryType.JOBS_STATUS, order_by=None):
    #     queries = {
    #         # query func, default sort by ASC
    #         EQueryType.TASKS: (self.task_query, "task_id ASC"),
    #         EQueryType.TASKS_STATUS: (self.tasks_status_query, "name ASC"),
    #         EQueryType.STATE_KWARGS: (self.state_kwargs_query, "state_kwargs_id ASC"),
    #         EQueryType.JOBS: (self.jobs_query, "job_id ASC"),
    #         EQueryType.JOBS_STATUS: (self.jobs_status_query, "jobs.job_id ASC"),
    #     }
    #     query, default_order_by = queries.get(query_type)

    #     if query is None:
    #         # should never get here
    #         raise RuntimeError(f"Unknown status type: {query_type}")

    #     query_str = query()
    #     if order_by:
    #         order_str = order_query(order_by)
    #         query_str += f" ORDER BY {order_str}"
    #     else:
    #         order_str = order_query(default_order_by)
    #         query_str += f" ORDER BY {order_str}"
    #     c.execute(query_str)
    #     rows = c.fetchall()
    #     col_names = [description[0] for description in c.description]
    #     # col_types = [description[1] for description in c.description]

    #     return rows, col_names

    @transaction_decorator()
    def count_pending_tasks_below_level(self, c, job_id, level) -> int:
        c.execute(
            f"SELECT COUNT(*) FROM tasks WHERE job_id = {job_id} AND level < {level} AND status in ('{EStatus.PENDING}')"
        )

        row = c.fetchone()
        return row[0]

    @transaction_decorator()
    def fail_pulse_timeout_tasks(self, c, timeout_sec):
        # set timeout tasks
        last_valid_pulse = datetime.now() - timedelta(seconds=timeout_sec)
        c.execute(
            f"UPDATE tasks SET status = '{EStatus.FAILURE}' WHERE pulse_time < {self.timestamp(last_valid_pulse)} AND status NOT IN ('{EStatus.SUCCESS}', '{EStatus.FAILURE}');"
        )

    @transaction_decorator(exclusive=True)
    def _take_next_task(self, c, job_id, level) -> Tuple[EAction, Task]:
        # todo: add FOR UPDATE in the queries postgresql
        level_query = f" AND level >= {level.start} AND level < {level.stop}" if level is not None else ""
        # get pending task with minimum level
        query = (
            f"SELECT * FROM tasks WHERE job_id = {job_id} AND status IN ('{EStatus.PENDING}'){level_query} AND level = "
            f"(SELECT MIN(level) FROM tasks WHERE job_id = {job_id} AND status IN ('{EStatus.PENDING}'){level_query})"
            f" {self.for_update}"
        )
        query = query.strip()
        c.execute(query)
        row = c.fetchone()
        if row is None:
            ptask = None
        else:
            col_names = [description[0] for description in c.description]
            ptask = self.from_interface(Task, dict(zip(col_names, row)))

        # get running task with minimum level
        query = (
            f"SELECT * FROM tasks WHERE job_id = {job_id} AND status IN ('{EStatus.RUNNING}'){level_query} AND level = "
            f"(SELECT MIN(level) FROM tasks WHERE job_id = {job_id} AND status IN ('{EStatus.RUNNING}'){level_query})"
            f" {self.for_update}"
        )
        query = query.strip()
        c.execute(query)
        row = c.fetchone()
        if row is None:
            rtask = None
        else:
            col_names = [description[0] for description in c.description]
            rtask = self.from_interface(Task, dict(zip(col_names, row)))

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
                    f"Running task with level higher than pending detected, taking pending. running id: {rtask.task_id}, pending id: {ptask.task_id}."
                )
                action = EAction.RUN_TASK
            else:
                action = EAction.RUN_TASK

        if action == EAction.RUN_TASK:
            now = datetime.now()
            c.execute(
                f"UPDATE tasks SET status = '{EStatus.RUNNING}', take_time = {self.timestamp(now)}, pulse_time = {self.timestamp(now)} WHERE task_id = {ptask.task_id};"
            )
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

        return action, task
