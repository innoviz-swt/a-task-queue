from datetime import datetime, timedelta
from typing import List, Tuple
from pathlib import Path
from io import TextIOWrapper
from abc import abstractmethod
from datetime import datetime

from .handler import Handler, get_query_kwargs
from ..imodel import IModel
from ..env import ATASKQ_DB_INIT_ON_HANDLER_INIT, ATASKQ_DB_LIMIT_DEFAULT
from .. import __schema_version__

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


def expand_query_str(query_str, _where=None, _order_by=None, _limit=None, _offset=None):
    if _where is not None:
        query_str += f" WHERE {_where}"

    if _order_by is not None:
        query_str += f" ORDER BY {order_query(_order_by)}"

    if _limit is not None:
        query_str += f" LIMIT {_limit}"

    if _offset is not None:
        query_str += f" OFFSET {_offset}"

    return query_str


class DBHandler(Handler):
    def __init__(self, init_db=ATASKQ_DB_INIT_ON_HANDLER_INIT, logger=None) -> None:
        super().__init__(logger)

        if init_db:
            self.init_db()

    @property
    def db_path(self):
        raise Exception(f"'{self.__class__.__name__}' db doesn't support db path property'")

    def count_all(self, model_cls: IModel, **kwargs):
        query_kwargs = get_query_kwargs(kwargs)
        count = self.count_query(model_cls, **query_kwargs)

        return count

    def get_all(self, model_cls: IModel, **kwargs) -> List[dict]:
        query_kwargs = get_query_kwargs(kwargs)
        rows, col_names, _ = self.select_query(model_cls, **query_kwargs)
        ret = [dict(zip(col_names, row)) for row in rows]

        return ret

    def get(self, model_cls: IModel, model_id) -> dict:
        rows, col_names, query_str = self.select_query(model_cls, _where=f"{model_cls.id_key()} = {model_id}")
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
    def _create(self, c, model_cls: IModel, **ikwargs) -> int:
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
    def _create_bulk(self, c, model_cls: IModel, ikwargs: List[dict]) -> List[int]:
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
    def _update(self, c, model_cls: IModel, model_id, **ikwargs):
        if len(ikwargs) == 0:
            return

        insert = ", ".join([f"{k} = {self.format_symbol}" for k in ikwargs.keys()])
        values = list(ikwargs.values())
        c.execute(f"UPDATE {model_cls.table_key()} SET {insert} WHERE {model_cls.id_key()} = {model_id};", values)

    @transaction_decorator()
    def update_all(self, c, model_cls: IModel, where: str = None, **ikwargs):
        if len(ikwargs) == 0:
            return

        insert = ", ".join([f"{k} = {self.format_symbol}" for k in ikwargs.keys()])
        values = list(ikwargs.values())

        query_str = f"UPDATE {model_cls.table_key()} SET {insert}"

        if where:
            query_str += f" WHERE {where}"

        c.execute(query_str, values)

    @abstractmethod
    def delete(self, model_cls: IModel, model_id: int):
        pass

    @transaction_decorator()
    def delete_all(self, c, model_cls: IModel, **kwargs):
        query_kwargs = get_query_kwargs(kwargs)
        query_str = f"DELETE FROM {model_cls.table_key()}"

        if "_where" in query_kwargs:
            query_str += f" WHERE {query_kwargs['_where']}"

        c.execute(query_str)

    @transaction_decorator()
    def delete(self, c, model_cls: IModel, model_id: int):
        c.execute(f"DELETE FROM {model_cls.table_key()} WHERE {model_cls.id_key()} = {model_id}")

    @transaction_decorator(exclusive=True)
    def init_db(self, c):
        from ..models import EStatus

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
    def count_query(
        self, c, model_cls: IModel, _where: str = None, _limit: int = ATASKQ_DB_LIMIT_DEFAULT, _offset: int = 0
    ):
        query_str = f"SELECT COUNT(*) FROM {model_cls.table_key()}"
        query_str = expand_query_str(query_str, _where=_where, _limit=_limit, _offset=_offset)

        c.execute(query_str)

        row = c.fetchone()
        return row[0]

    @transaction_decorator()
    def select_query(
        self,
        c,
        model_cls: IModel,
        _where: str = None,
        _order_by=None,
        _limit: int = ATASKQ_DB_LIMIT_DEFAULT,
        _offset: int = 0,
    ):
        query_str = f"SELECT * FROM {model_cls.table_key()}"
        if _order_by is None:
            _order_by = f"{model_cls.table_key()}.{model_cls.id_key()} ASC"
        query_str = expand_query_str(query_str, _where=_where, _order_by=_order_by, _limit=_limit, _offset=_offset)

        c.execute(query_str)
        rows = c.fetchall()
        col_names = [description[0] for description in c.description]

        return rows, col_names, query_str

    ##################
    # Custom Queries #
    ##################

    @transaction_decorator(exclusive=True)
    def take_next_task(self, c, job_id, level_start, level_stop):
        # imported here to avoid circular dependency
        from ..models import Task, EStatus
        from .handler import Handler, EAction

        # todo: add FOR UPDATE in the queries postgresql
        level_query = ""
        if level_start:
            level_query += f" AND level >= {level_start}"
        if level_stop:
            level_query += f" AND level < {level_stop}"

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

    @transaction_decorator()
    def tasks_status(
        self,
        c,
        job_id,
        _where: str = None,
        _order_by: str = None,
        _limit: int = ATASKQ_DB_LIMIT_DEFAULT,
        _offset: int = 0,
    ):
        from ..models import EStatus

        query_str = (
            "SELECT level, name,"
            "COUNT(*) as total, "
            + ",".join([f"SUM(CASE WHEN status = '{status}' THEN 1 ELSE 0 END) AS {status} " for status in EStatus])
            + f"FROM tasks WHERE job_id = {job_id} "
            "GROUP BY level, name"
        )

        if _order_by is None:
            _order_by = "name ASC"

        query_str = expand_query_str(query_str, _order_by=_order_by, _limit=_limit, _offset=_offset)

        c.execute(query_str)
        rows = c.fetchall()
        col_names = [description[0] for description in c.description]

        ret = [dict(zip(col_names, row)) for row in rows]
        return ret

    @transaction_decorator()
    def jobs_status(
        self, c, _where: str = None, _order_by: str = None, _limit: int = ATASKQ_DB_LIMIT_DEFAULT, _offset: int = 0
    ):
        from ..models import EStatus

        query_str = (
            "SELECT jobs.job_id, jobs.name, jobs.description, jobs.priority, "
            "COUNT(*) as tasks, "
            + ", ".join([f"SUM(CASE WHEN status = '{status}' THEN 1 ELSE 0 END) AS {status}" for status in EStatus])
            + f" FROM jobs "
            "LEFT JOIN tasks ON jobs.job_id = tasks.job_id "
            "GROUP BY jobs.job_id"
        )

        if _order_by is None:
            _order_by = "jobs.job_id DESC"

        query_str = expand_query_str(query_str, _order_by=_order_by, _limit=_limit, _offset=_offset)

        c.execute(query_str)
        rows = c.fetchall()
        col_names = [description[0] for description in c.description]

        ret = [dict(zip(col_names, row)) for row in rows]
        return ret

    @transaction_decorator()
    def fail_pulse_timeout_tasks(self, c, timeout_sec=None):
        from ..models import EStatus

        if timeout_sec is None:
            return

        # set timeout tasks
        last_valid_pulse = datetime.now() - timedelta(seconds=timeout_sec)
        c.execute(
            f"UPDATE tasks SET status = '{EStatus.FAILURE}' WHERE pulse_time < {self.timestamp(last_valid_pulse)} AND status NOT IN ('{EStatus.SUCCESS}', '{EStatus.FAILURE}');"
        )
