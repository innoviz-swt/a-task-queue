from datetime import datetime, timedelta
from typing import List, Union, Set, _GenericAlias
from abc import abstractmethod
from datetime import datetime

from .handler import Handler, get_query_kwargs
from ..model import Model, EState, Child, Parent
from .. import __schema_version__


def transaction_decorator(exclusive=False):
    def decorator(func):
        def wrapper(self, *args, **kwargs):
            with self.connect() as conn:
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


def expand_query_str(query_str, where=None, group_by=None, order_by=None, limit=None, offset=None):
    if where is not None:
        query_str += f" WHERE {where}"

    if group_by is not None:
        if not isinstance(group_by, (list, tuple)):
            group_by = [group_by]
        query_str += f" GROUP BY {', '.join(group_by)}"

    if order_by is not None:
        query_str += f" ORDER BY {order_query(order_by)}"

    if limit is not None:
        query_str += f" LIMIT {limit}"

    if offset is not None:
        query_str += f" OFFSET {offset}"

    return query_str


class DBHandler(Handler):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        if self.config["handler"]["db_init"]:
            self.init_db()

    def _add(self, c, model: Model, handled: Set[int]):
        # check if model already handled
        if id(model) in handled:
            return

        # handle parents
        for p_key in model.parents():
            if (parents := getattr(model, p_key)) is not None:
                # # parent create is required
                parent_mapping: Parent = getattr(model.__class__, p_key)
                p_id_key = parent_mapping.key
                parent_class = model.__annotations__[p_key]
                if isinstance(parent_class, _GenericAlias) and parent_class._name == "List":
                    parent_class = parent_class.__args__[0]
                else:
                    parents = [parents]

                for parent in parents:
                    parent: Model
                    if parent._state.value == EState.New:
                        self._add(c, parent, handled)
                        setattr(model, p_id_key, parent.id_val)
                    else:
                        assert getattr(model, p_id_key) == parent.id_val
                        self._add(c, parent, handled)

        if model._state.value == EState.New:
            self._create(c, model)
        elif model._state.value == EState.Modified and model._state.columns:
            self._update(c, model)

        # handle childs
        for c_key in model.childs():
            if (children := getattr(model, c_key)) is not None:
                # # parent create is required
                child_mapping: Child = getattr(model.__class__, c_key)
                c_id_key = child_mapping.key
                child_class = model.__annotations__[c_key]
                if isinstance(child_class, _GenericAlias) and child_class._name == "List":
                    child_class = child_class.__args__[0]
                else:
                    children = [children]

                for child in children:
                    child: Model
                    if child._state.value == EState.New:
                        setattr(child, c_id_key, model.id_val)
                    else:
                        assert getattr(child, c_id_key) == model.id_val
                    self._add(c, child, handled)

        handled.add(id(model))

    @transaction_decorator()
    def add(self, c, models: Union[Model, List[Model]]):
        handled = set()
        if not isinstance(models, list):
            models = [models]

        for model in models:
            self._add(c, model, handled)

    def count_all(self, model_cls: Model, **kwargs):
        query_kwargs = get_query_kwargs(kwargs)
        count = self.count_query(model_cls, **query_kwargs)

        return count

    def _get_all(self, model_cls: Model, **kwargs) -> List[dict]:
        query_kwargs = get_query_kwargs(kwargs)
        rows, col_names, _ = self.select_query(model_cls, **query_kwargs)
        ret = [dict(zip(col_names, row)) for row in rows]

        return ret

    def _get(self, model_cls: Model, model_id) -> dict:
        rows, col_names, query_str = self.select_query(model_cls, where=f"{model_cls.id_key()} = {model_id}")
        assert len(rows) != 0, f"no match found for '{model_cls.__name__}', query: '{query_str}'."
        assert len(rows) == 1, f"more than 1 row found for '{model_cls.__name__}', query: '{query_str}'."
        iret = [dict(zip(col_names, row)) for row in rows][0]

        return iret

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
    def update_all(self, c, model_cls: Model, where: str = None, **ikwargs):
        if len(ikwargs) == 0:
            return

        insert = ", ".join([f"{k} = {self.format_symbol}" for k in ikwargs.keys()])
        values = list(ikwargs.values())

        query_str = f"UPDATE {model_cls.table_key()} SET {insert}"

        if where:
            query_str += f" WHERE {where}"

        c.execute(query_str, values)

    @transaction_decorator()
    def delete_all(self, c, model_cls: Model, **kwargs):
        query_kwargs = get_query_kwargs(kwargs)
        query_str = f"DELETE FROM {model_cls.table_key()}"

        if "where" in query_kwargs:
            query_str += f" WHERE {query_kwargs['where']}"

        c.execute(query_str)

    @transaction_decorator()
    def _delete(self, c, model: Model):
        c.execute(f"DELETE FROM {model.table_key()} WHERE {model.id_key()} = {model.id_val}")

    @transaction_decorator(exclusive=True)
    def init_db(self, c):
        from ..models import Object

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

        # Create tasks table if not exists
        c.execute(
            f"CREATE TABLE IF NOT EXISTS tasks ("
            f"task_id {self.primary_key}, "
            "name TEXT, "
            "description TEXT, "
            "level REAL, "
            "entrypoint TEXT NOT NULL, "
            f"kwargs_id INTEGER, "  # object id
            f"args_id INTEGER, "  # object id
            f"ret_id INTEGER, "  # object id
            f"status TEXT ,"
            f"take_time {self.timestamp_type}, "
            f"start_time {self.timestamp_type}, "
            f"done_time {self.timestamp_type}, "
            f"pulse_time {self.timestamp_type}, "
            "job_id INTEGER NOT NULL, "
            "FOREIGN KEY (kwargs_id) REFERENCES objects(object_id), "
            "FOREIGN KEY (args_id) REFERENCES objects(object_id), "
            "FOREIGN KEY (ret_id) REFERENCES objects(object_id), "
            "CONSTRAINT fk_job_id FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE"
            ")"
        )

        # Create jobs tasks if not exists
        c.execute(
            f"CREATE TABLE IF NOT EXISTS objects ("
            f"{Object.id_key()} {self.primary_key}, "
            "serializer TEXT, "
            "desrializer TEXT, "
            f"blob {self.bytes_type}"
            ")"
        )

    @transaction_decorator()
    def count_query(self, c, model_cls: Model, where: str = None, limit: int = None, offset: int = 0):
        if limit is None:
            limit = self.config["api"]["limit"]
        query_str = f"SELECT COUNT(*) FROM {model_cls.table_key()}"
        query_str = expand_query_str(query_str, where=where, limit=limit, offset=offset)

        c.execute(query_str)

        row = c.fetchone()
        return row[0]

    @transaction_decorator()
    def select_query(
        self,
        c,
        model_cls: Model,
        where: str = None,
        order_by=None,
        limit: int = None,
        offset: int = 0,
    ):
        if limit is None:
            limit = self.config["api"]["limit"]
        query_str = f"SELECT * FROM {model_cls.table_key()}"
        if order_by is None:
            order_by = f"{model_cls.table_key()}.{model_cls.id_key()} ASC"
        query_str = expand_query_str(query_str, where=where, order_by=order_by, limit=limit, offset=offset)

        c.execute(query_str)
        rows = c.fetchall()
        col_names = [description[0] for description in c.description]

        return rows, col_names, query_str

    ##################
    # Custom Queries #
    ##################

    @transaction_decorator(exclusive=True)
    def take_next_task(self, c, job_id: int = None, level_start: int = None, level_stop: int = None):
        # imported here to avoid circular dependency
        from ..models import Task, EStatus
        from .handler import EAction

        # todo: add FOR UPDATE in the queries postgresql
        level_query = ""
        if level_start:
            level_query += f" AND level >= {level_start}"
        if level_stop:
            level_query += f" AND level < {level_stop}"

        # get pending task with minimum level
        job_query = ""
        if job_id is not None:
            job_query += f" AND job_id = {job_id}"

        query = (
            f"SELECT * FROM tasks WHERE status IN ('{EStatus.PENDING}'){job_query}{level_query} AND level = "
            f"(SELECT MIN(level) FROM tasks WHERE status IN ('{EStatus.PENDING}'){job_query}{level_query})"
            f" ORDER BY job_id ASC, task_id ASC {self.for_update}"
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
            f"SELECT * FROM tasks WHERE status IN ('{EStatus.RUNNING}'){job_query}{level_query} AND level = "
            f"(SELECT MIN(level) FROM tasks WHERE status IN ('{EStatus.RUNNING}'){job_query}{level_query})"
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
        **kwargs,
    ):
        from ..models import EStatus

        # todo add group by to get_query_kwargs

        if kwargs.get("limit") is None:
            kwargs["limit"] = self.config["api"]["limit"]
        if kwargs.get("order_by") is None:
            kwargs["order_by"] = "name ASC"
        if kwargs.get("group_by") is None:
            kwargs["group_by"] = ("level", "name")
        query_kwargs = get_query_kwargs(kwargs)

        query_str = (
            "SELECT level, name,"
            "COUNT(*) as total, "
            + ",".join([f"SUM(CASE WHEN status = '{status}' THEN 1 ELSE 0 END) AS {status} " for status in EStatus])
            + "FROM tasks"
        )
        query_str = expand_query_str(query_str, **query_kwargs)

        c.execute(query_str)
        rows = c.fetchall()
        col_names = [description[0] for description in c.description]

        ret = [dict(zip(col_names, row)) for row in rows]
        return ret

    @transaction_decorator()
    def jobs_status(self, c, order_by: str = None, limit: int = None, offset: int = 0):
        from ..models import EStatus

        if limit is None:
            limit = self.config["api"]["limit"]

        query_str = (
            "SELECT jobs.job_id, jobs.name, jobs.description, jobs.priority, "
            "COUNT(*) as tasks, "
            + ", ".join([f"SUM(CASE WHEN status = '{status}' THEN 1 ELSE 0 END) AS {status}" for status in EStatus])
            + f" FROM jobs "
            "LEFT JOIN tasks ON jobs.job_id = tasks.job_id "
            "GROUP BY jobs.job_id"
        )

        if order_by is None:
            order_by = "jobs.job_id DESC"

        query_str = expand_query_str(query_str, order_by=order_by, limit=limit, offset=offset)

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

    def _create(self, c, model: Model):
        d = self.to_interface()
        keys = list(d.keys())
        values = list(d.values())
        if keys:
            c.execute(
                f'INSERT INTO {model.table_key()} ({", ".join(keys)}) VALUES ({", ".join([self.format_symbol] * len(keys))}) RETURNING {model.id_key()}',
                values,
            )
        else:
            c.execute(f"INSERT INTO {model.table_key()} DEFAULT VALUES RETURNING {model.id_key()}"),

        model_id = c.lastrowid
        setattr(model, model.id_key(), model_id)

    def _update(self, c, model: Model):
        d = self.to_interface(model)
        insert = ", ".join([f"{k} = {self.format_symbol}" for k in d.keys()])
        values = list(d.values())
        c.execute(f"UPDATE {model.table_key()} SET {insert} WHERE {model.id_key()} = {model.id_val};", values)
