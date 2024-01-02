# todo: add queries order_by tests

from pathlib import Path
from copy import copy
from datetime import datetime

import pytest

from .handler import from_connection_str, EAction, EStatus
from .handler import Handler
from .db_handler import DBHandler, EQueryType
from .models import Task, StateKWArg
from .tasks_utils import dummy_args_task


@pytest.fixture
def handler(conn) -> Handler:
    return from_connection_str(conn)


@pytest.fixture
def jhandler(handler) -> Handler:
    return handler.create_job()


def test_db_format(conn, handler):
    assert isinstance(handler, Handler)
    if 'sqlite' in conn:
        from .db_handler.sqlite3 import SQLite3DBHandler
        assert isinstance(handler, SQLite3DBHandler)
        assert 'ataskq.db' in handler.db_path
    elif 'pg' in conn:
        from .db_handler.postgresql import PostgresqlDBHandler
        assert isinstance(handler, PostgresqlDBHandler)
    elif 'http' in conn:
        from .rest_handler import RESTHandler
        assert isinstance(handler, RESTHandler)
    else:
        raise Exception(f"unknown handler type in connection string '{conn}'")


def test_db_invalid_format_no_sep():
    with pytest.raises(RuntimeError) as excinfo:
        from_connection_str(conn=f'sqlite')
    assert 'connection must be of format <type>://<connection string>' == str(excinfo.value)


def test_db_invalid_format_no_type():
    with pytest.raises(RuntimeError) as excinfo:
        from_connection_str(conn=f'://ataskq.db')
    assert 'missing handler type, connection must be of format <type>://<connection string>' == str(excinfo.value)


def test_db_invalid_format_no_connectino():
    with pytest.raises(RuntimeError) as excinfo:
        from_connection_str(conn=f'sqlite://')
    assert 'missing connection string, connection must be of format <type>://<connection string>' == str(
        excinfo.value)


def test_job_default_name(handler: Handler):
    handler: Handler = handler.create_job()
    job = handler.get_jobs()[0]
    assert job.name is None


def test_job_custom_name(handler: Handler):
    handler: Handler = handler.create_job(name='my_job')
    job = handler.get_jobs()[0]
    assert job.name == 'my_job'


def _compare_task_taken(task1: Task, task2: Task, job_id=None):
    if job_id:
        assert task1.job_id == job_id
    assert task1.job_id == task2.job_id
    assert task1.name == task2.name
    assert task1.description == task2.description
    assert task1.level == task2.level
    assert task1.entrypoint == task2.entrypoint
    assert task1.targs == task2.targs


def test_update_task_start_time(jhandler):
    in_task1 = Task(entrypoint="ataskq.tasks_utils.dummy_args_task", level=1, name="task1")

    jhandler.add_tasks([
        in_task1,
    ])

    tasks = jhandler.get_tasks()
    assert len(tasks) == 1  # sanity
    task: Task = tasks[0]

    # update db with task time (also updates task inplace)
    start_time = datetime.now()
    start_time = start_time.replace(microsecond=0)  # test to seconds resolution
    jhandler.update_task_start_time(task, start_time)
    assert task.start_time == start_time

    # get task form db and validate
    tasks = jhandler.get_tasks()
    assert len(tasks) == 1  # sanity
    task: Task = tasks[0]
    assert task.start_time == start_time


def test_update_task_status(jhandler):
    in_task1 = Task(entrypoint="ataskq.tasks_utils.dummy_args_task", level=1, name="task1")

    jhandler.add_tasks([
        in_task1,
    ])

    tasks = jhandler.get_tasks()
    assert len(tasks) == 1  # sanity
    task: Task = tasks[0]
    assert task.status == EStatus.PENDING

    now = datetime.now()
    # update db with task status (also updates task inplace)
    jhandler.update_task_status(task, EStatus.RUNNING, timestamp=now)
    assert task.status == EStatus.RUNNING
    assert task.pulse_time == now
    assert task.done_time is None
    # get task form db and validate
    tasks = jhandler.get_tasks()
    assert len(tasks) == 1  # sanity
    task: Task = tasks[0]
    assert task.status == EStatus.RUNNING
    assert task.pulse_time == now
    assert task.done_time is None

    # update db with task status (also updates task inplace)
    jhandler.update_task_status(task, EStatus.SUCCESS, timestamp=now)
    assert task.status == EStatus.SUCCESS
    assert task.pulse_time == now
    assert task.done_time == now
    # get task form db and validate
    tasks = jhandler.get_tasks()
    assert len(tasks) == 1  # sanity
    task: Task = tasks[0]
    assert task.status == EStatus.SUCCESS
    assert task.pulse_time == now
    assert task.done_time == now


def test_take_next_task_sanity(jhandler):
    in_task1 = Task(entrypoint="ataskq.tasks_utils.dummy_args_task", level=1, name="task1")

    jhandler.add_tasks([
        in_task1,
    ])

    action, task = jhandler._take_next_task(level=None)
    assert action == EAction.RUN_TASK
    _compare_task_taken(in_task1, task)


def test_take_next(jhandler):
    in_task1 = Task(entrypoint="ataskq.tasks_utils.dummy_args_task", level=1, name="task1")
    in_task2 = Task(entrypoint="ataskq.tasks_utils.dummy_args_task", level=2, name="task2")
    in_task3 = copy(in_task1)

    jhandler.add_tasks([
        in_task2,
        in_task1,
        in_task3,
    ])

    tids = []

    action, task = jhandler._take_next_task(level=None)
    assert action == EAction.RUN_TASK
    assert task.task_id not in tids
    tids.append(task.task_id)
    _compare_task_taken(in_task1, task)

    action, task = jhandler._take_next_task(level=None)
    assert action == EAction.RUN_TASK
    assert task.task_id not in tids
    tids.append(task.task_id)
    _compare_task_taken(in_task3, task)  # note in_task3 is copy of in_task1

    action, task = jhandler._take_next_task(level=None)
    assert action == EAction.WAIT
    assert task is None


def test_take_next_task_2_jobs(conn):
    handler1: Handler = from_connection_str(conn=conn).create_job(name='job1')
    handler2: Handler = from_connection_str(conn=conn).create_job(name='job2')

    in_task1 = Task(entrypoint="ataskq.tasks_utils.dummy_args_task", level=2, name="taska")
    in_task2 = Task(entrypoint="ataskq.tasks_utils.dummy_args_task", level=1, name="taskb")
    in_task3 = Task(entrypoint="ataskq.tasks_utils.dummy_args_task", level=1, name="taskb")
    in_task4 = Task(entrypoint="ataskq.tasks_utils.dummy_args_task", level=1, name="taskd")
    in_task5 = Task(entrypoint="ataskq.tasks_utils.dummy_args_task", level=2, name="taske")

    handler1.add_tasks([
        in_task2,
        in_task1,
        in_task3,
    ])

    handler2.add_tasks([
        in_task5,
        in_task4,
    ])

    jid1 = handler1.job_id
    jid2 = handler2.job_id
    tids = []

    # sanity check
    assert len(handler1.get_jobs()) == 2
    assert len(handler1.get_tasks()) == 3
    assert len(handler2.get_jobs()) == 2
    assert len(handler2.get_tasks()) == 2

    # db handler 1
    action, task = handler1._take_next_task(level=None)
    assert action == EAction.RUN_TASK
    assert task.task_id not in tids
    tids.append(task.task_id)
    _compare_task_taken(in_task2, task, job_id=jid1)
    tids.append(task.task_id)

    action, task = handler1._take_next_task(level=None)
    assert action == EAction.RUN_TASK
    assert task.task_id not in tids
    tids.append(task.task_id)
    _compare_task_taken(in_task3, task, job_id=jid1)  # note in_task3 is copy of in_task1

    action, task = handler1._take_next_task(level=None)
    assert action == EAction.WAIT
    assert task is None

    # db handler 2
    action, task = handler2._take_next_task(level=None)
    assert action == EAction.RUN_TASK
    assert task.task_id not in tids
    tids.append(task.task_id)
    _compare_task_taken(in_task4, task, job_id=jid2)
    tids.append(task.task_id)

    action, task = handler2._take_next_task(level=None)
    assert action == EAction.WAIT
    assert task is None


def test_get_tasks(jhandler):
    in_task1 = Task(entrypoint=dummy_args_task, level=1, name="task1")
    in_task2 = Task(entrypoint=dummy_args_task, level=2, name="task2")
    in_task3 = Task(entrypoint=dummy_args_task, level=3, name="task3")

    jhandler.add_tasks([
        in_task3,
        in_task2,
        in_task1,
    ])

    tasks = jhandler.get_tasks()
    assert len(tasks) == 3
    for t in tasks:
        if t.level == 1:
            _compare_task_taken(in_task1, t)
        elif t.level == 2:
            _compare_task_taken(in_task2, t)
        elif t.level == 3:
            _compare_task_taken(in_task3, t)


def test_query(conn):
    if not isinstance(jhandler, DBHandler):
        pytest.skip()

    db_handler: Handler = from_connection_str(conn=conn).create_job()
    for q in EQueryType.__members__.values():
        try:
            db_handler.query(q)
        except Exception as ex:
            pytest.fail(f"query '{q}' failed, exception: {ex}")


def test_table(jhandler):
    if not isinstance(jhandler, DBHandler):
        pytest.skip()

    # very general sanity test
    try:
        for q in EQueryType.__members__.values():
            table = jhandler.html_table(q).split('\n')
            assert '<table>' in table[0]
            assert '</table>' in table[-1]
    except Exception as ex:
        pytest.fail(f"table query '{q}' failed, exception: {ex}")


def test_html(jhandler):
    if not isinstance(jhandler, DBHandler):
        pytest.skip()

    # very general sanity test
    try:
        for q in EQueryType.__members__.values():
            html = jhandler.html(query_type=EQueryType.TASKS_STATUS)
            assert '<body>' in html
            assert '</body>' in html

            html = html.split('\n')
            assert '<html>' in html[0]
            assert '</html>' in html[-2]
            assert '' == html[-1]
    except Exception as ex:
        pytest.fail(f"html query '{q}' failed, exception: {ex}")


def test_html_file_str_dump(jhandler, tmp_path: Path):
    if not isinstance(jhandler, DBHandler):
        pytest.skip()

    # very general sanity test
    file = tmp_path / 'test.html'
    html = jhandler.html(query_type=EQueryType.TASKS_STATUS, file=file)

    assert file.exists()
    assert html == file.read_text()


def test_html_file_io_dump(jhandler, tmp_path: Path):
    if not isinstance(jhandler, DBHandler):
        pytest.skip()

    # very general sanity test
    file = tmp_path / 'test.html'
    with open(file, 'w') as f:
        html = jhandler.html(query_type=EQueryType.TASKS_STATUS, file=f)

    assert file.exists()
    assert html == file.read_text()


def test_task_job_delete_cascade(conn):
    # test that deleting a job deletes all its tasks
    handler1: Handler = from_connection_str(conn=conn).create_job(name='job1')
    handler1.add_tasks([
        Task(),
        Task(),
        Task(),
    ])
    tasks = handler1.get_tasks()
    assert len(tasks) == 3

    handler2: Handler = from_connection_str(conn=conn).create_job(name='job2')
    handler2.add_tasks([
        Task(),
        Task(),
    ])
    tasks = handler2.get_tasks()
    assert len(tasks) == 2

    handler1.delete_job()

    # add get_tasks from global handler (not job handler)
    # tasks = db_handler.get_tasks()
    # assert len(tasks) == 2

    tasks = handler2.get_tasks()
    assert len(tasks) == 2


def test_state_kwargs_job_delete_cascade(conn):
    # test that deleting a job deletes all its tasks
    handler1: Handler = from_connection_str(conn=conn).create_job(name='job1')
    handler1.add_state_kwargs([
        StateKWArg(entrypoint=''),
        StateKWArg(entrypoint=''),
        StateKWArg(entrypoint=''),
    ])
    state_kwargs = handler1.get_state_kwargs()
    assert len(state_kwargs) == 3

    handler2: Handler = from_connection_str(conn=conn).create_job(name='job2')
    handler2.add_state_kwargs([
        StateKWArg(entrypoint=''),
        StateKWArg(entrypoint=''),
    ])
    state_kwargs = handler2.get_state_kwargs()
    assert len(state_kwargs) == 2

    handler1.delete_job()

    # todo: add get_state_kwargs from global handler, not job handler
    # state_kwargs = handler1.get_state_kwargs()
    # assert len(state_kwargs) == 2

    state_kwargs = handler2.get_state_kwargs()
    assert len(state_kwargs) == 2


def test_max_jobs(handler, conn):
    if not isinstance(handler, DBHandler):
        pytest.skip()

    max_jobs = 10
    jobs_id = []
    for i in range(max_jobs * 2):
        db_handler = from_connection_str(conn=conn, max_jobs=max_jobs)
        jobs_id.append(db_handler.create_job(name=f'job{i}').job_id)
    jobs = from_connection_str(conn=conn).get_jobs()
    assert len(jobs) == 10

    remaining_jobs = [j.job_id for j in jobs]
    assert remaining_jobs == jobs_id[-max_jobs:]
