# todo: add queries order_by tests

from pathlib import Path
from copy import copy
from .tasks_utils import dummy_args_task

import pytest

from .db_handler import from_connection_str, DBHandler, EQueryType, EAction
from .models import Task


def test_db_format(conn):
    # very general sanity test
    db_handler = from_connection_str(conn)

    assert isinstance(db_handler, DBHandler)
    if 'sqlite' in conn:
        from .db_handler.sqlite3 import SQLite3DBHandler
        assert isinstance(db_handler, SQLite3DBHandler)
        assert 'ataskq.db' in db_handler.db_path
    elif 'postgresql' in conn:
        from .db_handler.sqlite3 import SQLite3DBHandler
        assert isinstance(db_handler, SQLite3DBHandler)
    else:
        raise Exception(f"unknown db type in connection string '{conn}'")


def test_db_invalid_format_no_sep():
    with pytest.raises(RuntimeError) as excinfo:
        from_connection_str(conn=f'sqlite')
    assert 'connection must be of format <db type>://<connection string>' == str(excinfo.value)


def test_db_invalid_format_no_type():
    with pytest.raises(RuntimeError) as excinfo:
        from_connection_str(conn=f'://ataskq.db')
    assert 'missing db type, connection must be of format <db type>://<connection string>' == str(excinfo.value)


def test_db_invalid_format_no_connectino():
    with pytest.raises(RuntimeError) as excinfo:
        from_connection_str(conn=f'sqlite://')
    assert 'missing connection string, connection must be of format <db type>://<connection string>' == str(
        excinfo.value)


def test_job_default_name(conn):
    db_handler: DBHandler = from_connection_str(conn=conn).create_job()
    job = db_handler.get_jobs()[0]
    assert job.name is None


def test_job_custom_name(conn):
    db_handler: DBHandler = from_connection_str(conn=conn).create_job(name='my_job')
    job = db_handler.get_jobs()[0]
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


def test_take_next_task_sanity(conn):
    db_handler: DBHandler = from_connection_str(conn=conn).create_job()
    in_task1 = Task(entrypoint="ataskq.tasks_utils.dummy_args_task", level=1, name="task1")

    db_handler.add_tasks([
        in_task1,
    ])

    action, task = db_handler._take_next_task(level=None)
    assert action == EAction.RUN_TASK
    _compare_task_taken(in_task1, task)


def test_take_next(conn):
    db_handler: DBHandler = from_connection_str(conn=conn).create_job()
    in_task1 = Task(entrypoint="ataskq.tasks_utils.dummy_args_task", level=1, name="task1")
    in_task2 = Task(entrypoint="ataskq.tasks_utils.dummy_args_task", level=2, name="task2")
    in_task3 = copy(in_task1)

    db_handler.add_tasks([
        in_task2,
        in_task1,
        in_task3,
    ])

    tids = []

    action, task = db_handler._take_next_task(level=None)
    assert action == EAction.RUN_TASK
    assert task.task_id not in tids
    tids.append(task.task_id)
    _compare_task_taken(in_task1, task)

    action, task = db_handler._take_next_task(level=None)
    assert action == EAction.RUN_TASK
    assert task.task_id not in tids
    tids.append(task.task_id)
    _compare_task_taken(in_task3, task)  # note in_task3 is copy of in_task1

    action, task = db_handler._take_next_task(level=None)
    assert action == EAction.WAIT
    assert task is None


def test_take_next_task_2_jobs(conn):
    db_handler1: DBHandler = from_connection_str(conn=conn).create_job(name='job1')
    db_handler2: DBHandler = from_connection_str(conn=conn).create_job(name='job2')

    in_task1 = Task(entrypoint="ataskq.tasks_utils.dummy_args_task", level=2, name="taska")
    in_task2 = Task(entrypoint="ataskq.tasks_utils.dummy_args_task", level=1, name="taskb")
    in_task3 = Task(entrypoint="ataskq.tasks_utils.dummy_args_task", level=1, name="taskb")
    in_task4 = Task(entrypoint="ataskq.tasks_utils.dummy_args_task", level=1, name="taskd")
    in_task5 = Task(entrypoint="ataskq.tasks_utils.dummy_args_task", level=2, name="taske")

    db_handler1.add_tasks([
        in_task2,
        in_task1,
        in_task3,
    ])

    db_handler2.add_tasks([
        in_task5,
        in_task4,
    ])

    jid1 = db_handler1.job_id
    jid2 = db_handler2.job_id
    tids = []

    # sanity check
    assert len(db_handler1.get_jobs()) == 2
    assert len(db_handler1.get_tasks()) == 3
    assert len(db_handler2.get_jobs()) == 2
    assert len(db_handler2.get_tasks()) == 2

    # db handler 1
    action, task = db_handler1._take_next_task(level=None)
    assert action == EAction.RUN_TASK
    assert task.task_id not in tids
    tids.append(task.task_id)
    _compare_task_taken(in_task2, task, job_id=jid1)
    tids.append(task.task_id)

    action, task = db_handler1._take_next_task(level=None)
    assert action == EAction.RUN_TASK
    assert task.task_id not in tids
    tids.append(task.task_id)
    _compare_task_taken(in_task3, task, job_id=jid1)  # note in_task3 is copy of in_task1

    action, task = db_handler1._take_next_task(level=None)
    assert action == EAction.WAIT
    assert task is None

    # db handler 2
    action, task = db_handler2._take_next_task(level=None)
    assert action == EAction.RUN_TASK
    assert task.task_id not in tids
    tids.append(task.task_id)
    _compare_task_taken(in_task4, task, job_id=jid2)
    tids.append(task.task_id)

    action, task = db_handler2._take_next_task(level=None)
    assert action == EAction.WAIT
    assert task is None


def test_get_tasks(conn):
    db_handler: DBHandler = from_connection_str(conn=conn).create_job()
    in_task1 = Task(entrypoint=dummy_args_task, level=1, name="task1")
    in_task2 = Task(entrypoint=dummy_args_task, level=2, name="task2")
    in_task3 = Task(entrypoint=dummy_args_task, level=3, name="task3")

    db_handler.add_tasks([
        in_task3,
        in_task2,
        in_task1,
    ])

    tasks = db_handler.get_tasks()
    assert len(tasks) == 3
    for t in tasks:
        if t.level == 1:
            _compare_task_taken(in_task1, t)
        elif t.level == 2:
            _compare_task_taken(in_task2, t)
        elif t.level == 3:
            _compare_task_taken(in_task3, t)


def test_query(conn):
    db_handler: DBHandler = from_connection_str(conn=conn).create_job()
    for q in EQueryType.__members__.values():
        try:
            db_handler.query(q)
        except Exception as ex:
            pytest.fail(f"query '{q}' failed, exception: {ex}")


def test_table(conn):
    # very general sanity test
    db_handler: DBHandler = from_connection_str(conn=conn).create_job()
    try:
        for q in EQueryType.__members__.values():
            table = db_handler.html_table(q).split('\n')
            assert '<table>' in table[0]
            assert '</table>' in table[-1]
    except Exception as ex:
        pytest.fail(f"table query '{q}' failed, exception: {ex}")


def test_html(conn):
    # very general sanity test
    db_handler: DBHandler = from_connection_str(conn=conn).create_job()
    try:
        for q in EQueryType.__members__.values():
            html = db_handler.html(query_type=EQueryType.TASKS_STATUS)
            assert '<body>' in html
            assert '</body>' in html

            html = html.split('\n')
            assert '<html>' in html[0]
            assert '</html>' in html[-2]
            assert '' == html[-1]
    except Exception as ex:
        pytest.fail(f"html query '{q}' failed, exception: {ex}")


def test_html_file_str_dump(conn, tmp_path: Path):
    # very general sanity test
    db_handler: DBHandler = from_connection_str(conn=conn).create_job()
    file = tmp_path / 'test.html'
    html = db_handler.html(query_type=EQueryType.TASKS_STATUS, file=file)

    assert file.exists()
    assert html == file.read_text()


def test_html_file_io_dump(conn, tmp_path: Path):
    # very general sanity test
    db_handler: DBHandler = from_connection_str(conn=conn).create_job()
    file = tmp_path / 'test.html'
    with open(file, 'w') as f:
        html = db_handler.html(query_type=EQueryType.TASKS_STATUS, file=f)

    assert file.exists()
    assert html == file.read_text()


def test_task_job_delete_cascade(conn):
    # test that deleting a job deletes all its tasks
    db_handler1: DBHandler = from_connection_str(conn=conn).create_job(name='job1')
    db_handler1.add_tasks([
        Task(),
        Task(),
        Task(),
    ])
    tasks = db_handler1.get_tasks()
    assert len(tasks) == 3

    db_handler2: DBHandler = from_connection_str(conn=conn).create_job(name='job2')
    db_handler2.add_tasks([
        Task(),
        Task(),
    ])
    tasks = db_handler2.get_tasks()
    assert len(tasks) == 2

    db_handler1.delete_job()

    tasks = db_handler1.get_tasks()
    assert len(tasks) == 0

    tasks = db_handler2.get_tasks()
    assert len(tasks) == 2


def test_max_jobs(conn):
    max_jobs = 10
    jobs_id = []
    for i in range(max_jobs * 2):
        db_handler = from_connection_str(conn=conn, max_jobs=max_jobs)
        jobs_id.append(db_handler.create_job(name=f'job{i}').job_id)
    jobs = from_connection_str(conn=conn).get_jobs()
    assert len(jobs) == 10

    remaining_jobs = [j.job_id for j in jobs]
    assert remaining_jobs == jobs_id[-max_jobs:]
