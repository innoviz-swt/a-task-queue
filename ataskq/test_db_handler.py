from pathlib import Path

import pytest

from .db_handler import DBHandler, EQueryType, Task, EAction


def test_db_format():
    # very general sanity test
    db_handler = DBHandler(db=f'sqlite://ataskq.db')

    assert db_handler.db_type == 'sqlite'
    assert db_handler.db_conn == 'ataskq.db'
    assert db_handler.db_path == 'ataskq.db'


def test_db_invalid_format_no_sep():
    # very general sanity test
    with pytest.raises(RuntimeError) as excinfo:
        DBHandler(db=f'sqlite').create_job()
    assert 'db must be of format <type>://<connection string>' == str(excinfo.value)


def test_db_invalid_format_no_type():
    # very general sanity test
    with pytest.raises(RuntimeError) as excinfo:
        DBHandler(db=f'://ataskq.db').create_job()
    assert 'missing db type, db must be of format <type>://<connection string>' == str(excinfo.value)


def test_db_invalid_format_no_connectino():
    # very general sanity test
    with pytest.raises(RuntimeError) as excinfo:
        DBHandler(db=f'sqlite://').create_job()
    assert 'missing db connection string, db must be of format <type>://<connection string>' == str(excinfo.value)


def _compare_task_taken(task1: Task, task2: Task):
    assert task1.name == task2.name
    assert task1.description == task2.description
    assert task1.level == task2.level
    assert task1.entrypoint == task2.entrypoint
    assert task1.targs == task2.targs


def test_take_next_task(tmp_path: Path):
    db_handler: DBHandler = DBHandler(db=f'sqlite://{tmp_path}/ataskq.db').create_job()
    in_task1 = Task(entrypoint="ataskq.tasks_utils.dummy_args_task", level=1, name="task1")
    in_task2 = Task(entrypoint="ataskq.tasks_utils.dummy_args_task", level=2, name="task2")

    db_handler.add_tasks([
        in_task1,
    ])

    action, task = db_handler._take_next_task(level=None)
    assert action == EAction.RUN_TASK
    _compare_task_taken(in_task1, task)


def test_table(tmp_path):
    # very general sanity test
    db_handler: DBHandler = DBHandler(db=f'sqlite://{tmp_path}/ataskq.db').create_job()
    table = db_handler.html_table().split('\n')
    assert '<table>' in table[0]
    assert '</table>' in table[-1]


def test_html(tmp_path: Path):
    # very general sanity test
    db_handler: DBHandler = DBHandler(db=f'sqlite://{tmp_path}/ataskq.db').create_job()
    html = db_handler.html(query_type=EQueryType.TASKS_STATUS)
    assert '<body>' in html
    assert '</body>' in html

    html = html.split('\n')
    assert '<html>' in html[0]
    assert '</html>' in html[-1]


def test_html_file_str_dump(tmp_path: Path):
    # very general sanity test
    db_handler: DBHandler = DBHandler(db=f'sqlite://{tmp_path}/ataskq.db').create_job()
    file = tmp_path / 'test.html'
    html = db_handler.html(query_type=EQueryType.TASKS_STATUS, file=file)

    assert file.exists()
    assert html == file.read_text()


def test_html_file_io_dump(tmp_path: Path):
    # very general sanity test
    db_handler: DBHandler = DBHandler(db=f'sqlite://{tmp_path}/ataskq.db').create_job()
    file = tmp_path / 'test.html'
    with open(file, 'w') as f:
        html = db_handler.html(query_type=EQueryType.TASKS_STATUS, file=f)

    assert file.exists()
    assert html == file.read_text()
