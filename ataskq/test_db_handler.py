from pathlib import Path

import pytest

from .db_handler import DBHandler, EQueryType, Task, EAction
from .test_common import db_path


def test_db_format():
    # very general sanity test
    db_handler = DBHandler(db=f'sqlite://ataskq.db')

    assert db_handler.db_type == 'sqlite'
    assert db_handler.db_conn == 'ataskq.db'
    assert db_handler.db_path == 'ataskq.db'


def test_db_invalid_format_no_sep():
    with pytest.raises(RuntimeError) as excinfo:
        DBHandler(db=f'sqlite').create_job()
    assert 'db must be of format <type>://<connection string>' == str(excinfo.value)


def test_db_invalid_format_no_type():
    with pytest.raises(RuntimeError) as excinfo:
        DBHandler(db=f'://ataskq.db').create_job()
    assert 'missing db type, db must be of format <type>://<connection string>' == str(excinfo.value)


def test_db_invalid_format_no_connectino():
    with pytest.raises(RuntimeError) as excinfo:
        DBHandler(db=f'sqlite://').create_job()
    assert 'missing db connection string, db must be of format <type>://<connection string>' == str(excinfo.value)


def test_job_default_name(tmp_path):
    db_handler: DBHandler = DBHandler(db=db_path(tmp_path)).create_job()
    job = db_handler.get_jobs()[0]
    assert job.name == ''


def test_job_custom_name(tmp_path):
    db_handler: DBHandler = DBHandler(db=db_path(tmp_path)).create_job(name='my_job')
    job = db_handler.get_jobs()[0]
    assert job.name == 'my_job'


def _compare_task_taken(task1: Task, task2: Task):
    assert task1.name == task2.name
    assert task1.description == task2.description
    assert task1.level == task2.level
    assert task1.entrypoint == task2.entrypoint
    assert task1.targs == task2.targs


def test_take_next_task(tmp_path: Path):
    db_handler: DBHandler = DBHandler(db=db_path(tmp_path)).create_job()
    in_task1 = Task(entrypoint="ataskq.tasks_utils.dummy_args_task", level=1, name="task1")

    db_handler.add_tasks([
        in_task1,
    ])

    action, task = db_handler._take_next_task(level=None)
    assert action == EAction.RUN_TASK
    _compare_task_taken(in_task1, task)


def test_table(tmp_path):
    # very general sanity test
    db_handler: DBHandler = DBHandler(db=db_path(tmp_path)).create_job()
    table = db_handler.html_table().split('\n')
    assert '<table>' in table[0]
    assert '</table>' in table[-1]


def test_html(tmp_path: Path):
    # very general sanity test
    db_handler: DBHandler = DBHandler(db=db_path(tmp_path)).create_job()
    html = db_handler.html(query_type=EQueryType.TASKS_STATUS)
    assert '<body>' in html
    assert '</body>' in html

    html = html.split('\n')
    assert '<html>' in html[0]
    assert '</html>' in html[-1]


def test_html_file_str_dump(tmp_path: Path):
    # very general sanity test
    db_handler: DBHandler = DBHandler(db=db_path(tmp_path)).create_job()
    file = tmp_path / 'test.html'
    html = db_handler.html(query_type=EQueryType.TASKS_STATUS, file=file)

    assert file.exists()
    assert html == file.read_text()


def test_html_file_io_dump(tmp_path: Path):
    # very general sanity test
    db_handler: DBHandler = DBHandler(db=db_path(tmp_path)).create_job()
    file = tmp_path / 'test.html'
    with open(file, 'w') as f:
        html = db_handler.html(query_type=EQueryType.TASKS_STATUS, file=f)

    assert file.exists()
    assert html == file.read_text()


def test_task_job_delete_cascade(tmp_path: Path):
    # test that deleting a job deletes all its tasks
    db = db_path(tmp_path)
    db_handler1: DBHandler = DBHandler(db=db).create_job(name='job1')
    db_handler1.add_tasks([
        Task(),
        Task(),
        Task(),
    ])
    tasks = db_handler1.get_tasks()
    assert len(tasks) == 3

    db_handler2: DBHandler = DBHandler(db=db).create_job(name='job2')
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

def test_max_jobs(tmp_path):
    db = db_path(tmp_path)
    max_jobs = 10
    jobs_id = []
    for i in range(max_jobs * 2):
        jobs_id.append(DBHandler(db=db, max_jobs=max_jobs).create_job(name=f'job{i}').job_id) 
    jobs = DBHandler(db=db).get_jobs()   
    assert len(jobs) == 10
    
    remaining_jobs = [j.job_id for j in jobs]
    assert remaining_jobs == jobs_id[-max_jobs:]