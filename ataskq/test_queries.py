import pytest

from .ataskq import TaskQ, Task
from .handler import from_connection_str


@pytest.fixture()
def job_ids(conn):
    ret = []

    taskq = TaskQ(conn=conn).create_job(name="job1")
    taskq.add_tasks(
        [
            Task(name="n1", entrypoint=""),
            Task(name="n1", entrypoint=""),
            Task(name="n2", entrypoint=""),
        ]
    )
    ret.append(taskq.job_id)

    taskq = TaskQ(conn=conn).create_job(name="job2")
    taskq.add_tasks(
        [
            Task(name="n3", entrypoint=""),
            Task(name="n4", entrypoint=""),
            Task(name="n4", entrypoint=""),
        ]
    )
    ret.append(taskq.job_id)

    return ret


@pytest.fixture()
def handler(conn):
    return from_connection_str(conn)


def test_tasks_status(handler, job_ids):
    # job 1
    status = handler.tasks_status(job_ids[0])

    assert len(status) == 2

    assert status[0]["total"] == 2
    assert status[0]["name"] == "n1"
    assert status[0]["pending"] == 2

    assert status[1]["total"] == 1
    assert status[1]["name"] == "n2"
    assert status[1]["pending"] == 1

    # job 2
    status = handler.tasks_status(job_ids[1])

    assert len(status) == 2

    assert status[0]["total"] == 1
    assert status[0]["name"] == "n3"
    assert status[0]["pending"] == 1

    assert status[1]["total"] == 2
    assert status[1]["name"] == "n4"
    assert status[1]["pending"] == 2


def test_jobs_status(handler, job_ids):
    # job 1
    status = handler.jobs_status()

    assert len(status) == 2

    # order desc
    assert status[1]["job_id"] == job_ids[0]
    assert status[1]["name"] == "job1"
    assert status[1]["tasks"] == 3
    assert status[1]["pending"] == 3

    assert status[0]["job_id"] == job_ids[1]
    assert status[0]["name"] == "job2"
    assert status[0]["tasks"] == 3
    assert status[0]["pending"] == 3
