import json

import pytest

from . import TaskQ, Task
from .tasks_utils.write_to_file_tasks import write_to_file, write_to_file_mp_lock
from .__main__ import main


def test_empty():
    try:
        args = []
        main(args=args)
    except Exception as ex:
        pytest.fail("main without args failed")


def test_run_job(tmp_path, config):
    filepath = tmp_path / "file.txt"
    with open(configpath := tmp_path / "config.json", "w") as f:
        json.dump(config, f)

    taskq = TaskQ(config=config).create_job()

    taskq.add_tasks(
        [
            Task(entrypoint=write_to_file, kwargs=dict(filepath=filepath, text="task 0\n")),
            Task(entrypoint=write_to_file, kwargs=dict(filepath=filepath, text="task 1\n")),
            Task(entrypoint=write_to_file, kwargs=dict(filepath=filepath, text="task 2\n")),
        ]
    )

    args = ["run", "-c", str(configpath), "--job-id", str(taskq.job_id)]
    main(args=args)

    assert filepath.exists()
    assert filepath.read_text() == "task 0\n" "task 1\n" "task 2\n"


def test_run_level(tmp_path, config):
    filepath = tmp_path / "file.txt"
    with open(configpath := tmp_path / "config.json", "w") as f:
        json.dump(config, f)

    taskq = TaskQ(config=config).create_job()

    taskq.add_tasks(
        [
            Task(entrypoint=write_to_file, level=0, kwargs=dict(filepath=filepath, text="task 0\n")),
            Task(entrypoint=write_to_file, level=0, kwargs=dict(filepath=filepath, text="task 1\n")),
            Task(entrypoint=write_to_file, level=1, kwargs=dict(filepath=filepath, text="task 2\n")),
            Task(entrypoint=write_to_file, level=2, kwargs=dict(filepath=filepath, text="task 3\n")),
            Task(entrypoint=write_to_file, level=3, kwargs=dict(filepath=filepath, text="task 4\n")),
        ]
    )

    # run level 0
    args = ["run", "-c", str(configpath), "--job-id", str(taskq.job_id), "--level", "0"]
    main(args=args)

    assert filepath.exists()
    assert filepath.read_text() == "task 0\n" "task 1\n"

    # run level 1
    args = ["run", "-c", str(configpath), "--job-id", str(taskq.job_id), "--level", "1"]
    main(args=args)

    assert filepath.exists()
    assert filepath.read_text() == "task 0\n" "task 1\n" "task 2\n"

    # run level 2, 3
    args = ["run", "-c", str(configpath), "--job-id", str(taskq.job_id), "--level", "2", "4"]
    main(args=args)

    assert filepath.exists()
    assert filepath.read_text() == "task 0\n" "task 1\n" "task 2\n" "task 3\n" "task 4\n"


def test_run_concurrency(tmp_path, config):
    filepath = tmp_path / "file.txt"
    with open(configpath := tmp_path / "config.json", "w") as f:
        json.dump(config, f)

    taskq = TaskQ(config=config).create_job()

    taskq.add_tasks(
        [
            Task(entrypoint=write_to_file_mp_lock, kwargs=dict(filepath=filepath, text="@{pid}\n", sleep=1)),
            Task(entrypoint=write_to_file_mp_lock, kwargs=dict(filepath=filepath, text="@{pid}\n", sleep=1)),
            Task(entrypoint=write_to_file_mp_lock, kwargs=dict(filepath=filepath, text="@{pid}\n", sleep=1)),
            Task(entrypoint=write_to_file_mp_lock, kwargs=dict(filepath=filepath, text="@{pid}\n", sleep=1)),
            Task(entrypoint=write_to_file_mp_lock, kwargs=dict(filepath=filepath, text="@{pid}\n", sleep=1)),
        ]
    )

    args = ["run", "-c", str(configpath), "--job-id", str(taskq.job_id), "--concurrency", "3"]
    main(args=args)

    assert filepath.exists()
    assert len(set([l for l in filepath.read_text().split("\n") if l])) == 3
