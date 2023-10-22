from pathlib import Path
from datetime import datetime, timedelta

import pytest

from ataskq import TaskQ, Task, targs, EStatus


def db_path(tmp_path):
    return f'sqlite://{tmp_path}/ataskq.db'


def test_create_job(tmp_path: Path):
    taskq = TaskQ(db=db_path(tmp_path)).create_job(overwrite=True)
    assert isinstance(taskq, TaskQ)

    assert (tmp_path / 'ataskq.db').exists()
    assert (tmp_path / 'ataskq.db').is_file()


def test_run_default(tmp_path: Path):
    filepath = tmp_path / 'file.txt'

    taskq = TaskQ(db=db_path(tmp_path)).create_job(overwrite=True)

    taskq.add_tasks([
        Task(entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file",
             targs=targs(filepath, 'task 0\n')),
        Task(entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file",
             targs=targs(filepath, 'task 1\n')),
        Task(entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file",
             targs=targs(filepath, 'task 2\n')),
    ])

    taskq.run()

    assert filepath.exists()
    assert filepath.read_text() == \
        "task 0\n" \
        "task 1\n" \
        "task 2\n"


def test_run_task_raise_exception(tmp_path: Path):
    # no exception raised
    try:
        taskq: TaskQ = TaskQ(db=db_path(tmp_path), run_task_raise_exception=False).create_job(overwrite=True)
        taskq.add_tasks([
            Task(entrypoint="ataskq.tasks_utils.exception_task",
                 targs=targs(message="task failed")),
        ])
        taskq.run()
    except:
        assert False, f"exception_task raises exception with run_task_raise_exception=False"

    # exception raised
    taskq: TaskQ = TaskQ(db=db_path(tmp_path), run_task_raise_exception=True).create_job(overwrite=True)
    taskq.add_tasks([
        Task(entrypoint="ataskq.tasks_utils.exception_task",
             targs=targs(message="task failed")),
    ])
    with pytest.raises(Exception) as excinfo:
        taskq.run()
    assert excinfo.value.args[0] == "task failed"


def test_run_2_processes(tmp_path: Path):
    filepath = tmp_path / 'file.txt'

    taskq = TaskQ(db=db_path(tmp_path)).create_job(overwrite=True)

    taskq.add_tasks([
        Task(entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file_mp_lock",
             targs=targs(filepath, 'task 0\n')),
        Task(entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file_mp_lock",
             targs=targs(filepath, 'task 1\n')),
        Task(entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file_mp_lock",
             targs=targs(filepath, 'task 2\n')),
    ])

    taskq.run(num_processes=2)

    assert filepath.exists()
    text = filepath.read_text()
    assert "task 0\n" in text
    assert "task 1\n" in text
    assert "task 1\n" in text


def _test_run_by_level(tmp_path: Path, num_processes: int):
    filepath = tmp_path / 'file.txt'

    taskq = TaskQ(db=db_path(tmp_path)).create_job(overwrite=True)

    taskq.add_tasks([
        Task(level=0, entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file_mp_lock",
             targs=targs(filepath, 'task 0\n')),
        Task(level=1, entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file_mp_lock",
             targs=targs(filepath, 'task 1\n')),
        Task(level=1, entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file_mp_lock",
             targs=targs(filepath, 'task 2\n')),
        Task(level=2, entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file_mp_lock",
             targs=targs(filepath, 'task 3\n')),
    ])

    assert taskq.count_pending_tasks_below_level(3) == 4

    assert taskq.count_pending_tasks_below_level(1) == 1
    taskq.run(level=0, num_processes=num_processes)
    taskq.count_pending_tasks_below_level(1) == 0
    assert filepath.exists()
    text = filepath.read_text()
    assert "task 0\n" in text

    assert taskq.count_pending_tasks_below_level(2) == 2
    taskq.run(level=1, num_processes=num_processes)
    taskq.count_pending_tasks_below_level(2) == 0
    text = filepath.read_text()
    assert "task 0\n" in text
    assert "task 1\n" in text
    assert "task 2\n" in text

    assert taskq.count_pending_tasks_below_level(3) == 1
    taskq.run(level=2, num_processes=num_processes)
    taskq.count_pending_tasks_below_level(3) == 0
    text = filepath.read_text()
    assert "task 0\n" in text
    assert "task 1\n" in text
    assert "task 2\n" in text
    assert "task 3\n" in text


def test_run_by_level(tmp_path: Path):
    _test_run_by_level(tmp_path, num_processes=None)


def test_run_by_level_2_processes(tmp_path: Path):
    _test_run_by_level(tmp_path, num_processes=2)


def test_monitor_pulse_failure(tmp_path):
    # set monitor pulse longer than timeout
    taskq = TaskQ(db=db_path(tmp_path), monitor_pulse_interval=10,
                  monitor_timeout_internal=1.5).create_job(overwrite=True)
    taskq.add_tasks([
        # reserved keyward for ignored task for testing
        Task(entrypoint='ataskq.skip_run_task', targs=targs('task will fail')),
        Task(entrypoint='ataskq.tasks_utils.dummy_args_task', targs=targs('task will success')),
    ])
    start = datetime.now()
    taskq.run()
    stop = datetime.now()

    tasks = taskq.get_tasks()

    assert tasks[0].status == EStatus.FAILURE
    assert tasks[1].status == EStatus.SUCCESS
    assert stop - start > timedelta(seconds=1.5)
