from pathlib import Path
from datetime import datetime, timedelta

import pytest

from ataskq import TaskQ, StateKWArg, Task, targs, EStatus


def test_create_job(conn):
    taskq = TaskQ(conn=conn).create_job()
    assert isinstance(taskq, TaskQ)

    if 'sqlite' in conn:
        assert Path(taskq.db_handler.db_path).exists()
        assert Path(taskq.db_handler.db_path).is_file()
    elif 'postgresql' in conn:
        pass
    else:
        raise Exception(f"unknown db type in connection string '{conn}'")


def test_run_default(conn, tmp_path: Path):
    filepath = tmp_path / 'file.txt'

    taskq = TaskQ(conn=conn).create_job()

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


def test_run_task_raise_exception(conn):
    # no exception raised
    try:
        taskq: TaskQ = TaskQ(conn=conn, run_task_raise_exception=False).create_job()
        taskq.add_tasks([
            Task(entrypoint="ataskq.tasks_utils.exception_task",
                 targs=targs(message="task failed")),
        ])
        taskq.run()
    except Exception:
        assert False, f"exception_task raises exception with run_task_raise_exception=False"

    # exception raised
    taskq: TaskQ = TaskQ(conn=conn, run_task_raise_exception=True).create_job()
    taskq.add_tasks([
        Task(entrypoint="ataskq.tasks_utils.exception_task",
             targs=targs(message="task failed")),
    ])
    with pytest.raises(Exception) as excinfo:
        taskq.run()
    assert excinfo.value.args[0] == "task failed"


def test_run_2_processes(conn, tmp_path: Path):
    filepath = tmp_path / 'file.txt'

    taskq = TaskQ(conn=conn).create_job()

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


def _test_run_by_level(conn, tmp_path: Path, num_processes: int):
    filepath = tmp_path / 'file.txt'

    taskq = TaskQ(conn=conn).create_job()

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


def test_run_by_level(conn, tmp_path: Path):
    _test_run_by_level(conn, tmp_path, num_processes=None)


def test_run_by_level_2_processes(conn, tmp_path: Path):
    _test_run_by_level(conn, tmp_path, num_processes=2)


def test_monitor_pulse_failure(conn):
    # set monitor pulse longer than timeout
    taskq = TaskQ(conn=conn, monitor_pulse_interval=10,
                  monitor_timeout_internal=1.5).create_job()
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


def test_run_with_state_kwargs(conn):
    from ataskq.tasks_utils.counter_task import counter_kwarg, counter_task
    taskq = TaskQ(conn=conn, run_task_raise_exception=True).create_job()

    taskq.add_state_kwargs([
        StateKWArg(entrypoint=counter_kwarg, name='counter'),
    ])

    taskq.add_tasks([
        Task(entrypoint=counter_task,
             targs=targs(print_counter=True)),])

    taskq.run()
