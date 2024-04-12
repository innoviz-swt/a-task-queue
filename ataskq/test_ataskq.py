from pathlib import Path
from datetime import datetime, timedelta
from copy import copy

import pytest

from ataskq import TaskQ, StateKWArg, Job, Task, targs, EStatus
from ataskq.handler import DBHandler
from ataskq.handler import EAction, from_connection_str

from .tasks_utils import dummy_args_task


@pytest.fixture
def jtaskq(conn) -> TaskQ:
    return TaskQ(conn=conn).create_job()


def test_create_job(conn):
    taskq = TaskQ(conn=conn).create_job()
    assert isinstance(taskq, TaskQ)

    if "sqlite" in conn:
        assert Path(taskq.handler.db_path).exists()
        assert Path(taskq.handler.db_path).is_file()
    elif "pg" in conn:
        pass
    elif "http" in conn:
        pass
    else:
        raise Exception(f"unknown db type in connection string '{conn}'")


def test_job_default_name(conn):
    job = TaskQ(conn=conn).create_job().job
    assert job.name is None


def test_job_custom_name(conn):
    job = TaskQ(conn=conn).create_job(name="my_job").job
    assert job.name == "my_job"


def test_task_job_delete_cascade(conn):
    # test that deleting a job deletes all its tasks
    handler = from_connection_str(conn=conn)
    taskq1: TaskQ = TaskQ(conn=handler).create_job(name="job1")
    taskq2: TaskQ = TaskQ(conn=handler).create_job(name="job2")
    assert Job.count_all(_handler=handler) == 2

    taskq1.add_tasks(
        [
            Task(entrypoint=""),
            Task(entrypoint=""),
            Task(entrypoint=""),
        ]
    )
    assert Task.count_all(_handler=handler) == 3
    assert len(taskq1.get_tasks()) == 3

    taskq2.add_tasks(
        [
            Task(entrypoint=""),
            Task(entrypoint=""),
        ]
    )
    assert Task.count_all(_handler=handler) == 5
    assert len(taskq1.get_tasks()) == 3
    assert len(taskq2.get_tasks()) == 2

    taskq2.delete_job()
    assert len(Job.get_all(_handler=handler)) == 1

    assert Task.count_all(_handler=handler) == 3
    assert len(taskq1.get_tasks()) == 3

    taskq1.delete_job()
    assert Task.count_all(_handler=handler) == 0


def test_state_kwargs_job_delete_cascade(conn):
    # todo: test should ne under ataskq
    handler = from_connection_str(conn=conn)
    taskq1: TaskQ = TaskQ(conn=handler).create_job(name="job1")
    taskq2: TaskQ = TaskQ(conn=handler).create_job(name="job2")
    assert len(Job.get_all(_handler=handler)) == 2

    taskq1.add_state_kwargs(
        [
            StateKWArg(entrypoint=""),
            StateKWArg(entrypoint=""),
            StateKWArg(entrypoint=""),
        ]
    )
    assert len(StateKWArg.get_all(_handler=handler)) == 3
    assert len(taskq1.get_state_kwargs()) == 3

    taskq2.add_state_kwargs(
        [
            StateKWArg(entrypoint=""),
            StateKWArg(entrypoint=""),
        ]
    )
    assert len(StateKWArg.get_all(_handler=handler)) == 5
    assert len(taskq1.get_state_kwargs()) == 3
    assert len(taskq2.get_state_kwargs()) == 2

    taskq2.delete_job()
    assert len(Job.get_all(_handler=handler)) == 1

    assert len(StateKWArg.get_all(_handler=handler)) == 3
    assert len(taskq1.get_state_kwargs()) == 3

    taskq1.delete_job()
    assert len(StateKWArg.get_all(_handler=handler)) == 0


def test_update_task_start_time(jtaskq):
    in_task1 = Task(entrypoint="ataskq.tasks_utils.dummy_args_task", level=1, name="task1")

    jtaskq.add_tasks(
        [
            in_task1,
        ]
    )

    tasks = jtaskq.get_tasks()
    assert len(tasks) == 1  # sanity
    task: Task = tasks[0]

    # update db with task time (also updates task inplace)
    start_time = datetime.now()
    start_time = start_time.replace(microsecond=0)  # test to seconds resolution
    jtaskq.update_task_start_time(task, start_time)
    assert task.start_time == start_time

    # get task form db and validate
    tasks = jtaskq.get_tasks()
    assert len(tasks) == 1  # sanity
    task: Task = tasks[0]
    assert task.start_time == start_time


def test_update_task_status(jtaskq):
    in_task1 = Task(entrypoint="ataskq.tasks_utils.dummy_args_task", level=1, name="task1")

    jtaskq.add_tasks(
        [
            in_task1,
        ]
    )

    tasks = jtaskq.get_tasks()
    assert len(tasks) == 1  # sanity
    task: Task = tasks[0]
    assert task.status == EStatus.PENDING

    now = datetime.now()
    # update db with task status (also updates task inplace)
    jtaskq.update_task_status(task, EStatus.RUNNING, timestamp=now)
    assert task.status == EStatus.RUNNING
    assert task.pulse_time == now
    assert task.done_time is None
    # get task form db and validate
    tasks = jtaskq.get_tasks()
    assert len(tasks) == 1  # sanity
    task: Task = tasks[0]
    assert task.status == EStatus.RUNNING
    assert task.pulse_time == now
    assert task.done_time is None

    # update db with task status (also updates task inplace)
    jtaskq.update_task_status(task, EStatus.SUCCESS, timestamp=now)
    assert task.status == EStatus.SUCCESS
    assert task.pulse_time == now
    assert task.done_time == now
    # get task form db and validate
    tasks = jtaskq.get_tasks()
    assert len(tasks) == 1  # sanity
    task: Task = tasks[0]
    assert task.status == EStatus.SUCCESS
    assert task.pulse_time == now
    assert task.done_time == now


def _compare_task_taken(task1: Task, task2: Task, job_id=None):
    if job_id:
        assert task1.job_id == job_id
    assert task1.job_id == task2.job_id
    assert task1.name == task2.name
    assert task1.description == task2.description
    assert task1.level == task2.level
    assert task1.entrypoint == task2.entrypoint
    assert task1.targs == task2.targs


def test_take_next_task_sanity(jtaskq):
    in_task1 = Task(entrypoint="ataskq.tasks_utils.dummy_args_task", level=1, name="task1")

    jtaskq.add_tasks(
        [
            in_task1,
        ]
    )

    action, task = jtaskq._take_next_task(level=None)
    assert action == EAction.RUN_TASK
    _compare_task_taken(in_task1, task)


def test_take_next_task(jtaskq):
    in_task1 = Task(entrypoint="ataskq.tasks_utils.dummy_args_task", level=1, name="task1")
    in_task2 = Task(entrypoint="ataskq.tasks_utils.dummy_args_task", level=2, name="task2")
    in_task3 = copy(in_task1)

    jtaskq.add_tasks(
        [
            in_task2,
            in_task1,
            in_task3,
        ]
    )

    tids = []

    action, task = jtaskq._take_next_task(level=None)
    assert action == EAction.RUN_TASK
    assert task.task_id not in tids
    tids.append(task.task_id)
    _compare_task_taken(in_task1, task)

    action, task = jtaskq._take_next_task(level=None)
    assert action == EAction.RUN_TASK
    assert task.task_id not in tids
    tids.append(task.task_id)
    _compare_task_taken(in_task3, task)  # note in_task3 is copy of in_task1

    action, task = jtaskq._take_next_task(level=None)
    assert action == EAction.WAIT
    assert task is None


def test_take_next_task_2_jobs(conn):
    # todo: test should ne under ataskq
    handler = from_connection_str(conn=conn)
    taskq1: TaskQ = TaskQ(conn=handler).create_job(name="job1")
    taskq2: TaskQ = TaskQ(conn=handler).create_job(name="job2")

    in_task1 = Task(entrypoint="ataskq.tasks_utils.dummy_args_task", level=2, name="taska")
    in_task2 = Task(entrypoint="ataskq.tasks_utils.dummy_args_task", level=1, name="taskb")
    in_task3 = Task(entrypoint="ataskq.tasks_utils.dummy_args_task", level=1, name="taskb")
    in_task4 = Task(entrypoint="ataskq.tasks_utils.dummy_args_task", level=1, name="taskd")
    in_task5 = Task(entrypoint="ataskq.tasks_utils.dummy_args_task", level=2, name="taske")

    taskq1.add_tasks(
        [
            in_task2,
            in_task1,
            in_task3,
        ]
    )

    taskq2.add_tasks(
        [
            in_task5,
            in_task4,
        ]
    )

    jid1 = taskq1.job_id
    jid2 = taskq2.job_id
    tids = []

    # sanity check
    assert len(Job.get_all(_handler=handler)) == 2
    assert len(taskq1.get_tasks()) == 3
    assert len(Job.get_all(_handler=handler)) == 2
    assert len(taskq2.get_tasks()) == 2

    # db handler 1
    action, task = taskq1._take_next_task(level=None)
    assert action == EAction.RUN_TASK
    assert task.task_id not in tids
    tids.append(task.task_id)
    _compare_task_taken(in_task2, task, job_id=jid1)
    tids.append(task.task_id)

    action, task = taskq1._take_next_task(level=None)
    assert action == EAction.RUN_TASK
    assert task.task_id not in tids
    tids.append(task.task_id)
    _compare_task_taken(in_task3, task, job_id=jid1)  # note in_task3 is copy of in_task1

    action, task = taskq1._take_next_task(level=None)
    assert action == EAction.WAIT
    assert task is None

    # db handler 2
    action, task = taskq2._take_next_task(level=None)
    assert action == EAction.RUN_TASK
    assert task.task_id not in tids
    tids.append(task.task_id)
    _compare_task_taken(in_task4, task, job_id=jid2)
    tids.append(task.task_id)

    action, task = taskq2._take_next_task(level=None)
    assert action == EAction.WAIT
    assert task is None


def test_get_tasks(jtaskq):
    in_task1 = Task(entrypoint=dummy_args_task, level=1, name="task1")
    in_task2 = Task(entrypoint=dummy_args_task, level=2, name="task2")
    in_task3 = Task(entrypoint=dummy_args_task, level=3, name="task3")

    jtaskq.add_tasks(
        [
            in_task3,
            in_task2,
            in_task1,
        ]
    )

    tasks = jtaskq.get_tasks()
    assert len(tasks) == 3
    for t in tasks:
        if t.level == 1:
            _compare_task_taken(in_task1, t)
        elif t.level == 2:
            _compare_task_taken(in_task2, t)
        elif t.level == 3:
            _compare_task_taken(in_task3, t)


def test_run_default(conn, tmp_path: Path):
    filepath = tmp_path / "file.txt"

    taskq = TaskQ(conn=conn).create_job()

    taskq.add_tasks(
        [
            Task(entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file", targs=targs(filepath, "task 0\n")),
            Task(entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file", targs=targs(filepath, "task 1\n")),
            Task(entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file", targs=targs(filepath, "task 2\n")),
        ]
    )

    taskq.run()

    assert filepath.exists()
    assert filepath.read_text() == "task 0\n" "task 1\n" "task 2\n"


def test_run_task_raise_exception(conn):
    # no exception raised
    try:
        taskq: TaskQ = TaskQ(conn=conn, run_task_raise_exception=False).create_job()
        taskq.add_tasks(
            [
                Task(entrypoint="ataskq.tasks_utils.exception_task", targs=targs(message="task failed")),
            ]
        )
        taskq.run()
    except Exception:
        assert False, "exception_task raises exception with run_task_raise_exception=False"

    # exception raised
    taskq: TaskQ = TaskQ(conn=conn, run_task_raise_exception=True).create_job()
    taskq.add_tasks(
        [
            Task(entrypoint="ataskq.tasks_utils.exception_task", targs=targs(message="task failed")),
        ]
    )
    with pytest.raises(Exception) as excinfo:
        taskq.run()
    assert excinfo.value.args[0] == "task failed"


def test_run_2_processes(conn, tmp_path: Path):
    filepath = tmp_path / "file.txt"

    taskq = TaskQ(conn=conn).create_job()

    taskq.add_tasks(
        [
            Task(
                entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file_mp_lock",
                targs=targs(filepath, "task 0\n"),
            ),
            Task(
                entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file_mp_lock",
                targs=targs(filepath, "task 1\n"),
            ),
            Task(
                entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file_mp_lock",
                targs=targs(filepath, "task 2\n"),
            ),
        ]
    )

    taskq.run(num_processes=2)

    assert filepath.exists()
    text = filepath.read_text()
    assert "task 0\n" in text
    assert "task 1\n" in text
    assert "task 1\n" in text


@pytest.mark.parametrize("num_processes", [None, 2])
def test_run_by_level(conn, tmp_path: Path, num_processes: int):
    filepath = tmp_path / "file.txt"

    taskq = TaskQ(conn=conn).create_job()

    taskq.add_tasks(
        [
            Task(
                level=0,
                entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file_mp_lock",
                targs=targs(filepath, "task 0\n"),
            ),
            Task(
                level=1,
                entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file_mp_lock",
                targs=targs(filepath, "task 1\n"),
            ),
            Task(
                level=1,
                entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file_mp_lock",
                targs=targs(filepath, "task 2\n"),
            ),
            Task(
                level=2,
                entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file_mp_lock",
                targs=targs(filepath, "task 3\n"),
            ),
        ]
    )

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


def test_monitor_pulse_failure(conn):
    # set monitor pulse longer than timeout
    taskq = TaskQ(conn=conn, monitor_pulse_interval=10, task_pulse_timeout=1.5).create_job()
    taskq.add_tasks(
        [
            # reserved keyward for ignored task for testing
            Task(entrypoint="ataskq.skip_run_task", targs=targs("task will fail")),
            Task(entrypoint="ataskq.tasks_utils.dummy_args_task", targs=targs("task will success")),
        ]
    )
    start = datetime.now()
    taskq.run()
    stop = datetime.now()

    tasks = taskq.get_tasks()

    assert tasks[0].status == EStatus.FAILURE
    assert tasks[1].status == EStatus.SUCCESS
    assert stop - start > timedelta(seconds=1.5)


def test_task_wait_timeout(conn):
    # set monitor pulse longer than timeout
    taskq = TaskQ(conn=conn, run_task_raise_exception=True, task_wait_timeout=0).create_job()
    taskq.add_tasks(
        [
            Task(entrypoint="ataskq.tasks_utils.dummy_args_task", level=1, targs=targs("task will success", sleep=0.2)),
            Task(entrypoint="ataskq.tasks_utils.dummy_args_task", level=2, targs=targs("task will success")),
        ]
    )

    with pytest.raises(Exception) as excinfo:
        taskq.run(num_processes=2)
    assert excinfo.value.args[0] == "Some processes failed, see logs for details"


def test_run_with_state_kwargs(conn):
    from ataskq.tasks_utils.counter_task import counter_kwarg, counter_task

    taskq = TaskQ(conn=conn, run_task_raise_exception=True).create_job()

    taskq.add_state_kwargs(
        [
            StateKWArg(entrypoint=counter_kwarg, name="counter"),
        ]
    )

    taskq.add_tasks(
        [
            Task(entrypoint=counter_task, targs=targs(print_counter=True)),
        ]
    )

    taskq.run()


def test_max_jobs(conn):
    max_jobs = 10
    taskq = TaskQ(conn=conn, max_jobs=max_jobs)
    if not isinstance(taskq.handler, DBHandler):
        pytest.skip()

    jobs_id = []
    for i in range(max_jobs * 2):
        taskq.clear_job()
        taskq.create_job(name=f"job{i}")
        jobs_id.append(taskq.job.job_id)
    jobs = Job.get_all(taskq.handler)
    assert len(jobs) == 10

    remaining_jobs = [j.job_id for j in jobs]
    assert remaining_jobs == jobs_id[-max_jobs:]
