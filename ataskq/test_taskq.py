from pathlib import Path
from datetime import datetime, timedelta
from copy import copy
from multiprocessing import Process
import time

import pytest

from . import TaskQ, Job, Task, targs, EStatus
from .handler import DBHandler
from .handler import EAction, from_config

from .tasks_utils import dummy_args_task, write_to_file


def non_decreasing(L):
    return all(x <= y for x, y in zip(L, L[1:]))


def non_increasing(L):
    return all(x >= y for x, y in zip(L, L[1:]))


def monotonic(L):
    return non_decreasing(L) or non_increasing(L)


@pytest.fixture
def jtaskq(config) -> TaskQ:
    return TaskQ(config=config).create_job()


def test_create_job(config):
    taskq = TaskQ(config=config).create_job()
    assert isinstance(taskq, TaskQ)

    conn = config["connection"]
    if "sqlite" in conn:
        assert Path(taskq.handler.db_path).exists()
        assert Path(taskq.handler.db_path).is_file()
    elif "pg" in conn:
        pass
    elif "http" in conn:
        pass
    else:
        raise Exception(f"unknown db type in connection string '{conn}'")


def test_job_default_name(config):
    job = TaskQ(config=config).create_job().job
    assert job.name is None


def test_job_custom_name(config):
    job = TaskQ(config=config).create_job(name="my_job").job
    assert job.name == "my_job"


def test_task_job_delete_cascade(config):
    # test that deleting a job deletes all its tasks
    handler = from_config(config)
    taskq1: TaskQ = TaskQ(handler=handler).create_job(name="job1")
    taskq2: TaskQ = TaskQ(handler=handler).create_job(name="job2")
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


def test_update_task_start_time(jtaskq):
    in_task1 = Task(entrypoint=dummy_args_task, level=1, name="task1")

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
    in_task1 = Task(entrypoint=dummy_args_task, level=1, name="task1")

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


def _compare_tasks(task1: Task, task2: Task, job_id=None):
    if job_id:
        assert task1.job_id == job_id
    assert task1.job_id == task2.job_id
    assert task1.name == task2.name
    assert task1.description == task2.description
    assert task1.level == task2.level
    assert task1.entrypoint == task2.entrypoint
    assert task1.targs == task2.targs


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
            _compare_tasks(in_task1, t)
        elif t.level == 2:
            _compare_tasks(in_task2, t)
        elif t.level == 3:
            _compare_tasks(in_task3, t)


def test_take_next_task_sanity(jtaskq):
    in_task1 = Task(entrypoint=dummy_args_task, level=1, name="task1")

    jtaskq.add_tasks(
        [
            in_task1,
        ]
    )

    action, task = jtaskq._take_next_task(level=None)
    assert action == EAction.RUN_TASK
    _compare_tasks(in_task1, task)


def test_take_next_task(jtaskq):
    in_task1 = Task(entrypoint=dummy_args_task, level=1, name="task1")
    in_task2 = Task(entrypoint=dummy_args_task, level=2, name="task2")
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
    _compare_tasks(in_task1, task)

    action, task = jtaskq._take_next_task(level=None)
    assert action == EAction.RUN_TASK
    assert task.task_id not in tids
    tids.append(task.task_id)
    _compare_tasks(in_task3, task)  # note in_task3 is copy of in_task1

    action, task = jtaskq._take_next_task(level=None)
    assert action == EAction.WAIT
    assert task is None


def test_take_next_task_2_jobs(config):
    # todo: test should ne under ataskq
    handler = from_config(config)
    taskq1: TaskQ = TaskQ(handler=handler).create_job(name="job1")
    taskq2: TaskQ = TaskQ(handler=handler).create_job(name="job2")

    in_task1 = Task(entrypoint=dummy_args_task, level=2, name="taska")
    in_task2 = Task(entrypoint=dummy_args_task, level=1, name="taskb")
    in_task3 = Task(entrypoint=dummy_args_task, level=1, name="taskb")
    in_task4 = Task(entrypoint=dummy_args_task, level=1, name="taskd")
    in_task5 = Task(entrypoint=dummy_args_task, level=2, name="taske")

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

    # taskq 1
    action, task = taskq1._take_next_task(level=None)
    assert action == EAction.RUN_TASK
    assert task.task_id not in tids
    tids.append(task.task_id)
    _compare_tasks(in_task2, task, job_id=jid1)
    tids.append(task.task_id)

    action, task = taskq1._take_next_task(level=None)
    assert action == EAction.RUN_TASK
    assert task.task_id not in tids
    tids.append(task.task_id)
    _compare_tasks(in_task3, task, job_id=jid1)  # note in_task3 is copy of in_task1

    action, task = taskq1._take_next_task(level=None)
    assert action == EAction.WAIT
    assert task is None

    # taskq 2
    action, task = taskq2._take_next_task(level=None)
    assert action == EAction.RUN_TASK
    assert task.task_id not in tids
    tids.append(task.task_id)
    _compare_tasks(in_task4, task, job_id=jid2)
    tids.append(task.task_id)

    action, task = taskq2._take_next_task(level=None)
    assert action == EAction.WAIT
    assert task is None


def test_take_next_all_jobs(config):
    handler = from_config(config)
    taskq1: TaskQ = TaskQ(handler=handler).create_job(name="job1")
    taskq2: TaskQ = TaskQ(handler=handler).create_job(name="job2")

    def init_task(name):
        return Task(entrypoint=dummy_args_task, name=name)

    in_task1 = init_task("job1 - task1")
    in_task2 = init_task("job1 - task2")
    taskq1.add_tasks(
        [
            in_task1,
            in_task2,
        ]
    )

    in_task4 = init_task("job2 - task4")
    in_task5 = init_task("job2 - task5")
    taskq2.add_tasks(
        [
            in_task4,
            in_task5,
        ]
    )

    in_task3 = init_task("job1 - task3")
    taskq1.add_tasks(
        [
            in_task3,
        ]
    )

    jid1 = taskq1.job_id
    jid2 = taskq2.job_id

    # sanity check
    assert len(Job.get_all(_handler=handler)) == 2
    assert len(taskq1.get_tasks()) == 3
    assert len(Job.get_all(_handler=handler)) == 2
    assert len(taskq2.get_tasks()) == 2

    taskq = TaskQ(config=config)
    in_tasks = [in_task1, in_task2, in_task3, in_task4, in_task5]
    in_jids = [jid1, jid1, jid1, jid2, jid2]
    for t, jid in zip(in_tasks, in_jids):
        action, task = taskq._take_next_task()
        assert action == EAction.RUN_TASK
        assert task.task_id == t.task_id
        _compare_tasks(t, task, job_id=jid)

    assert not monotonic([t.task_id for t in in_tasks])


def test_run_default(config, tmp_path: Path):
    filepath = tmp_path / "file.txt"

    taskq = TaskQ(config=config).create_job()

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


def test_run_task_raise_exception(config):
    # no exception raised
    try:
        config["run"]["raise_exception"] = False
        taskq: TaskQ = TaskQ(config=config).create_job()
        taskq.add_tasks(
            [
                Task(entrypoint="ataskq.tasks_utils.exception_task", targs=targs(message="task failed")),
            ]
        )
        taskq.run()
    except Exception:
        assert False, "exception_task raises exception with run_task_raise_exception=False"

    # exception raised
    config["run"]["raise_exception"] = True
    taskq: TaskQ = TaskQ(config=config).create_job()
    taskq.add_tasks(
        [
            Task(entrypoint="ataskq.tasks_utils.exception_task", targs=targs(message="task failed")),
        ]
    )
    with pytest.raises(Exception) as excinfo:
        taskq.run()
    assert excinfo.value.args[0] == "task failed"


def test_run_2_processes(config, tmp_path: Path):
    filepath = tmp_path / "file.txt"

    taskq = TaskQ(config=config).create_job()

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

    taskq.run(concurrency=2)

    assert filepath.exists()
    text = filepath.read_text()
    assert "task 0\n" in text
    assert "task 1\n" in text
    assert "task 1\n" in text


def test_run_all_jobs(config, tmp_path: Path):
    filepath1 = tmp_path / "file1.txt"
    taskq1 = TaskQ(config=config).create_job()
    taskq1.add_tasks(
        [
            Task(
                entrypoint=write_to_file,
                targs=targs(filepath1, "task 0\n"),
            ),
        ]
    )

    filepath2 = tmp_path / "file2.txt"
    taskq2 = TaskQ(config=config).create_job()
    taskq2.add_tasks(
        [
            Task(
                entrypoint=write_to_file,
                targs=targs(filepath2, "task 1\n"),
            ),
        ]
    )

    TaskQ(config=config).run()

    assert filepath1.exists()
    assert "task 0\n" == filepath1.read_text()
    assert filepath2.exists()
    assert "task 1\n" == filepath2.read_text()


@pytest.mark.parametrize("num_processes", [None, 2])
def test_run_by_level(config, tmp_path: Path, num_processes: int):
    filepath = tmp_path / "file.txt"

    taskq = TaskQ(config=config).create_job()

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
    taskq.run(level=0, concurrency=num_processes)
    taskq.count_pending_tasks_below_level(1) == 0
    assert filepath.exists()
    text = filepath.read_text()
    assert "task 0\n" in text

    assert taskq.count_pending_tasks_below_level(2) == 2
    taskq.run(level=1, concurrency=num_processes)
    taskq.count_pending_tasks_below_level(2) == 0
    text = filepath.read_text()
    assert "task 0\n" in text
    assert "task 1\n" in text
    assert "task 2\n" in text

    assert taskq.count_pending_tasks_below_level(3) == 1
    taskq.run(level=2, concurrency=num_processes)
    taskq.count_pending_tasks_below_level(3) == 0
    text = filepath.read_text()
    assert "task 0\n" in text
    assert "task 1\n" in text
    assert "task 2\n" in text
    assert "task 3\n" in text


def test_monitor_pulse_failure(config):
    # set monitor pulse longer than timeout
    config["monitor"]["pulse_interval"] = 2
    config["monitor"]["pulse_timeout"] = 1.5
    taskq = TaskQ(config=config).create_job()
    taskq.add_tasks(
        [
            # reserved keyward for ignored task for testing
            Task(entrypoint="ataskq.skip_run_task", targs=targs("task will fail")),
            Task(entrypoint=dummy_args_task, targs=targs("task will success")),
        ]
    )
    start = datetime.now()
    taskq.run()
    stop = datetime.now()

    tasks = taskq.get_tasks()

    assert tasks[0].status == EStatus.FAILURE
    assert tasks[1].status == EStatus.SUCCESS
    assert stop - start > timedelta(seconds=1.5)


def test_task_wait_timeout(config):
    # set monitor pulse longer than timeout
    config["run"]["raise_exception"] = True
    config["run"]["wait_timeout"] = 0
    taskq = TaskQ(config=config).create_job()
    taskq.add_tasks(
        [
            Task(entrypoint=dummy_args_task, level=1, targs=targs("task will success", sleep=0.2)),
            Task(entrypoint=dummy_args_task, level=2, targs=targs("task will success")),
        ]
    )

    with pytest.raises(Exception) as excinfo:
        taskq.run(concurrency=2)
    assert excinfo.value.args[0] == "Some processes failed, see logs for details"


def test_max_jobs(config):
    max_jobs = 10
    config["db"]["max_jobs"] = max_jobs
    taskq = TaskQ(config=config)
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


def test_run_forever(config):
    taskq = TaskQ(config=config)
    taskq.run()  # no tasks, instant complete

    def run():
        TaskQ(config=[config, {"run": {"run_forever": True}}]).run()

    p = Process(target=run)
    p.start()
    while not p.is_alive():
        time.sleep(0.2)
    time.sleep(2)
    assert p.is_alive(), "run finished with run_forever True"
    p.kill()
    p.join()
