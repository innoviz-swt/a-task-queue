from pathlib import Path
from datetime import datetime, timedelta
from copy import copy
from multiprocessing import Process
import time

import pytest

from . import TaskQ, Job, Task, EStatus
from .handler import DBHandler
from .handler import EAction, from_config

from .tasks_utils import dummy_args_task, write_to_file, skip_run_task, write_to_file_mp_lock, exception_task


def get_taskq(config, tasks, handler=None):
    if not isinstance(tasks, list):
        tasks = [tasks]

    taskq = TaskQ(config=config, handler=handler)
    job = Job(tasks=tasks)
    taskq.add(job)
    return taskq, job


def non_decreasing(L):
    return all(x <= y for x, y in zip(L, L[1:]))


def non_increasing(L):
    return all(x >= y for x, y in zip(L, L[1:]))


def monotonic(L):
    return non_decreasing(L) or non_increasing(L)


def test_create_job(config):
    taskq = TaskQ(config=config).add(Job())
    assert isinstance(taskq, TaskQ)

    conn = config["connection"]
    if "sqlite" in conn:
        assert Path(taskq.handler.connection.path).exists()
        assert Path(taskq.handler.connection.path).is_file()
    elif "pg" in conn:
        pass
    elif "http" in conn:
        pass
    else:
        raise Exception(f"unknown db type in connection string '{conn}'")


def test_update_task_status(config):
    taskq = TaskQ(config=config)
    task = Task(entrypoint=dummy_args_task, level=1, name="task1")
    job = Job(tasks=[task])
    taskq.add(job)

    now = datetime.now()
    # update db with task status (also updates task inplace)
    taskq.update_task_status(task, EStatus.RUNNING, timestamp=now)
    taskq.add(task)
    assert task.status == EStatus.RUNNING
    assert task.pulse_time == now
    assert task.done_time is None

    # get task form db and validate
    taskq.add(task)
    tasks = taskq._handler.get_all(Task)
    assert len(tasks) == 1  # sanity
    task: Task = tasks[0]

    assert task.status == EStatus.RUNNING
    assert task.pulse_time == now
    assert task.done_time is None

    # update db with task status (also updates task inplace)
    taskq.add(task)
    taskq.update_task_status(task, EStatus.SUCCESS, timestamp=now)
    assert task.status == EStatus.SUCCESS
    assert task.pulse_time == now
    assert task.done_time == now

    # get task form db and validate
    taskq.add(task)
    tasks = taskq._handler.get_all(Task)
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
    assert task1.kwargs == task2.kwargs


def test_get_tasks(config):
    taskq = TaskQ(config=config)
    in_task1 = Task(entrypoint=dummy_args_task, level=1, name="task1")
    in_task2 = Task(entrypoint=dummy_args_task, level=2, name="task2")
    in_task3 = Task(entrypoint=dummy_args_task, level=3, name="task3")
    job = Job(
        tasks=[
            in_task1,
            in_task2,
            in_task3,
        ]
    )
    taskq.add(job)
    # todo: change to job refresh
    tasks = taskq.handler.get_all(Task, job_id=job.job_id)
    assert len(tasks) == 3
    for t in tasks:
        if t.level == 1:
            _compare_tasks(in_task1, t)
        elif t.level == 2:
            _compare_tasks(in_task2, t)
        elif t.level == 3:
            _compare_tasks(in_task3, t)


def test_take_next_task_sanity(config):
    in_task1 = Task(entrypoint=dummy_args_task, level=1, name="task1")
    taskq, _ = get_taskq(config, in_task1)
    action, task = taskq._take_next_task(level=None)
    assert action == EAction.RUN_TASK
    _compare_tasks(in_task1, task)


def test_take_next_task(config):
    in_task1 = Task(entrypoint=dummy_args_task, level=1, name="task1")
    in_task2 = Task(entrypoint=dummy_args_task, level=2, name="task2")
    in_task3 = copy(in_task1)
    taskq, _ = get_taskq(
        config,
        [
            in_task2,
            in_task1,
            in_task3,
        ],
    )

    tids = []
    action, task = taskq._take_next_task(level=None)
    assert action == EAction.RUN_TASK
    assert task.task_id not in tids
    tids.append(task.task_id)
    _compare_tasks(in_task1, task)

    action, task = taskq._take_next_task(level=None)
    assert action == EAction.RUN_TASK
    assert task.task_id not in tids
    tids.append(task.task_id)
    _compare_tasks(in_task3, task)  # note in_task3 is copy of in_task1

    action, task = taskq._take_next_task(level=None)
    assert action == EAction.WAIT
    assert task is None


def test_take_next_task_2_jobs(config):
    # todo: test should ne under ataskq
    handler = from_config(config)

    in_task1 = Task(entrypoint=dummy_args_task, level=2, name="taska")
    in_task2 = Task(entrypoint=dummy_args_task, level=1, name="taskb")
    in_task3 = Task(entrypoint=dummy_args_task, level=1, name="taskb")
    in_task4 = Task(entrypoint=dummy_args_task, level=1, name="taskd")
    in_task5 = Task(entrypoint=dummy_args_task, level=2, name="taske")

    _, job1 = get_taskq(
        config,
        [
            in_task2,
            in_task1,
            in_task3,
        ],
        handler=handler,
    )

    taskq, job2 = get_taskq(
        config,
        [
            in_task5,
            in_task4,
        ],
        handler=handler,
    )

    jid1 = job1.job_id
    jid2 = job2.job_id
    tids = []

    # sanity check
    assert len(handler.get_all(Job)) == 2
    assert len(handler.get_all(Task)) == 5
    assert len(handler.get_all(Task, job_id=jid1)) == 3
    assert len(handler.get_all(Task, job_id=jid2)) == 2

    # taskq 1
    action, task = taskq._take_next_task(job_id=jid1)
    assert action == EAction.RUN_TASK
    assert task.task_id not in tids
    tids.append(task.task_id)
    _compare_tasks(in_task2, task, job_id=jid1)
    tids.append(task.task_id)

    action, task = taskq._take_next_task(job_id=jid1)
    assert action == EAction.RUN_TASK
    assert task.task_id not in tids
    tids.append(task.task_id)
    _compare_tasks(in_task3, task, job_id=jid1)  # note in_task3 is copy of in_task1

    action, task = taskq._take_next_task(job_id=jid1)
    assert action == EAction.WAIT
    assert task is None

    # taskq 2
    action, task = taskq._take_next_task(job_id=jid2)
    assert action == EAction.RUN_TASK
    assert task.task_id not in tids
    tids.append(task.task_id)
    _compare_tasks(in_task4, task, job_id=jid2)
    tids.append(task.task_id)

    action, task = taskq._take_next_task(job_id=jid2)
    assert action == EAction.WAIT
    assert task is None


def test_take_next_all_jobs(config):
    handler = from_config(config)

    in_task1 = Task(entrypoint=dummy_args_task, name="job1 - task1")
    in_task2 = Task(entrypoint=dummy_args_task, name="job1 - task2")
    in_task3 = Task(entrypoint=dummy_args_task, name="job1 - task3")
    in_task4 = Task(entrypoint=dummy_args_task, name="job2 - task4")
    in_task5 = Task(entrypoint=dummy_args_task, name="job2 - task5")

    _, job1 = get_taskq(
        config,
        [
            in_task1,
            in_task2,
        ],
        handler=handler,
    )

    taskq, job2 = get_taskq(
        config,
        [
            in_task4,
            in_task5,
        ],
        handler=handler,
    )

    jid1 = job1.job_id
    jid2 = job2.job_id

    # sanity check
    assert len(handler.get_all(Job)) == 2
    assert len(handler.get_all(Task)) == 4
    assert len(handler.get_all(Task, job_id=jid1)) == 2
    assert len(handler.get_all(Task, job_id=jid2)) == 2

    job1.tasks += [in_task3]
    taskq.add(job1)

    # sanity check
    assert len(handler.get_all(Job)) == 2
    assert len(handler.get_all(Task)) == 5
    assert len(handler.get_all(Task, job_id=jid1)) == 3
    assert len(handler.get_all(Task, job_id=jid2)) == 2

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

    taskq, job = get_taskq(
        config,
        [
            Task(entrypoint=write_to_file, args=[filepath, "task 0\n"]),
            Task(entrypoint=write_to_file, args=[filepath, "task 1\n"]),
            Task(entrypoint=write_to_file, args=[filepath, "task 2\n"]),
        ],
    )
    taskq.run(job=job.job_id)

    assert filepath.exists()
    assert filepath.read_text() == "task 0\n" "task 1\n" "task 2\n"


def test_run_task_raise_exception(config):
    # no exception raised
    try:
        config["run"]["raise_exception"] = False
        taskq, job = get_taskq(
            config,
            [
                Task(entrypoint=exception_task, kwargs=dict(message="task failed")),
            ],
        )
        taskq.run(job)
    except Exception:
        assert False, "exception_task raises exception with run_task_raise_exception=False"

    # exception raised
    config["run"]["raise_exception"] = True
    taskq, job = get_taskq(
        config,
        [
            Task(entrypoint=exception_task, kwargs=dict(message="task failed")),
        ],
    )
    with pytest.raises(Exception) as excinfo:
        taskq.run(job)
    assert excinfo.value.args[0] == "task failed"


def test_run_2_processes(config, tmp_path: Path):
    filepath = tmp_path / "file.txt"

    taskq, _ = get_taskq(
        config,
        [
            Task(
                entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file_mp_lock",
                args=(filepath, "task 0\n"),
            ),
            Task(
                entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file_mp_lock",
                args=(filepath, "task 1\n"),
            ),
            Task(
                entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file_mp_lock",
                args=(filepath, "task 2\n"),
            ),
        ],
    )

    taskq.run(concurrency=2)

    assert filepath.exists()
    text = filepath.read_text()
    assert "task 0\n" in text
    assert "task 1\n" in text
    assert "task 1\n" in text


def test_run_all_jobs(config, tmp_path: Path):
    handler = from_config(config)
    filepath1 = tmp_path / "file1.txt"
    handler.add(
        Job(
            tasks=[
                Task(
                    entrypoint=write_to_file,
                    args=(filepath1, "task 0\n"),
                ),
            ],
        )
    )

    filepath2 = tmp_path / "file2.txt"
    handler.add(
        Job(
            tasks=[
                Task(
                    entrypoint=write_to_file,
                    args=(filepath2, "task 1\n"),
                ),
            ],
        )
    )

    TaskQ(config=config).run()

    assert filepath1.exists()
    assert "task 0\n" == filepath1.read_text()
    assert filepath2.exists()
    assert "task 1\n" == filepath2.read_text()


@pytest.mark.parametrize("num_processes", [None, 2])
def test_run_by_level(config, tmp_path: Path, num_processes: int):
    filepath = tmp_path / "file.txt"

    taskq, job = get_taskq(
        config,
        [
            Task(
                level=0,
                entrypoint=write_to_file_mp_lock,
                args=(filepath, "task 0\n"),
            ),
            Task(
                level=1,
                entrypoint=write_to_file_mp_lock,
                args=(filepath, "task 1\n"),
            ),
            Task(
                level=1,
                entrypoint=write_to_file_mp_lock,
                args=(filepath, "task 2\n"),
            ),
            Task(
                level=2,
                entrypoint=write_to_file_mp_lock,
                args=(filepath, "task 3\n"),
            ),
        ],
    )

    assert taskq.count_pending_tasks_below_level(job, 3) == 4
    assert taskq.count_pending_tasks_below_level(job, 1) == 1

    taskq.run(job, level=0, concurrency=num_processes)
    taskq.count_pending_tasks_below_level(job, 1) == 0
    assert filepath.exists()
    text = filepath.read_text()
    assert "task 0\n" in text
    assert taskq.count_pending_tasks_below_level(job, 2) == 2

    taskq.run(job, level=1, concurrency=num_processes)
    taskq.count_pending_tasks_below_level(job, 2) == 0
    text = filepath.read_text()
    assert "task 0\n" in text
    assert "task 1\n" in text
    assert "task 2\n" in text

    assert taskq.count_pending_tasks_below_level(job, 3) == 1
    taskq.run(job, level=2, concurrency=num_processes)
    taskq.count_pending_tasks_below_level(job, 3) == 0
    text = filepath.read_text()
    assert "task 0\n" in text
    assert "task 1\n" in text
    assert "task 2\n" in text
    assert "task 3\n" in text


def test_monitor_pulse_failure(config):
    # set monitor pulse longer than timeout
    config["monitor"]["pulse_interval"] = 2
    config["monitor"]["pulse_timeout"] = 1.5
    taskq, job = get_taskq(
        config,
        [
            # reserved keyward for ignored task for testing
            Task(entrypoint=skip_run_task, args=("task will fail",)),
            Task(entrypoint=dummy_args_task, args=("task will success",)),
        ],
    )
    start = datetime.now()
    taskq.run(job)
    stop = datetime.now()

    tasks = taskq.handler.get_all(Task, job_id=job.job_id)

    assert tasks[0].status == EStatus.FAILURE
    assert tasks[1].status == EStatus.SUCCESS
    assert stop - start > timedelta(seconds=1.5)


# def test_task_wait_timeout(config):
#     # set monitor pulse longer than timeout
#     config["run"]["raise_exception"] = True
#     config["run"]["wait_timeout"] = 0
#     taskq = TaskQ(config=config).create_job()
#     taskq.add_tasks(
#         [
#             Task(entrypoint=dummy_args_task, level=1, targs=targs("task will success", sleep=0.2)),
#             Task(entrypoint=dummy_args_task, level=2, targs=targs("task will success")),
#         ]
#     )

#     with pytest.raises(Exception) as excinfo:
#         taskq.run(concurrency=2)
#     assert excinfo.value.args[0] == "Some processes failed, see logs for details"


def test_max_jobs(config):
    max_jobs = 10
    config["db"]["max_jobs"] = max_jobs
    taskq, _ = get_taskq(config, [])
    if not isinstance(taskq.handler, DBHandler):
        pytest.skip()

    jobs_id = []
    for i in range(max_jobs * 2):
        taskq, job = get_taskq(config, [])
        job.name = f"job {i}"
        jobs_id.append(job.job_id)
        taskq.add(job)
        taskq.run(job)  # deletes job on run start
    jobs = taskq.handler.get_all(Job)
    assert len(jobs) == 10
    assert len(set(jobs_id)) == max_jobs * 2

    remaining_jobs = [j.job_id for j in jobs]
    assert remaining_jobs == jobs_id[-max_jobs:]


def test_run_forever(config):
    def run():
        TaskQ(config=[config, {"run": {"run_forever": True}}]).run(Job())

    p = Process(target=run)
    p.start()
    while not p.is_alive():
        time.sleep(0.2)
    time.sleep(2)
    assert p.is_alive(), "run finished with run_forever True"
    p.kill()
    p.join()
