from pathlib import Path

from ataskq.runner import TaskRunner, targs
from ataskq.task import Task


def test_create_job(tmp_path: Path):
    runner = TaskRunner(job_path=tmp_path).create_job(overwrite=True)
    assert isinstance(runner, TaskRunner)

    assert tmp_path.exists()
    assert (tmp_path / '.ataskqjob').exists()

    assert (tmp_path / 'tasks.sqlite.db').exists()
    assert (tmp_path / 'tasks.sqlite.db').is_file()


def test_run_default(tmp_path: Path):
    job_path = tmp_path / 'ataskq'
    filepath = tmp_path / 'file.txt'

    runner = TaskRunner(job_path=job_path).create_job(overwrite=True)

    runner.add_tasks([
        Task(entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file",
             targs=targs(filepath, 'task 0\n')),
        Task(entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file",
             targs=targs(filepath, 'task 1\n')),
        Task(entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file",
             targs=targs(filepath, 'task 2\n')),
    ])

    runner.run()

    assert filepath.exists()
    assert filepath.read_text() == \
        "task 0\n" \
        "task 1\n" \
        "task 2\n"


def test_run_2_processes(tmp_path: Path):
    job_path = tmp_path / 'ataskq'
    filepath = tmp_path / 'file.txt'

    runner = TaskRunner(job_path=job_path).create_job(overwrite=True)

    runner.add_tasks([
        Task(entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file_mp_lock",
             targs=targs(filepath, 'task 0\n')),
        Task(entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file_mp_lock",
             targs=targs(filepath, 'task 1\n')),
        Task(entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file_mp_lock",
             targs=targs(filepath, 'task 2\n')),
    ])

    runner.run(num_processes=2)

    assert filepath.exists()
    text = filepath.read_text()
    assert "task 0\n" in text
    assert "task 1\n" in text
    assert "task 1\n" in text


def _test_run_by_level(tmp_path: Path, num_processes: int):
    job_path = tmp_path / 'ataskq'
    filepath = tmp_path / 'file.txt'

    runner = TaskRunner(job_path=job_path).create_job(overwrite=True)

    runner.add_tasks([
        Task(level=0, entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file_mp_lock",
             targs=targs(filepath, 'task 0\n')),
        Task(level=1, entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file_mp_lock",
             targs=targs(filepath, 'task 1\n')),
        Task(level=1, entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file_mp_lock",
             targs=targs(filepath, 'task 2\n')),
        Task(level=2, entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file_mp_lock",
             targs=targs(filepath, 'task 3\n')),
    ])

    assert runner.count_pending_tasks_below_level(3) == 4

    assert runner.count_pending_tasks_below_level(1) == 1
    runner.run(level=0, num_processes=num_processes)
    runner.count_pending_tasks_below_level(1) == 0
    assert filepath.exists()
    text = filepath.read_text()
    assert "task 0\n" in text

    assert runner.count_pending_tasks_below_level(2) == 2
    runner.run(level=1, num_processes=num_processes)
    runner.count_pending_tasks_below_level(2) == 0
    text = filepath.read_text()
    assert "task 0\n" in text
    assert "task 1\n" in text
    assert "task 2\n" in text

    assert runner.count_pending_tasks_below_level(3) == 1
    runner.run(level=2, num_processes=num_processes)
    runner.count_pending_tasks_below_level(3) == 0
    text = filepath.read_text()
    assert "task 0\n" in text
    assert "task 1\n" in text
    assert "task 2\n" in text
    assert "task 3\n" in text


def test_run_by_level(tmp_path: Path):
    _test_run_by_level(tmp_path, num_processes=None)


def test_run_by_level_2_processes(tmp_path: Path):
    _test_run_by_level(tmp_path, num_processes=2)
