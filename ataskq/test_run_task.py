from pathlib import Path

from ataskq.task_runner import TaskRunner, Task, targs

def test_create_job(tmp_path: Path):
    runner = TaskRunner(job_path=tmp_path).create_job(overwrite=True)
    assert isinstance(runner, TaskRunner)

    assert tmp_path.exists()
    assert (tmp_path / '.ataskqjob').exists()

    assert (tmp_path / 'tasks.db').exists()
    assert (tmp_path / 'tasks.db').is_file()

    # dbm db
    assert (tmp_path / 'keyvalue.dir').exists()
    assert (tmp_path / 'keyvalue.dat').exists()


def test_run_default(tmp_path: Path):
    job_path = tmp_path / 'ataskq'
    filepath = tmp_path / 'file.txt'

    runner = TaskRunner(job_path=job_path).create_job(overwrite=True)

    runner.add_tasks([
        Task(entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file", targs=targs(tmp_path / 'file.txt', 'task 0\n')),
        Task(entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file", targs=targs(tmp_path / 'file.txt', 'task 1\n')),
        Task(entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file", targs=targs(tmp_path / 'file.txt', 'task 2\n')),
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
        Task(entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file_mp_lock", targs=targs(tmp_path / 'file.txt', 'task 0\n')),
        Task(entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file_mp_lock", targs=targs(tmp_path / 'file.txt', 'task 1\n')),
        Task(entrypoint="ataskq.tasks_utils.write_to_file_tasks.write_to_file_mp_lock", targs=targs(tmp_path / 'file.txt', 'task 2\n')),
    ])

    runner.run(num_processes=2)

    assert filepath.exists()
    text = filepath.read_text()
    assert  "task 0\n" in text 
    assert  "task 1\n" in text 
    assert  "task 1\n" in text 

