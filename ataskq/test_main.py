from . import TaskQ, Task, targs
from .tasks_utils.write_to_file_tasks import write_to_file
from .__main__ import main


def test_run_job(tmp_path, conn):
    filepath = tmp_path / "file.txt"

    taskq = TaskQ(conn=conn).create_job()

    taskq.add_tasks(
        [
            Task(entrypoint=write_to_file, targs=targs(filepath, "task 0\n")),
            Task(entrypoint=write_to_file, targs=targs(filepath, "task 1\n")),
            Task(entrypoint=write_to_file, targs=targs(filepath, "task 2\n")),
        ]
    )

    args = ["run", "-c", conn, "--job-id", str(taskq.job_id)]
    main(args=args)

    assert filepath.exists()
    assert filepath.read_text() == "task 0\n" "task 1\n" "task 2\n"


from . import TaskQ, Task, targs
from .tasks_utils.write_to_file_tasks import write_to_file
from .__main__ import main


def test_run_level(tmp_path, conn):
    filepath = tmp_path / "file.txt"

    taskq = TaskQ(conn=conn).create_job()

    taskq.add_tasks(
        [
            Task(entrypoint=write_to_file, level=0, targs=targs(filepath, "task 0\n")),
            Task(entrypoint=write_to_file, level=0, targs=targs(filepath, "task 1\n")),
            Task(entrypoint=write_to_file, level=1, targs=targs(filepath, "task 2\n")),
            Task(entrypoint=write_to_file, level=2, targs=targs(filepath, "task 3\n")),
            Task(entrypoint=write_to_file, level=3, targs=targs(filepath, "task 4\n")),
        ]
    )

    # run level 0
    args = ["run", "-c", conn, "--job-id", str(taskq.job_id), "--level", "0"]
    main(args=args)

    assert filepath.exists()
    assert filepath.read_text() == "task 0\n" "task 1\n"

    # run level 1
    args = ["run", "-c", conn, "--job-id", str(taskq.job_id), "--level", "1"]
    main(args=args)

    assert filepath.exists()
    assert filepath.read_text() == "task 0\n" "task 1\n" "task 2\n"

    # run level 2, 3
    args = ["run", "-c", conn, "--job-id", str(taskq.job_id), "--level", "2", "4"]
    main(args=args)

    assert filepath.exists()
    assert filepath.read_text() == "task 0\n" "task 1\n" "task 2\n" "task 3\n" "task 4\n"
