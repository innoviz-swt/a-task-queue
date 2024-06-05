import context
from ataskq import TaskQ, Task, targs
from ataskq.tasks_utils import hello_world, dummy_args_task


def hello_world():
    print("hello world")


def task_with_args(*args, **kwargs):
    print(f"task_with_args args: {args}, kwargs: {kwargs}")


# create  job
tr = TaskQ().create_job()

# add tasks (functions to run)
tr.add_tasks(
    [
        Task(entrypoint=hello_world),
        Task(entrypoint=task_with_args, targs=targs("arg0", "arg1", kwarg1=10, kwarg2="this is kwarg2")),
    ]
)

# run the tasks
tr.run()  # to run in parallel add concurrency=N
