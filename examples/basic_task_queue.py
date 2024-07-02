import context
from ataskq import TaskQ, Task
from ataskq.object import pickle_dict as pdict


def hello_world():
    print("hello world")


def task_with_args(**kwargs):
    print(f"task_with_args kwargs: {kwargs}")


# create  job
tr = TaskQ().create_job()

# add tasks (functions to run)
tr.add_tasks(
    [
        Task(entrypoint=hello_world),
        Task(entrypoint=task_with_args, kwargs=pdict(arg1=10, arg2="this is kwarg2")),
    ]
)

# run the tasks
tr.run()  # to run in parallel add concurrency=N
