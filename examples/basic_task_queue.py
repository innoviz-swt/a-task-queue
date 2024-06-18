import context
from ataskq import TaskQ, Task, Object
from ataskq.tasks_utils import hello_world, dummy_args_task


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
        Task(entrypoint=task_with_args, kwargs_oid=tr.create_object(dict(arg1=10, arg2="this is kwarg2")).object_id),
    ]
)

# run the tasks
tr.run()  # to run in parallel add concurrency=N
