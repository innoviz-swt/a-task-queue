import context
from ataskq import TaskQ, Job, Task


def hello_world():
    print("hello world")


def task_with_args(**kwargs):
    print(f"task_with_args kwargs: {kwargs}")


# create  job
tq = TaskQ()
job = Job().screate(_handler=tq.handler)
job.add_tasks(
    [
        Task(entrypoint=hello_world),
        Task(entrypoint=task_with_args, kwargs=dict(arg1=10, arg2="this is kwarg2")),
    ],
    _handler=tq.handler,
)
TaskQ().run(job)
