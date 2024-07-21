import context
from ataskq import TaskQ, Job, Task


def hello_world():
    print("hello world")


def task_with_args(**kwargs):
    print(f"task_with_args kwargs: {kwargs}")


# create  job
tq = TaskQ()
job = Job(
    tasks=[
        Task(entrypoint=hello_world),
        Task(entrypoint=task_with_args, kwargs=dict(arg1=10, arg2="this is kwarg2")),
    ]
)
tq.add(job).run(job)
