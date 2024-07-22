import context
from ataskq import TaskQ, Job, Task, Object


def hello_world():
    print("hello world")


def task_with_args(*args, **kwargs):
    print(f"task_with_args args: {args}, kwargs: {kwargs}")


def task_sum(a, b):
    ret = a + b
    print(f"sum {a} + {b} = {ret}")
    return ret


# create  job
tq = TaskQ()
job = Job(
    tasks=[
        Task(entrypoint=hello_world),
        Task(entrypoint=task_with_args, args=["ataskq", 10], kwargs=dict(arg1=10, arg2="this is kwarg2")),
        Task(entrypoint=task_sum, args=[1, 2.2]),
    ]
)
print("# running tasks")
tq.run(job)
print()

# get tasks results (return values)
tasks = tq.get_all(Task, relationships=["ret"], job_id=job.job_id)
print("# return values")
for t in tasks:
    print(f"task {t} return value: {t.ret and t.ret()}")
