import context
from ataskq import TaskQ, Task, targs
from ataskq.tasks_utils import hello_world, dummy_args_task

# create  job
tr = TaskQ().create_job()

# add tasks
# entrypoint stands for the relevant function import statement
# (here we use build in demo functions)
tr.add_tasks(
    [
        Task(entrypoint=hello_world),
        Task(entrypoint=dummy_args_task, targs=targs("arg0", "arg1", kwarg1=10, kwarg2="this is kwarg2")),
    ]
)

# run the tasks
tr.run()  # to run in parallel add num_processes=N
