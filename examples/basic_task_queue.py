import context
from ataskq import TaskQ, Task, targs

# create  job
# postgres://{user}:{password}@{hostname}:{port}/{database-name}
tr = TaskQ(db='postgresql://postgres:cvalgo.devops@localhost:5432/ataskq').create_job(overwrite=True)

# add tasks
# entrypoint stands for the relevant function import statement
# (here we use build in demo functions)
tr.add_tasks([
    Task(entrypoint='ataskq.tasks_utils.hello_world'),
    Task(entrypoint='ataskq.tasks_utils.dummy_args_task', targs=targs(
        'arg0', 'arg1', kwarg1=10, kwarg2='this is kwarg2')),
])

# run the tasks
tr.run()  # to run in parallel add num_processes=N
