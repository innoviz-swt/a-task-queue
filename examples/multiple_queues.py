import sys
import os
sys.path.append(os.path.dirname(__file__) + '/..')
from ataskq import TaskQ, Task, targs

# create  job to first quee
tr = TaskQ(name='queue1').create_job(overwrite=True)
tr.db_handler.query('jobs')
tr.db_handler.query('jobs_status')

# add tasks
tr.add_tasks([
    Task(entrypoint='ataskq.tasks_utils.hello_world'),
    Task(entrypoint='ataskq.tasks_utils.dummy_args_task', targs=targs(
        'queue1')),
])

# run the tasks
tr.run() # to run in parallel add num_processes=N
