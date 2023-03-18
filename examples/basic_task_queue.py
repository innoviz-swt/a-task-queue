import sys
import os
sys.path.append(os.path.dirname(__file__) + '/..')

from ataskq.task_runner import EStatus, TaskRunner, Task

# create  job
tr = TaskRunner().create_job(overwrite=True)    

def targs(*args, **kwargs):
    return (args, kwargs)

# add tasks
tr.add_tasks([
    Task(level=1, entrypoint='examples.exmodule.hello_world'),
    Task(level=2, entrypoint='examples.exmodule.entrypoint_with_args', targs=targs('args for entrypoint_with_args')),

])
tr.get_tasks()
tr.run()