import sys
import os
sys.path.append(os.path.dirname(__file__) + '/..')

from ataskq.task_runner import EStatus, TaskRunner, Task

# create  job
tr = TaskRunner().create_job(overwrite=True)    

# add tasks
tr.add_tasks([
    Task(level=1, entrypoint='examples.exmodule.hello_world', status=EStatus.CREATED.value),
    Task(level=2, entrypoint='examples.exmodule.entrypoint_with_args', status=EStatus.CREATED.value),

])
tr.get_tasks()
tr.run()