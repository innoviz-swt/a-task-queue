from pathlib import Path

import context
from ataskq import TaskQ, Task, targs

# create  job to first quee
tr = TaskQ().create_job(name=Path(__file__).stem)

# add tasks
tr.add_tasks(
    [
        Task(entrypoint="ataskq.tasks_utils.hello_world"),
        Task(entrypoint="ataskq.tasks_utils.dummy_args_task", targs=targs("queue1")),
    ]
)

# run the tasks
tr.run()
