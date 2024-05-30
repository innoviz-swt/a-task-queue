from pathlib import Path

import context
from ataskq import TaskQ, Task, targs

# create  job
tr = TaskQ().create_job(name=Path(__file__).stem)

# add tasks
tr.add_tasks(
    [
        Task(name="hello task", level=1, entrypoint="ataskq.tasks_utils.hello_world"),
        Task(
            name="dummy task",
            level=1.1,
            entrypoint="ataskq.tasks_utils.dummy_args_task",
            targs=targs("1.1a", level=1.1),
        ),
        Task(
            name="dummy task",
            level=1.1,
            entrypoint="ataskq.tasks_utils.dummy_args_task",
            targs=targs("1.1a", level=1.1),
        ),
        Task(name="exception task", level=1.1, entrypoint="ataskq.tasks_utils.exception_task"),
    ]
)

tr.run()
