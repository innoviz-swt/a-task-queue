from pathlib import Path

import context
from ataskq.tasks_utils.counter_task import counter_kwarg, counter_task
from ataskq import TaskQ, Task, targs

taskq = TaskQ(run_task_raise_exception=True).create_job(name=Path(__file__).stem)

taskq.add_tasks(
    [
        Task(entrypoint=counter_task, targs=targs(print_counter=True)),
        Task(entrypoint=counter_task, targs=targs(print_counter=True)),
        Task(entrypoint=counter_task, targs=targs(print_counter=True)),
    ]
)


taskq.run()
