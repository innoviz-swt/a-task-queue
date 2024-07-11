from pathlib import Path

import context
from ataskq import TaskQ, Task


# create  job
tr = TaskQ().create_job(name=Path(__file__).stem)

# create following flow.
#  level 1   level 2             level 3
#          / 'run in parallel' \
#  'start' - 'run in parallel' - 'end'
#          \  ...              /
# Note: in general each level can contain multiple tasks, and the next level start only after all tasks in previous finishes
tr.add_tasks(
    [
        Task(level=1, entrypoint="ataskq.tasks_utils.dummy_args_task", kwargs=dict(bane="1:start", level=1)),
        Task(
            level=2,
            entrypoint="ataskq.tasks_utils.dummy_args_task",
            kwargs=dict(name="2:run in parallel:a", level=2, sleep=1),
        ),
        Task(
            level=2,
            entrypoint="ataskq.tasks_utils.dummy_args_task",
            kwargs=dict(name="2:run in parallel:b", level=2, sleep=0),
        ),
        Task(
            level=2,
            entrypoint="ataskq.tasks_utils.dummy_args_task",
            kwargs=dict(name="2:run in parallel:c", level=2, sleep=1),
        ),
        Task(
            level=2,
            entrypoint="ataskq.tasks_utils.dummy_args_task",
            kwargs=dict(name="2:run in parallel:d", level=2, sleep=2),
        ),
        Task(
            level=2,
            entrypoint="ataskq.tasks_utils.dummy_args_task",
            kwargs=dict(name="2:run in parallel:e", level=2, sleep=3),
        ),
        Task(
            level=2,
            entrypoint="ataskq.tasks_utils.dummy_args_task",
            kwargs=dict(name="2:run in parallel:f", level=2, sleep=0),
        ),
        Task(
            level=2,
            entrypoint="ataskq.tasks_utils.dummy_args_task",
            kwargs=dict(name="2:run in parallel:g", level=2, sleep=1),
        ),
        Task(
            level=2,
            entrypoint="ataskq.tasks_utils.dummy_args_task",
            kwargs=dict(name="2:run in parallel:h", level=2, sleep=2),
        ),
        Task(
            level=2,
            entrypoint="ataskq.tasks_utils.dummy_args_task",
            kwargs=dict(name="2:run in parallel:i", level=2, sleep=3),
        ),
        Task(
            level=2,
            entrypoint="ataskq.tasks_utils.dummy_args_task",
            kwargs=dict(name="2:run in parallel:j", level=2, sleep=0),
        ),
        Task(level=3, entrypoint="ataskq.tasks_utils.dummy_args_task", kwargs=dict(name="3:end", level=3)),
    ]
)

tr.run(1.0)
