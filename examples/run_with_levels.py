import logging
from pathlib import Path
from pathlib import Path

import os

import context
from ataskq import TaskQ, Task, Job


# init logger
logger = logging.getLogger("ataskq")
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(logging.BASIC_FORMAT))
handler.setLevel(os.environ.get("LOGLEVEL", "INFO"))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# create  job
job = Job(
    name="levels example",
    tasks=[
        Task(level=1, entrypoint="ataskq.tasks_utils.dummy_args_task", kwargs=dict(name="1", level=1)),
        Task(level=2, entrypoint="ataskq.tasks_utils.dummy_args_task", kwargs=dict(name="2a", level=2)),
        Task(level=2, entrypoint="ataskq.tasks_utils.dummy_args_task", kwargs=dict(name="2b", level=2)),
        Task(level=3, entrypoint="ataskq.tasks_utils.dummy_args_task", kwargs=dict(name="3", level=3)),
    ],
)
tr = TaskQ(logger=logger)

for i in range(1, 4):
    logger.info(f"running tasks level {i}...")
    tr.run(job, level=i)
    logger.info("-" * 60)
