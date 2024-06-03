import logging
from pathlib import Path
from pathlib import Path

import os

import context
from ataskq import TaskQ, Task, targs


# init logger
logger = logging.getLogger("ataskq")
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(logging.BASIC_FORMAT))
handler.setLevel(os.environ.get("LOGLEVEL", "INFO"))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# create  job
tr = TaskQ(logger=logger).create_job(name=Path(__file__).stem)

# add tasks
tr.add_tasks(
    [
        Task(level=1, entrypoint="ataskq.tasks_utils.hello_world"),
        Task(level=2, entrypoint="ataskq.tasks_utils.dummy_args_task", targs=targs("2a", level=2)),
        Task(level=2, entrypoint="ataskq.tasks_utils.dummy_args_task", targs=targs("2b", level=2)),
        Task(level=3, entrypoint="ataskq.tasks_utils.dummy_args_task", targs=targs("args for entrypoint_with_args")),
    ]
)

for i in range(1, 4):
    logger.info(f"running tasks level {i}...")
    tr.run(level=i)
    logger.info("-" * 80)
