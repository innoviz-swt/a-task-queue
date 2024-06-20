from pathlib import Path

import context
from ataskq import TaskQ, Task, targs
import os
import logging


# init logger
log_level = "INFO"
logger = logging.getLogger("ataskq")
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(logging.BASIC_FORMAT))
handler.setLevel(log_level)
logger.addHandler(handler)
logger.setLevel(log_level)

# create  job
tr = TaskQ(logger=logger).create_job()

# add tasks
tr.add_tasks(
    [
        Task(level=1, entrypoint="ataskq.tasks_utils.hello_world"),
        Task(level=1.1, entrypoint="ataskq.tasks_utils.dummy_args_task", targs=targs("1.1a", level=1.1)),
        Task(level=1.1, entrypoint="ataskq.tasks_utils.dummy_args_task", targs=targs("1.1b", level=1.1)),
        Task(level=2, entrypoint="ataskq.tasks_utils.dummy_args_task", targs=targs("args for entrypoint_with_args")),
    ]
)

logger.info("running tasks...")
tr.run()
