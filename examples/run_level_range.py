import sys
import os
import logging 
sys.path.append(os.path.dirname(__file__) + '/..')

from ataskq.task import Task
from ataskq.runner import TaskRunner, targs


# init logger
logger = logging.getLogger('ataskq')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(logging.BASIC_FORMAT))
handler.setLevel(os.environ.get("LOGLEVEL", "INFO"))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# create  job
tr = TaskRunner(logger=logger).create_job(overwrite=True)

# add tasks
tr.add_tasks([
    Task(level=1, entrypoint='ataskq.tasks_utils.hello_world'),
    Task(level=2, entrypoint='ataskq.tasks_utils.dummy_args_task', targs=targs('2a', level=2)),
    Task(level=2, entrypoint='ataskq.tasks_utils.dummy_args_task', targs=targs('2b', level=2)),
    Task(level=3, entrypoint='ataskq.tasks_utils.dummy_args_task', targs=targs('args for entrypoint_with_args')),
])
tr.log_tasks()
logger.info('-' * 80)

for i in range(1, 4):
    logger.info(f'running tasks level {i}...')
    tr.run(level=i)
    logger.info('-' * 80)

tr.log_tasks()
logger.info('-' * 80)
