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
with TaskRunner(logger=logger) as tr:
    tr.create_job()

    # add tasks
    tr.add_tasks([
        Task(level=1, entrypoint='ataskq.tasks_utils.hello_world'),
        Task(level=1.1, entrypoint='ataskq.tasks_utils.dummy_args_task', targs=targs('1.1a', level=1.1)),
        Task(level=1.1, entrypoint='ataskq.tasks_utils.dummy_args_task', targs=targs('1.1b', level=1.1)),
        Task(level=2, entrypoint='ataskq.tasks_utils.dummy_args_task', targs=targs('args for entrypoint_with_args')),
    ])
    logger.info('tasks:')
    tr.log_tasks()

    logger.info('running tasks...')
    tr.run()

    logger.info('tasks:')
    tr.log_tasks()
