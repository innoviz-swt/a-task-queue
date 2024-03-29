import logging

import context
from ataskq import TaskQ, Task, targs

# init logger
logger = logging.getLogger('ataskq')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(logging.BASIC_FORMAT))
handler.setLevel(os.environ.get("LOGLEVEL", "INFO"))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# create  job
tr = TaskQ(logger=logger).create_job()

# add tasks
tr.add_tasks([
    Task(level=1, entrypoint='ataskq.tasks_utils.hello_world'),
    Task(level=1.1, entrypoint='ataskq.tasks_utils.dummy_args_task', targs=targs('1.1a', level=1.1)),
    Task(level=1.1, entrypoint='ataskq.tasks_utils.dummy_args_task', targs=targs('1.1a', level=1.1)),
    Task(level=1.1, entrypoint='ataskq.tasks_utils.exception_task'),
])
logger.info('tasks:')
tr.log_tasks()

logger.info('running tasks...')
tr.run()

logger.info('tasks:')
tr.log_tasks()
