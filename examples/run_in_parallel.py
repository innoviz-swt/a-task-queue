import logging

import context
from ataskq import TaskQ, Task, targs

# init logger
log_level = logging.INFO
logger = logging.getLogger('ataskq')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(logging.BASIC_FORMAT))
logger.addHandler(handler)
handler.setLevel(log_level)
logger.setLevel(log_level)

# create  job
tr = TaskQ(logger=logger).create_job(overwrite=True)

# create following flow
#          / 'run in parallel' \
#  'start' - 'run in parallel' - 'end'
#          \  ...              /
tr.add_tasks([
    Task(level=1, entrypoint='ataskq.tasks_utils.dummy_args_task', targs=targs('1:start', level=1)),

    Task(level=2, entrypoint='ataskq.tasks_utils.dummy_args_task', targs=targs('2:run in parallel:a', level=2, sleep=1)),
    Task(level=2, entrypoint='ataskq.tasks_utils.dummy_args_task', targs=targs('2:run in parallel:b', level=2, sleep=0)),
    Task(level=2, entrypoint='ataskq.tasks_utils.dummy_args_task', targs=targs('2:run in parallel:c', level=2, sleep=1)),
    Task(level=2, entrypoint='ataskq.tasks_utils.dummy_args_task', targs=targs('2:run in parallel:d', level=2, sleep=2)),
    Task(level=2, entrypoint='ataskq.tasks_utils.dummy_args_task', targs=targs('2:run in parallel:e', level=2, sleep=3)),
    Task(level=2, entrypoint='ataskq.tasks_utils.dummy_args_task', targs=targs('2:run in parallel:f', level=2, sleep=0)),
    Task(level=2, entrypoint='ataskq.tasks_utils.dummy_args_task', targs=targs('2:run in parallel:g', level=2, sleep=1)),
    Task(level=2, entrypoint='ataskq.tasks_utils.dummy_args_task', targs=targs('2:run in parallel:h', level=2, sleep=2)),
    Task(level=2, entrypoint='ataskq.tasks_utils.dummy_args_task', targs=targs('2:run in parallel:i', level=2, sleep=3)),
    Task(level=2, entrypoint='ataskq.tasks_utils.dummy_args_task', targs=targs('2:run in parallel:j', level=2, sleep=0)),

    Task(level=3, entrypoint='ataskq.tasks_utils.dummy_args_task', targs=targs('3:end', level=3)),
])
# tr.log_tasks()

logger.info('running tasks...')
tr.run(1.0)
