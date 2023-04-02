import sys
import os
import logging 
sys.path.append(os.path.dirname(__file__) + '/..')

from ataskq.task_runner import TaskRunner, Task

# init logger
logger = logging.getLogger('ataskq')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(logging.BASIC_FORMAT))
handler.setLevel(os.environ.get("LOGLEVEL", "INFO"))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# create  job
tr = TaskRunner(logger=logger).create_job(overwrite=True)    

def targs(*args, **kwargs):
    return (args, kwargs)

# create following flow
#          / 'run in parallel' \
#  'start' - 'run in parallel' - 'finalize'
#          \ 'run in parallel' /
tr.add_tasks([
    Task(level=1, entrypoint='examples.exmodule.entrypoint_with_args', targs=targs('1:start', level=1)),
    Task(level=2, entrypoint='examples.exmodule.entrypoint_with_args', targs=targs('2:run in parallel:a', level=2, sleep=1)),

    Task(level=2, entrypoint='examples.exmodule.entrypoint_with_args', targs=targs('2:run in parallel:b', level=2, sleep=0)),
    Task(level=2, entrypoint='examples.exmodule.entrypoint_with_args', targs=targs('2:run in parallel:c', level=2, sleep=1)),
    Task(level=2, entrypoint='examples.exmodule.entrypoint_with_args', targs=targs('2:run in parallel:d', level=2, sleep=2)),
    Task(level=2, entrypoint='examples.exmodule.entrypoint_with_args', targs=targs('2:run in parallel:e', level=2, sleep=3)),
    Task(level=2, entrypoint='examples.exmodule.entrypoint_with_args', targs=targs('2:run in parallel:f', level=2, sleep=0)),
    Task(level=2, entrypoint='examples.exmodule.entrypoint_with_args', targs=targs('2:run in parallel:g', level=2, sleep=1)),
    Task(level=2, entrypoint='examples.exmodule.entrypoint_with_args', targs=targs('2:run in parallel:h', level=2, sleep=2)),
    Task(level=2, entrypoint='examples.exmodule.entrypoint_with_args', targs=targs('2:run in parallel:i', level=2, sleep=3)),
    Task(level=2, entrypoint='examples.exmodule.entrypoint_with_args', targs=targs('2:run in parallel:j', level=2, sleep=0)),
    Task(level=2, entrypoint='examples.exmodule.entrypoint_with_args', targs=targs('2:run in parallel:k', level=2, sleep=1)),

    Task(level=3, entrypoint='examples.exmodule.entrypoint_with_args', targs=targs('3:end', level=3)),
])
logger.info('tasks:')
tr.log_tasks()

logger.info('running tasks...')
tr.run_all_multiprocess(5)

# logger.info('tasks:')
# tr.log_tasks()
