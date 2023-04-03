import sys
import os
import logging 
sys.path.append(os.path.dirname(__file__) + '/..')

from ataskq.task_runner import TaskRunner, Task

# init logger
log_level = logging.INFO
logger = logging.getLogger('ataskq')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(logging.BASIC_FORMAT))
logger.addHandler(handler)
handler.setLevel(log_level)
logger.setLevel(log_level)

# create  job
tr = TaskRunner(logger=logger).create_job(overwrite=True)    

def targs(*args, **kwargs):
    return (args, kwargs)

# create following flow
#          / 'run in parallel' \
#  'start' - 'run in parallel' - 'end'
#          \  ...              /
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

    Task(level=3, entrypoint='examples.exmodule.entrypoint_with_args', targs=targs('3:end', level=3)),
])
# tr.log_tasks()

logger.info('running tasks...')
tr.run_all_multiprocess(1.0)
