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
tr = TaskRunner(logger=logger, job_path=f"./ataskqjob-{os.getpid()}").create_job(overwrite=True)    

def targs(*args, **kwargs):
    return (args, kwargs)

# add tasks
tr.add_tasks([
    Task(level=1, entrypoint='examples.exmodule.hello_world'),
    Task(level=1.1, entrypoint='examples.exmodule.entrypoint_with_args', targs=targs('1.1a', level=1.1)),
    Task(level=1.1, entrypoint='examples.exmodule.entrypoint_with_args', targs=targs('1.1b', level=1.1)),
    Task(level=2, entrypoint='examples.exmodule.entrypoint_with_args', targs=targs('args for entrypoint_with_args')),
])
logger.info('tasks:')
tr.log_tasks()

logger.info('running tasks...')
tr.run_all_sequential()

logger.info('tasks:')
tr.log_tasks()
