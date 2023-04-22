import sys
import os
import time
import webbrowser

sys.path.append(os.path.dirname(__file__) + '/..')
from ataskq.task_runner import TaskRunner, Task, targs
from ataskq.simple_summary_server import run_server

from common import init_logger

# create  job
logger = init_logger()
tr = TaskRunner(logger=logger).create_job(overwrite=True) 
run_server(tr, background=True)   
webbrowser.open('http://localhost:8000?auto_refresh=true')
webbrowser.open('http://localhost:8000/num_units_summary?auto_refresh=true')

# add tasks
tr.add_tasks([
    Task(level=1, name='hello task', entrypoint='ataskq.tasks_utils.hello_world', num_units=1),
    Task(level=1.1, name="sleep task", entrypoint='ataskq.tasks_utils.dummy_args_task', targs=targs('1.1a', level=1.1, sleep=10), num_units=10),
    Task(level=1.1, name="sleep task", entrypoint='ataskq.tasks_utils.dummy_args_task', targs=targs('1.1b', level=1.1, sleep=5), num_units=5),
    Task(level=1.1, name="failure task", entrypoint='ataskq.tasks_utils.exception_task', num_units=1),
    Task(level=2, entrypoint='ataskq.tasks_utils.dummy_args_task', targs=targs('args for entrypoint_with_args'), num_units=2),
])

logger.info('running tasks...')
tr.run()

print(tr.tasks_summary())
tr.summary_html(file=tr.job_path / 'summary.html')

time.sleep(5)
# once process is down, "opened status browser window will be replaced by 'site can't be reached' window"
