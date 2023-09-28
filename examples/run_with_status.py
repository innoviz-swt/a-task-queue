import sys
import os
import time
import webbrowser
from pathlib import Path

sys.path.append(os.path.dirname(__file__) + '/..')
from ataskq import TaskQ, Task, targs
from ataskq.db_handler import EQueryType
from ataskq.server import run_server

from common import init_logger

# create  job
logger = init_logger()
port = 8900

tr = TaskQ(monitor_pulse_interval=1, logger=logger)
# set monitor update interval to 1 second for example to show monitor pulse update
tr.create_job(overwrite=True) 
run_server(tr.db_handler, port=port, background=True, popup='tasks_status', print_logs=False)

# add tasks
tr.add_tasks([
    Task(level=1, name='hello task', entrypoint='ataskq.tasks_utils.hello_world'),
    Task(level=1.1, name="sleep task", entrypoint='ataskq.tasks_utils.dummy_args_task', targs=targs('1.1a', level=1.1, sleep=10)),
    Task(level=1.1, name="sleep task", entrypoint='ataskq.tasks_utils.dummy_args_task', targs=targs('1.1b', level=1.1, sleep=5)),
    Task(level=1.1, name="failure task", entrypoint='ataskq.tasks_utils.exception_task'),
    Task(level=2, entrypoint='ataskq.tasks_utils.dummy_args_task', targs=targs('args for entrypoint_with_args')),
])

logger.info('running tasks...')
tr.run(num_processes=3)

time.sleep(5)
# once process is down, "opened status browser window will be replaced by 'site can't be reached' window"
