import multiprocessing
from typing import Union, List
import logging
from multiprocessing import Process
import time
from datetime import datetime


from .logger import Logger
from .models import EStatus, Job, Task, Object
from .monitor import MonitorThread
from .handler import Handler, DBHandler, from_config, EAction
from .config import load_config
from .utils.dynamic_import import import_callable


class TaskQ(Logger):
    def __init__(
        self,
        handler: Handler = None,
        logger: Union[str, logging.Logger, None] = None,
        config=None,
    ) -> None:
        super().__init__(logger)

        # init config
        self._config = load_config(config)

        # init db handler
        # todo: handler shouldn't get job_id
        if handler is None:
            self._handler: Handler = from_config(
                config=config,
                logger=self._logger,
            )
        else:
            self._handler = handler

        self._running = False

    @property
    def config(self):
        return self._config

    @property
    def handler(self):
        return self._handler

    def add(self, data):
        self._handler.add(data)

        return self

    def update_task_start_time(self, task: Task, start_time: datetime = None):
        if start_time is None:
            start_time = datetime.now()

        task.start_time = start_time
        self._handler.add(task)

    def update_task_status(self, task: Task, status: EStatus, timestamp: datetime = None):
        if timestamp is None:
            timestamp = datetime.now()

        if status == EStatus.RUNNING:
            # for running task update pulse_time
            task.status = status
            task.pulse_time = timestamp
        elif status == EStatus.SUCCESS or status == EStatus.FAILURE:
            # for done task update pulse_time and done_time time as well
            task.status = status
            task.pulse_time = timestamp
            task.done_time = timestamp
        else:
            raise RuntimeError(f"Unsupported status '{status}' for status update")

        self._handler.add(task)

    def count_pending_tasks_below_level(self, level):
        ret = Task.count_all(
            where=f"job_id = {self.job_id} AND level < {level} AND status in ('{EStatus.PENDING}')",
            _handler=self._handler,
        )
        return ret

    def oid(self, obj, serializer="pickle.dumps", desrializer="pickle.loads") -> int:
        """return object id if isinstance(obj, Object)
        otherwise creates new object in db and returns its id

        Returns:
            int: Object id
        """
        if isinstance(obj, Object):
            return obj.object_id

        obj = self.create_object(obj=obj, serializer=serializer, _desrializer=desrializer)

        return obj.object_id

    def _run_task(self, task: Task):
        self.info(f"Running task '{task}'")

        # get entry point func to execute
        func = import_callable(task.entrypoint)

        # get task kwargs
        task_kwargs = dict()
        if task.kwargs_id is not None:
            try:
                task_kwargs_obj = self._handler.get(Object, task.kwargs_id)
                task_kwargs = task_kwargs_obj.deserialize()
            except Exception as ex:
                msg = f"Getting tasks '{task}' kwargs failed."
                if self.config["run"]["raise_exception"]:  # for debug purposes only
                    self.warning(msg)
                    self.update_task_status(task, EStatus.FAILURE)
                    raise ex

                self.warning(msg, exc_info=True)
                self.update_task_status(task, EStatus.FAILURE)

                return

        # update task start time
        self.update_task_start_time(task)

        # run task
        monitor = MonitorThread(task, self, pulse_interval=self.config["monitor"]["pulse_interval"])
        monitor.start()

        try:
            func(**task_kwargs)
            status = EStatus.SUCCESS
        except Exception as ex:
            msg = f"Running task '{task}' failed with exception."
            if self.config["run"]["raise_exception"]:  # for debug purposes only
                self.warning(msg)
                self.update_task_status(task, EStatus.FAILURE)
                monitor.stop()
                monitor.join()
                raise ex

            self.warning(msg, exc_info=True)
            status = EStatus.FAILURE

        monitor.stop()
        monitor.join()
        self.update_task_status(task, status)

    def _take_next_task(self, job_id=None, level=None):
        level_start = level.start if level is not None else None
        level_stop = level.stop if level is not None else None
        ret = self._handler.take_next_task(job_id=job_id, level_start=level_start, level_stop=level_stop)

        return ret

    def _run(self, job_id, level):
        self.info(f"Started task pulling loop.")

        # check for error code
        task_pull_start = time.time()
        while True:
            # if the taskq handler is db handler, the taskq performs background tasks before each run
            if self.config["run"]["fail_pulse_timeout"] and isinstance(self._handler, DBHandler):
                self._handler.fail_pulse_timeout_tasks(self.config["monitor"]["pulse_timeout"])
            # grab tasks and set them in Q
            action, task = self._take_next_task(job_id, level)

            # handle no task available
            if not self.config["run"]["run_forever"] and action == EAction.STOP:
                break
            if action == EAction.RUN_TASK:
                self._run_task(task)
            elif action == EAction.WAIT or action == EAction.STOP:
                if (
                    wait_timeout := self.config["run"]["wait_timeout"]
                ) is not None and time.time() - task_pull_start > wait_timeout:
                    raise Exception(f"task pull timeout of '{wait_timeout}' sec reached.")

                self.info(f'Task pulling loop - waiting for {self.config["run"]["pull_interval"]} sec')
                time.sleep(self.config["run"]["pull_interval"])
            else:
                raise Exception(f"Unsupported action {action}")

    def assert_level(self, level):
        if isinstance(level, int):
            level = range(level, level + 1)
        elif isinstance(level, (list, tuple)):
            assert len(level) == 2, "level of type list or tuple must have length of 2"
            level = range(level[0], level[1])
        else:
            assert isinstance(level, range), "level must be int, list, tuple or range"

        # check all task < level.start are done
        count = self.count_pending_tasks_below_level(level.start)
        assert count == 0, f"all tasks below level must be done before running tasks at levels {level}"

        return level

    def run(self, job: Job, concurrency=None, level=None):
        if self.config["db"]["max_jobs"] is not None:
            # keep max jbos
            self._handler.delete_all(
                Job,
                where=f"job_id NOT IN (SELECT job_id FROM jobs ORDER BY job_id DESC limit {self.config['db']['max_jobs']})",
            )

        self.info(f"Start running with connection {self.config['connection']}")
        if level is not None:
            level = self.assert_level(level)

        self._running = True

        # default to run in current process
        if concurrency is None:
            self._run(job.job_id, level)
            self._running = False
            return

        assert isinstance(concurrency, (int, float))

        if isinstance(concurrency, float):
            assert 0.0 <= concurrency <= 1.0
            nprocesses = int(multiprocessing.cpu_count() * concurrency)
        elif concurrency < 0:
            nprocesses = multiprocessing.cpu_count() - concurrency
        else:
            nprocesses = concurrency

        # set processes and Q
        processes = [
            Process(
                target=self._run,
                args=(
                    job.job_id,
                    level,
                ),
            )
            for i in range(nprocesses)
        ]
        [p.start() for p in processes]

        # join all processes
        [p.join() for p in processes]

        # log failed processes
        fail = False
        for p in processes:
            if p.exitcode != 0:
                fail = True
                self.error(f"Process '{p.pid}' failed with exitcode '{p.exitcode}'")

        self._running = False

        if self.config["run"]["raise_exception"] and fail:
            raise Exception("Some processes failed, see logs for details")

        return self
