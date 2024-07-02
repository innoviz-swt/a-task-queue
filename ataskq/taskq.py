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
        job_id=None,
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

        # get job
        if job_id is not None:
            try:
                job = Job.get(job_id, _handler=self._handler)
            except Exception as ex:
                raise Exception(f"Failed to fetch job (job_id={job_id}).") from ex
        else:
            job = None
        self._job = job

        self._running = False

    @property
    def config(self):
        return self._config

    @property
    def job(self):
        return self._job

    @property
    def job_id(self):
        return self._job.job_id

    @property
    def handler(self):
        return self._handler

    @property
    def task_wait_interval(self):
        return self._task_pull_interval

    @property
    def monitor_pulse_interval(self):
        return self._monitor_pulse_interval

    def clear_job(self):
        self._job = None

    def set_job(self, job_id):
        job = Job.get(job_id, self._handler)
        self._handler.set_job_id(job.job_id)  # todo remove - handler should need job id ...
        self._job = job

    def create_job(self, name=None, description=None):
        assert self._job is None, "job already assigned to current taskq, run clear_job to create new job."

        job = Job(name=name, description=description).create(_handler=self._handler)
        self._job = job

        if self.config["db"]["max_jobs"] is not None:
            # keep max jbos
            Job.delete_all(
                _where=f"job_id NOT IN (SELECT job_id FROM jobs ORDER BY job_id DESC limit {self.config['db']['max_jobs']})",
                _handler=self.handler,
            )

        return self

    def delete_job(self):
        self.job.delete(_handler=self.handler)
        self._job = None

    def get_tasks(self, **kwargs) -> List[Task]:
        if self.job:
            return self.job.get_tasks(_handler=self._handler, **kwargs)
        else:
            return Task.get_all(**kwargs)

    def add_tasks(self, tasks):
        self.job.add_tasks(tasks, _handler=self._handler)

        return self

    def update_task_start_time(self, task: Task, start_time: datetime = None):
        if start_time is None:
            start_time = datetime.now()

        task.update(start_time=start_time, _handler=self._handler)

    def update_task_status(self, task: Task, status: EStatus, timestamp: datetime = None):
        if timestamp is None:
            timestamp = datetime.now()

        if status == EStatus.RUNNING:
            # for running task update pulse_time
            task.update(_handler=self._handler, status=status, pulse_time=timestamp)
        elif status == EStatus.SUCCESS or status == EStatus.FAILURE:
            # for done task update pulse_time and done_time time as well
            task.update(_handler=self._handler, status=status, pulse_time=timestamp, done_time=timestamp)
        else:
            raise RuntimeError(f"Unsupported status '{status}' for status update")

    def count_pending_tasks_below_level(self, level):
        ret = Task.count_all(
            _where=f"job_id = {self.job_id} AND level < {level} AND status in ('{EStatus.PENDING}')",
            _handler=self._handler,
        )
        return ret

    def get_object(self, obj):
        if isinstance(obj, int):  # object id
            obj = Object.get(obj, _handler=self._handler)

        ret = obj.deserialize()

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
        if task.kwargs_oid is not None:
            try:
                task_kwargs_obj = task.get_kwargs(self._handler)
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

    def _take_next_task(self, level=None):
        level_start = level.start if level is not None else None
        level_stop = level.stop if level is not None else None

        job_id = self.job_id if self.job is not None else None
        return self._handler.take_next_task(job_id=job_id, level_start=level_start, level_stop=level_stop)

    def _run(self, level):
        self.info(f"Started task pulling loop.")

        # check for error code
        task_pull_start = time.time()
        while True:
            # if the taskq handler is db handler, the taskq performs background tasks before each run
            if self.config["run"]["fail_pulse_timeout"] and isinstance(self._handler, DBHandler):
                self._handler.fail_pulse_timeout_tasks(self.config["monitor"]["pulse_timeout"])
            # grab tasks and set them in Q
            action, task = self._take_next_task(level)

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

    def run(self, concurrency=None, level=None):
        self.info(f"Start running with connection {self.config['connection']}")
        if level is not None:
            level = self.assert_level(level)

        self._running = True

        # default to run in current process
        if concurrency is None:
            self._run(level)
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
        processes = [Process(target=self._run, args=(level,)) for i in range(nprocesses)]
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
