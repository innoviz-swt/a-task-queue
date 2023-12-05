from abc import abstractmethod
from .logger import Logger


class Handler(Logger):
    def __init__(self, job_id=None, logger: Logger = None):
        super().__init__(logger)

        self._job_id = job_id

    @property
    def job_id(self):
        return self._job_id

    @abstractmethod
    def create_job(self, c, name='', description=''):
        pass
