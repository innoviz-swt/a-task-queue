from enum import Enum


class EStatus(str, Enum):
    PENDING = 'pending'
    RUNNING = 'running'
    SUCCESS = 'success'
    FAILURE = 'failure'

class Job:
    def __init__(self,
                 job_id: int = None,
                 name: str = '',
                 priority: float = 0,
                 description: str = '') -> None:
        self.job_id = job_id
        self.name = name
        self.priority = priority
        self.description = description
        
        

class Task:
    def __init__(self, 
            task_id: int = None, 
            name: str = '', 
            level: float = 0, 
            entrypoint: str = "", 
            targs: tuple or None = None, 
            status: EStatus = EStatus.PENDING, 
            take_time = None, 
            start_time = None, 
            done_time = None,  
            pulse_time = None,  
            description = None,
            # summary_cookie = None,
            job_id = None,
        ) -> None:

        self.task_id = task_id
        self.name = name
        self.level = level
        self.entrypoint = entrypoint
        self.targs = targs
        self.status = status
        self.take_time = take_time
        self.start_time = start_time
        self.done_time = done_time
        self.pulse_time = pulse_time
        self.description = description
        # self.summary_cookie = summary_cookie
        self.job_id = job_id
