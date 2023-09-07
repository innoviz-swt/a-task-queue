from enum import Enum


class EStatus(str, Enum):
    PENDING = 'pending'
    RUNNING = 'running'
    SUCCESS = 'success'
    FAILURE = 'failure'


class Task:
    def __init__(self, 
            tid = None, 
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
        ) -> None:

        self.tid = tid
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
