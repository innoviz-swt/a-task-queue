from typing import Callable
from enum import Enum
import pickle
from importlib import import_module


class EntryPointRuntimeError(RuntimeError):
    pass


class TARGSLoadRuntimeError(EntryPointRuntimeError):
    pass


class EntrypointLoadRuntimeError(EntryPointRuntimeError):
    pass


class EntrypointCallRuntimeError(EntryPointRuntimeError):
    pass


class EStatus(str, Enum):
    PENDING = 'pending'
    RUNNING = 'running'
    SUCCESS = 'success'
    FAILURE = 'failure'


class EntryPoint:
    def __init__(self, targs=None, entrypoint='') -> None:
        self.targs = targs
        self.entrypoint = entrypoint

    def get_targs(self):
        if self.targs is not None:
            try:
                targs = pickle.loads(self.targs)
                assert len(targs) == 2, "targs must be tuple of 2 elements"
                assert isinstance(targs[0], tuple), "targs[0] must be args tuple"
                assert isinstance(targs[1], dict), "targs[0] must be kwargs dict"
            except Exception as ex:
                raise TARGSLoadRuntimeError() from ex

        else:
            targs = ((), {})

        return targs[0], targs[1]

    def get_entrypoint(self):
        ep = self.entrypoint

        try:
            assert '.' in ep, 'entry point must be inside a module.'
            module_name, func_name = ep.rsplit('.', 1)
            m = import_module(module_name)
            assert hasattr(
                m, func_name), f"failed to load entry point, module '{module_name}' doen't have func named '{func_name}'."
            func = getattr(m, func_name)
            assert callable(func), f"entry point is not callable, '{module_name}.{func}'."
        except ImportError as ex:
            raise EntrypointLoadRuntimeError(f"Failed to load module '{module_name}'. Exception: '{ex}'")
        except Exception as ex:
            raise EntrypointLoadRuntimeError(f"Failed to load entry point '{ep}'. Exception: '{ex}'") from ex

        return func

    def call(self):
        args, kwargs = self.get_targs()
        entrypoint = self.get_entrypoint()

        try:
            ret = entrypoint(*args, **kwargs)
        except Exception as ex:
            raise EntrypointCallRuntimeError(
                f"Failed while call entrypoint function '{self.entrypoint}'. Exception: '{ex}'") from ex

        return ret


class Task:
    def __init__(self,
                 task_id: int = None,
                 name: str = '',
                 level: float = 0,
                 entrypoint: str = "",
                 targs: tuple or bytes or None = None,
                 status: EStatus = EStatus.PENDING,
                 take_time=None,
                 start_time=None,
                 done_time=None,
                 pulse_time=None,
                 description=None,
                 # summary_cookie = None,
                 job_id=None,
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


class StateKWArg(EntryPoint):
    def __init__(self,
                 state_kwargs_id: int = None,
                 name: str = '',
                 entrypoint: Callable or str = '',
                 targs: tuple or bytes or None = None,
                 description: str = '',
                 job_id: int = None) -> None:
        super().__init__(targs=targs, entrypoint=entrypoint)

        self.state_kwargs_id = state_kwargs_id
        self.name = name
        self.description = description
        self.job_id = job_id


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
