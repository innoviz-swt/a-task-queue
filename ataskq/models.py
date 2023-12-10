from typing import Union, Callable
from enum import Enum
import pickle
from importlib import import_module
import inspect
from datetime import datetime


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


def _handle_union(self, cls_name, member, annotations, value):
    # check if value is of supported types
    for a in annotations:
        if isinstance(value, a):
            setattr(self, member, value)
            return

    # attemp cast value
    success = False
    for a in annotations:
        try:
            v = a(v)
            setattr(self, member, v)
            success = True
            break
        except Exception:
            continue

    if not success:
        raise Exception(f"{cls_name}::{member}({annotations}) failed casting {type(value)} - '{value}'.")


def model_class(**defaults):
    def wrapper(cls):
        # Save the original __init__ method
        cls_init = cls.__init__
        cls_annotations = cls.__annotations__
        cls_name = cls.__name__

        def new_init(self, **kwargs):
            for member, annotation in cls_annotations.items():
                # default None to members not passed
                if member not in kwargs:
                    value = defaults.get(member)
                    setattr(self, member, value)
                    continue

                value = kwargs[member]

                # value is None, no type casting required
                if value is None:
                    setattr(self, member, value)
                    continue

                annotation = cls_annotations.get(member)
                if annotation is None:
                    # no annotation, not type casting required
                    setattr(self, member, value)
                    continue

                if getattr(annotation, '__origin__', None) is Union:
                    _handle_union(self, cls_name, member, annotation.__args__, value)
                else:
                    # Single annotation - avoid casting None
                    try:
                        value = annotation(value)
                    except Exception as ex:
                        raise Exception(f"{cls_name}::{member}({annotation}) failed casting '{value}'.") from ex
                    setattr(self, member, value)

            # Call the original __init__ method
            cls_init(self)

        # Replace the original __init__ method with the new one
        cls.__init__ = new_init

        # Return the modified class
        return cls
    return wrapper


@model_class(status=EStatus.PENDING, entrypoint='', level=0.0)
class Task:
    task_id: int
    name: str
    level: float
    entrypoint: Union[str, Callable]
    targs: Union[tuple, bytes]
    status: EStatus
    take_time: Union[str, datetime]
    start_time: Union[str, datetime]
    done_time: Union[str, datetime]
    pulse_time: Union[str, datetime]
    description: Union[str, datetime]
    # summary_cookie = None,
    job_id: int

    @staticmethod
    def id_key():
        return 'task_id'


class StateKWArg(EntryPoint):
    def __init__(self,
                 state_kwargs_id: int = None,
                 name: str = None,
                 entrypoint: Callable or str = None,
                 targs: tuple or bytes or None = None,
                 description: str = None,
                 job_id: int = None) -> None:
        super().__init__(targs=targs, entrypoint=entrypoint)

        self.state_kwargs_id = state_kwargs_id
        self.name = name
        self.description = description
        self.job_id = job_id


class Job:
    def __init__(self,
                 job_id: int = None,
                 name: None or str = None,
                 priority: float = 0,
                 description: None or str = None) -> None:
        self.job_id = job_id
        self.name = name
        self.priority = priority
        self.description = description
