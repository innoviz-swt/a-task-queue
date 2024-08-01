from importlib import import_module
from typing import Type


def import_callable(func: str):
    try:
        assert "." in func, "entry point must be inside a module."
        module_name, func_name = func.rsplit(".", 1)
        m = import_module(module_name)
        assert hasattr(
            m, func_name
        ), f"failed to load function, module '{module_name}' doen't have entry named '{func_name}'."
        func = getattr(m, func_name)
        assert callable(func), f"entry point is not callable, '{module_name}.{func}'."
    except ImportError as ex:
        raise RuntimeError(f"Failed to import module '{module_name}'. ImportError: '{ex}'")
    except Exception as ex:
        raise RuntimeError(f"Failed to load function '{func}'. Exception: '{ex}'") from ex

    return func


def import_class(cls: str, cls_type: Type = None):
    try:
        assert "." in cls, "entry point must be inside a module."
        module_name, cls_name = cls.rsplit(".", 1)
        m = import_module(module_name)
        assert hasattr(
            m, cls_name
        ), f"failed to load class, module '{module_name}' doen't have entry named '{cls_name}'."
        cls = getattr(m, cls_name)
        if cls_type:
            assert issubclass(cls, cls_type), f"entry point is not of types: {[c.__name__ for c in cls_type]}"
    except ImportError as ex:
        raise RuntimeError(f"Failed to import module '{module_name}'. ImportError: '{ex}'")
    except Exception as ex:
        raise RuntimeError(f"Failed to load class '{cls}'. Exception: '{ex}'") from ex

    return cls
