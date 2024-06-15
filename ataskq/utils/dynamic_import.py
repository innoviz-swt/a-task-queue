from importlib import import_module


def import_callable(func: str):
    try:
        assert "." in func, "entry point must be inside a module."
        module_name, func_name = func.rsplit(".", 1)
        m = import_module(module_name)
        assert hasattr(
            m, func_name
        ), f"failed to load entry point, module '{module_name}' doen't have func named '{func_name}'."
        func = getattr(m, func_name)
        assert callable(func), f"entry point is not callable, '{module_name}.{func}'."
    except ImportError as ex:
        raise RuntimeError(f"Failed to import module '{module_name}'. ImportError: '{ex}'")
    except Exception as ex:
        raise RuntimeError(f"Failed to load entry point '{func}'. Exception: '{ex}'") from ex

    return func
