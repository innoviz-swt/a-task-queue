from typing import Union
from pathlib import Path
import json
import os

from .config import CONFIG_FORMAT, DEFAULT_CONFIG_SETS


def _load_config(config: dict, format=CONFIG_FORMAT, path="", environ=False):
    """load config dict,

    Args:
        config (dict): _description_
        path (str, optional): _description_. Defaults to "".
    """
    if not environ:
        return config

    for k, v in config.items():
        kpath = (path and path + ".") + k
        if isinstance(v, dict):
            config[k] = _load_config(v, format=format[k], path=kpath, environ=True)
        else:
            # overwrite config with environment value
            env_var = f"ataskq.{kpath}"
            if env_var in os.environ:
                try:
                    config[k] = format[k](os.environ[env_var])
                    continue
                except:
                    raise ValueError(
                        f"Failed parsing config env variable '{env_var}' value '{os.environ[env_var]}' to '{format[k].__name__}'"
                    )

            env_var = env_var.replace(".", "_")
            if env_var in os.environ:
                try:
                    config[k] = format[k](os.environ[env_var])
                    continue
                except:
                    raise ValueError(
                        f"Failed parsing config env variable '{env_var}' value '{os.environ[env_var]}' to '{format[k].__name__}'"
                    )

    return config


def load_config_from_file(config: Union[str, Path], environ=False):
    config = Path(config)
    if not config.exists():
        raise FileExistsError(f"config file '{config}' doesn't exists.")

    if config.suffix == ".py":
        try:
            ret = eval(config.read_text())
        except Exception:
            raise RuntimeError(f"Failed evaluation .py file with config dict from '{config}'.")
    elif config.suffix == ".json":
        with open(config) as f:
            ret = json.load()
    else:
        raise RuntimeError("Unsupported config file. only [.py, .json] file types are supported.")

    ret = _load_config(ret, environ=environ)
    return ret


def load_config(config: Union[str, Path, dict], environ=False):
    # presets
    if config == "default":
        config = "standalone"

    if config in DEFAULT_CONFIG_SETS:
        ret = _load_config(DEFAULT_CONFIG_SETS[config], environ=environ)
    elif isinstance(config, Path):
        ret = load_config_from_file(config, environ=environ)
    elif isinstance(config, dict):
        ret = _load_config(config, environ=environ)

    return ret
