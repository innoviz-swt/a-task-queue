from typing import Union, List
from pathlib import Path
import json
import os

from .config import CONFIG_FORMAT, CONFIG_SETS, DEFAULT_CONFIG

CONGIF_TYPE = Union[str, Path, dict]


def _load_config(config: List[dict], default: dict, format=CONFIG_FORMAT, path="", environ=False):
    """load config dict,

    Args:
        config (dict): _description_
        path (str, optional): _description_. Defaults to "".
    """
    ret = {}
    for k in format.keys():
        kpath = (path and path + ".") + k

        # recursive iterate dicts
        if isinstance(format[k], dict):
            ret[k] = _load_config(
                [c[k] for c in config if k in c],
                default=default[k],
                format=format[k],
                path=kpath,
                environ=environ,
            )
            continue

        # set default value
        ret[k] = default[k]

        # overwrite config with environment value (if exists)
        if environ:
            env_var = f"ataskq.{kpath}"
            if env_var in os.environ:
                try:
                    ret[k] = format[k](os.environ[env_var])
                    continue
                except:
                    raise ValueError(
                        f"Failed parsing config env variable '{env_var}' value '{os.environ[env_var]}' to '{format[k].__name__}'"
                    )

            env_var = env_var.replace(".", "_")
            if env_var in os.environ:
                try:
                    ret[k] = format[k](os.environ[env_var])
                    continue
                except:
                    raise ValueError(
                        f"Failed parsing config env variable '{env_var}' value '{os.environ[env_var]}' to '{format[k].__name__}'"
                    )

        # set first config value found
        try:
            ret[k] = next(c[k] for c in config if k in c)
        except StopIteration:  # no matching key in config
            pass

        if ret[k] is not None:
            try:
                ret[k] = format[k](ret[k])
            except:
                raise ValueError(f"config '{kpath}' value  '{ret[k]}' can't be cast to '{format[k].__name__}'")

    return ret


def load_file(config: Path):
    if not config.exists():
        raise FileExistsError(f"config file '{config}' doesn't exists.")

    if config.suffix == ".json":
        with open(config) as f:
            ret = json.load(f)
    else:
        raise RuntimeError("Unsupported config file. only [.py, .json] file types are supported.")

    return ret


def load_config(config: Union[CONGIF_TYPE, List[CONGIF_TYPE]]):
    if config is None or config == DEFAULT_CONFIG:
        # default config used as base configuration for _load_config
        config = []
    elif not isinstance(config, list):
        config = [config]

    # convert config to dicts
    for i, c in enumerate(config):
        if isinstance(c, str):
            if c in CONFIG_SETS:
                # presets
                config[i] = CONFIG_SETS[c]
            else:
                config[i] = load_file(Path(c))
        elif isinstance(c, Path):
            config[i] = load_file(c)
        elif isinstance(c, dict):
            continue

    ret = _load_config(config, CONFIG_SETS[DEFAULT_CONFIG], environ=True)

    return ret
