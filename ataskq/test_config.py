import copy

import pytest

from .config import load_config
from .config.config import CONFIG_FORMAT, CONFIG_SETS, DEFAULT_CONFIG


def assert_config(ref_config: dict, test_config: dict, format=CONFIG_FORMAT, path="", count=0):
    """load config dict,

    Args:
        config (dict): _description_
        path (str, optional): _description_. Defaults to "".
    """
    for k in ref_config.keys():
        kpath = (path and path + ".") + k
        assert k in ref_config, f"{kpath} missing in dst_config"
        if isinstance(ref_config[k], dict):
            count += assert_config(ref_config[k], test_config[k], format=format[k], path=kpath, count=count)
        else:
            # overwrite config with environment value
            assert ref_config[k] is None or isinstance(ref_config[k], format[k]), f"config '{kpath}' mismatch"
            assert ref_config[k] == test_config[k], f"config '{kpath}' mismatch"
            count += 1

    return count


def test_load_default_none():
    config = load_config(None)
    assert_config(CONFIG_SETS[DEFAULT_CONFIG], config)


def test_load_default():
    config = load_config(DEFAULT_CONFIG)
    assert_config(CONFIG_SETS[DEFAULT_CONFIG], config)


def test_load_client_preset():
    config = load_config("client")

    ref = copy.deepcopy(CONFIG_SETS[DEFAULT_CONFIG])
    ref["connection"] = "http://localhost:8080"
    ref["handler"]["db_init"] = False

    assert_config(ref, config)


def test_load_custom():
    config = load_config({"connection": "test", "run": {"wait_timeout": 100}})

    ref = copy.deepcopy(CONFIG_SETS[DEFAULT_CONFIG])
    ref["connection"] = "test"
    ref["run"]["wait_timeout"] = 100.0

    assert_config(ref, config)


def test_load_custom2():
    config = load_config([{"connection": "test", "run": {"wait_timeout": 100}}])

    ref = copy.deepcopy(CONFIG_SETS[DEFAULT_CONFIG])
    ref["connection"] = "test"
    ref["run"]["wait_timeout"] = 100.0

    assert_config(ref, config)


def test_load_custom_and_preset():
    config = load_config([{"connection": "test"}, "client"])

    ref = copy.deepcopy(CONFIG_SETS[DEFAULT_CONFIG])
    ref["connection"] = "test"
    ref["handler"]["db_init"] = False

    assert_config(ref, config)


def test_invalid_config_type():
    with pytest.raises(RuntimeError) as excinfo:
        load_config(config=2)

    assert "invalid config type. supported types: ['str', 'Path', 'dict']" == str(excinfo.value)


def test_invalid_config_type_in_list():
    with pytest.raises(RuntimeError) as excinfo:
        load_config(config=[DEFAULT_CONFIG, 2])

    assert "onvalid config[1] element type. supported types: ['str', 'Path', 'dict']" == str(excinfo.value)


def test_loaded_config():
    c = {"a": 3, "_loaded": True}
    config = load_config(c)

    assert id(c) == id(config)
