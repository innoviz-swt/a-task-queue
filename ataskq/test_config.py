import copy
import os

import pytest

from .config import load_config, get_config_set
from .config.config import CONFIG_FORMAT, DEFAULT_CONFIG


def assert_instance(ref, format):
    if ref is None:
        return True
    elif isinstance(ref, (int, float)) and issubclass(format, (int, float)):
        return True
    else:
        return isinstance(ref, format)


def assert_config(ref_config: dict, test_config: dict, format=CONFIG_FORMAT, path="", count=None):
    if count is None:
        count = [0]

    for k in ref_config.keys():
        kpath = (path and path + ".") + k
        assert k in ref_config, f"{kpath} missing in dst_config"
        if isinstance(ref_config[k], dict):
            assert_config(ref_config[k], test_config[k], format=format[k], path=kpath, count=count)
        else:
            # overwrite config with environment value
            assert assert_instance(ref_config[k], format[k]), f"config '{kpath}' type mismatch"
            assert ref_config[k] == test_config[k], f"config '{kpath}' value mismatch"
            count[0] += 1

    return count[0]


def test_load_default_none():
    config = load_config(environ=False)
    count = assert_config(get_config_set(), config)
    # sanity
    assert count == 12, "invalid number of configurations."


def test_load_default():
    config = load_config(DEFAULT_CONFIG, environ=False)
    assert_config(get_config_set(), config)


def test_load_client_preset():
    config = load_config("client", environ=False)

    ref = copy.deepcopy(get_config_set())
    ref["connection"] = "http://localhost:8080"
    ref["handler"]["db_init"] = False
    ref["run"]["fail_pulse_timeout"] = False

    assert_config(ref, config)


def test_load_custom():
    config = load_config({"connection": "test", "run": {"wait_timeout": 100}}, environ=False)

    ref = copy.deepcopy(get_config_set())
    ref["connection"] = "test"
    ref["run"]["wait_timeout"] = 100.0

    assert_config(ref, config)


def test_load_custom2():
    config = load_config([{"connection": "test", "run": {"wait_timeout": 100}}], environ=False)

    ref = copy.deepcopy(get_config_set())
    ref["connection"] = "test"
    ref["run"]["wait_timeout"] = 100.0

    assert_config(ref, config)


def test_load_custom_and_preset():
    config = load_config(["client", {"connection": "test"}], environ=False)

    ref = copy.deepcopy(get_config_set())
    ref["connection"] = "test"
    ref["handler"]["db_init"] = False
    ref["run"]["fail_pulse_timeout"] = False

    assert_config(ref, config)


def test_invalid_config_format():
    with pytest.raises(ValueError) as excinfo:
        load_config({"run": {"wait_timeout": "asd"}}, environ=False)

    assert "config 'run.wait_timeout' value 'asd' can't be cast to 'float'" == str(excinfo.value)


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
