"""Special tests scripts which tests environment variables handling
   outside test suite scope (due to conflicts)
"""

import os
import sys

sys.path.append(f"{os.path.dirname(__file__)}/..")
from ataskq.config import load_config
from ataskq.config.config import DEFAULT_CONFIG


def load_with_env_override():
    os.environ["ataskq.connection"] = "test"
    os.environ["ataskq.run.wait_timeout"] = "111"
    config = load_config(DEFAULT_CONFIG)

    assert config["connection"] == "test"
    assert config["run"]["wait_timeout"] == 111.0

    os.environ.pop("ataskq.connection")
    os.environ.pop("ataskq.run.wait_timeout")


def load_with_env_override2():
    os.environ["ataskq_connection"] = "test-2"
    os.environ["ataskq_run_wait_timeout"] = "111.3"
    config = load_config(DEFAULT_CONFIG)

    assert config["connection"] == "test-2"
    assert config["run"]["wait_timeout"] == 111.3

    os.environ.pop("ataskq_connection")
    os.environ.pop("ataskq_run_wait_timeout")


if __name__ == "__main__":
    load_with_env_override()
    load_with_env_override2()
