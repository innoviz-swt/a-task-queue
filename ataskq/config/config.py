DEFAULT_CONFIG = "standalone"

CONFIG_FORMAT = {
    "connection": str,  # ATASKQ_CONNECTION
    "run": {
        "wait_timeout": float,
        "pull_interval": float,
        "fail_pulse_timeout": bool,
        "raise_exception": bool,
        "run_forever": bool,
    },
    "handler": {
        "db_init": bool,
    },
    "db": {
        "max_jobs": int,
    },
    "monitor": {
        "pulse_interval": float,
        "pulse_timeout": float,
    },
    "background": {
        "pulse_timeout_interval": float,
    },
    "api": {
        "limit": int,
    },
}

CONFIG_SETS = {
    "standalone": {
        "connection": "sqlite://ataskq.db.sqlite3",
        "run": {
            "wait_timeout": None,
            "pull_interval": 15,
            "fail_pulse_timeout": True,
            "raise_exception": False,
            "run_forever": False,
        },
        "handler": {
            "db_init": True,
        },
        "db": {
            "max_jobs": None,
        },
        "monitor": {
            "pulse_interval": 15,
            "pulse_timeout": 60 * 5,
        },
        "background": {
            "pulse_timeout_interval": 60,
        },
        "api": {
            "limit": 100,
        },
    },
    "test": {
        "connection": "sqlite://{tmp_path}/ataskq.db.sqlite3",
        "run": {
            "pull_interval": 0.3,
        },
    },
    "client": {
        "connection": "http://localhost:8080",
        "run": {
            "fail_pulse_timeout": False,
        },
        "handler": {
            "db_init": False,
        },
    },
    "server": {
        "run": {
            "fail_pulse_timeout": False,
        },
        "handler": {
            "db_init": False,
        },
    },
}
