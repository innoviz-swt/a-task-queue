DEFAULT_CONFIG = "standalone"

CONFIG_FORMAT = {
    "connection": str,  # ATASKQ_CONNECTION
    "run": {
        "wait_timeout": float,
        "pull_interval": float,
        "raise_exception": bool,
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
    "api": {
        "limit": int,
    },
}

CONFIG_SETS = {
    "standalone": {
        "connection": "sqlite://ataskq.db.sqlite3",
        "run": {
            "wait_timeout": None,
            "pull_interval": 0.2,
            "raise_exception": True,
        },
        "handler": {
            "db_init": True,
        },
        "db": {
            "max_jobs": None,
        },
        "monitor": {
            "pulse_interval": 0.2,
            "pulse_timeout": 60 * 5.0,
        },
        "api": {
            "limit": 100,
        },
    },
    "client": {
        "handler": {
            "db_init": False,
        },
    },
    "server": {
        "handler": {
            "db_init": False,
        }
    },
}
