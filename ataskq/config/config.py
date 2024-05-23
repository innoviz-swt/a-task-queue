CONFIG_FORMAT = {
    "connection": str,  # ATASKQ_CONNECTION
    "run": {
        "wait_timeout": float,  # ATASKQ_TASK_WAIT_TIMEOUT
        "pull_interval": float,  # ATASKQ_TASK_PULL_INTERVAL
        "db_init": bool,  # ATASKQ_DB_INIT_ON_HANDLER_INIT
    },
    "monitor": {
        "pulse_interval": float,  # ATASKQ_MONITOR_PULSE_INTERVAL
        "pulse_timeout": float,  # ATASKQ_TASK_PULSE_TIMEOUT
    },
    "api": {
        "limit": int,  # ATASKQ_DB_LIMIT_DEFAULT
    },
}

DEFAULT_CONFIG_SETS = {
    "standalone": {
        "connection": "sqlite://ataskq.db.sqlite3",  # ATASKQ_CONNECTION
        "run": {
            "wait_timeout": None,  # ATASKQ_TASK_WAIT_TIMEOUT
            "pull_interval": 0.2,  # ATASKQ_TASK_PULL_INTERVAL
            "db_init": True,  # ATASKQ_DB_INIT_ON_HANDLER_INIT
        },
        "monitor": {
            "pulse_interval": 0.2,  # ATASKQ_MONITOR_PULSE_INTERVAL
            "pulse_timeout": 60 * 5,  # ATASKQ_TASK_PULSE_TIMEOUT
        },
        "api": {
            "limit": 100,  # ATASKQ_DB_LIMIT_DEFAULT
        },
    },
}
