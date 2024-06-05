# ChangeLog

# 0.6.2
- bugfix: Docker image update
- db schema v5: state_kwargs removed from schema

# 0.6.1
- bugfix: sqlite3 returning error on sqlite ver smaller 3.35.0

# 0.6.0
- taskq configuration moved to config
(conn, run_task_raise_exception, task_wait_timeout, task_pull_intervnal, monitor_pulse_interval, task_pulse_timeout, max_jobs)
- taskq.run num_processes renamed to concurrency
- added \_\_main\_\_.py entry point for running ataskq
    ```
    usage: ataskq run [-h] [--config CONFIG] [--job-id JOB_ID] [--level LEVEL [LEVEL ...]]
                    [--concurrency CONCURRENCY]

    optional arguments:
        -h, --help            show this help message and exit
        --config CONFIG, -c CONFIG
                            config preset ['standalone', 'client', 'server'] or path to file
        --job-id JOB_ID, -jid JOB_ID
                            job id to run
        --level LEVEL [LEVEL ...], -l LEVEL [LEVEL ...]
                            job level to run
        --concurrency CONCURRENCY, -cn CONCURRENCY
                            number of task execution runner to open in parallel
    ```

# 0.5.1
- fixed python 3.11+ support.

# 0.5.0
- TASKQ __init__ db renamed to conn
- add postgresql and rest handlers
- ATASKQ::create_job removed overwrite flag

# 0.4.1
- bugfix: exception during code execution causing crash (unassigne variable 'ex')

# 0.4.0
- log warning on run exception

# 0.3.0
- update README with basic usage example

# 0.2.0
- removed job_path, keeping db only.

# 0.1.0
- first release.

# 0.0.0
- creating empty package and upload to pypi.
