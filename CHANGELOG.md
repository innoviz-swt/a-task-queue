# ChangeLog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## Unreleased
### Added
### Changed
### Fixed

# 0.6.4
### Added
### Changed
### Fixed
- dashboard handling no job_id

# 0.6.3
### Added
- background task implemented as server
### Changed
### Fixed
- Docker image update

# 0.6.2
### Added
### Changed
- db schema v5: state_kwargs removed from schema
### Fixed
- Docker image update


# 0.6.1
### Added
### Changed
### Fixed
- sqlite3 returning error on sqlite ver smaller 3.35.0

# 0.6.0
### Added
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
### Changed
- taskq configuration moved to config
(conn, run_task_raise_exception, task_wait_timeout, task_pull_intervnal, monitor_pulse_interval, task_pulse_timeout, max_jobs)
- taskq.run num_processes renamed to concurrency
### Fixed

# 0.5.1
### Added
### Changed
### Fixed
- fixed python 3.11+ support.

# 0.5.0
### Added
- add postgresql and rest handlers
### Changed
- TASKQ __init__ db renamed to conn
- ATASKQ::create_job removed overwrite flag
### Fixed

# 0.4.1
### Added
### Changed
### Fixed
- exception during code execution causing crash (unassigne variable 'ex')

# 0.4.0
### Added
### Changed
- log warning on run exception
### Fixed

# 0.3.0
### Added
### Changed
- update README with basic usage example
### Fixed

# 0.2.0
### Added
### Changed
- removed job_path, keeping db only.
### Fixed


# 0.1.0
### Added
- first release.
### Changed
### Fixed
