import asyncio
import logging

from ataskq.handler import DBHandler, from_connection_str
from ataskq.env import (
    ATASKQ_SERVER_CONNECTION,
    ATASKQ_SERVER_TASK_PULSE_TIMEOUT_MONITOR_INTERVAL,
    ATASKQ_TASK_PULSE_TIMEOUT,
)

def init_logger(level=logging.INFO):
    logger = logging.getLogger("server-worker")

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(logging.BASIC_FORMAT))
    handler.setLevel(level)

    logger.addHandler(handler)
    logger.setLevel(level)

    return logger


logger = init_logger()
logger.info(f"ATASKQ_SERVER_CONNECTION: {ATASKQ_SERVER_CONNECTION}")
logger.info(f"ATASKQ_TASK_PULSE_TIMEOUT: {ATASKQ_TASK_PULSE_TIMEOUT}")
logger.info(f"ATASKQ_SERVER_TASK_PULSE_TIMEOUT_MONITOR_INTERVAL: {ATASKQ_SERVER_TASK_PULSE_TIMEOUT_MONITOR_INTERVAL}")


def db_handler() -> DBHandler:
    return from_connection_str(ATASKQ_SERVER_CONNECTION)


async def set_timout_tasks_task():
    dbh = db_handler()
    while True:
        logger.debug("Set Timeout Tasks")
        dbh.fail_pulse_timeout_tasks(ATASKQ_TASK_PULSE_TIMEOUT)
        await asyncio.sleep(ATASKQ_SERVER_TASK_PULSE_TIMEOUT_MONITOR_INTERVAL)


async def main():
    await asyncio.gather(
        set_timout_tasks_task(),
    ) 


def run():
    dbh: DBHandler = db_handler()
    dbh.init_db()
    asyncio.run(main())


if __name__ == "__main__":
    run()