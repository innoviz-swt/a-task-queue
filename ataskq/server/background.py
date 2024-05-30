import asyncio
import logging

from ataskq.handler import DBHandler, from_config
from ataskq.env import ATASKQ_SERVER_CONFIG


def init_logger(level=logging.INFO):
    logger = logging.getLogger("server-worker")

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(logging.BASIC_FORMAT))
    handler.setLevel(level)

    logger.addHandler(handler)
    logger.setLevel(level)

    return logger


logger = init_logger()


def db_handler() -> DBHandler:
    return from_config(ATASKQ_SERVER_CONFIG or "server")


async def set_timout_tasks_task():
    dbh = db_handler()
    while True:
        logger.debug("Set Timeout Tasks")
        dbh.fail_pulse_timeout_tasks(dbh.config["monitor"]["pulse_timeout"])
        await asyncio.sleep(dbh.config["monitor"]["pulse_interval"])


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
