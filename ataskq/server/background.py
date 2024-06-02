import asyncio
import logging

from ataskq.handler import DBHandler, from_config
from ataskq.env import ATASKQ_SERVER_CONFIG


def init_logger(level=logging.INFO):
    logger = logging.getLogger("server-background")

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
    i = 0
    while True:
        if i % 10 == 0:
            logger.info(f"[{i}] Set Timeout Tasks")
        else:
            logger.debug(f"[{i}] Set Timeout Tasks")
        dbh.fail_pulse_timeout_tasks(dbh.config["monitor"]["pulse_timeout"])
        await asyncio.sleep(dbh.config["monitor"]["pulse_interval"])
        i += 1


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
