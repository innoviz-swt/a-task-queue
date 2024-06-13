import asyncio
import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from ataskq.handler import DBHandler, from_config
from ataskq.env import ATASKQ_SERVER_CONFIG

logger = logging.getLogger("uvicorn")


def db_handler() -> DBHandler:
    return from_config(ATASKQ_SERVER_CONFIG or "server")


async def set_timeout_tasks_task():
    dbh = db_handler()
    while True:
        logger.info(f"Set Timeout Tasks - {dbh.config['background']['pulse_timeout_interval']} sec interval")
        dbh.fail_pulse_timeout_tasks(dbh.config["monitor"]["pulse_timeout"])
        await asyncio.sleep(dbh.config["background"]["pulse_timeout_interval"])


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("enter lifspan")

    logger.info("init db")
    db_handler().init_db()

    task = asyncio.create_task(set_timeout_tasks_task())

    # Load the ML model
    yield
    # Clean up the ML models and release the resources
    logger.info("cancel task")
    task.cancel()
    logger.info("exit lifspan")


app = FastAPI(lifespan=lifespan)

# allow all cors
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
@app.get("/health")
async def health():
    return "Background manager is running"
