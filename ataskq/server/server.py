import logging
import asyncio
from typing import Union
from pathlib import Path

from fastapi import FastAPI, Request, Depends
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager


from ataskq.handler import from_connection_str
from ataskq.db_handler import DBHandler
from ataskq.rest_handler import RESTHandler as rh
from ataskq.models import Task, StateKWArg
from ataskq.env import ATASKQ_SERVER_CONNECTION, ATASKQ_SERVER_TASK_PULSE_TIMEOUT_MONITOR_INTERVAL, ATASKQ_TASK_PULSE_TIMEOUT
# from .form_utils import form_data_array

logger = logging.getLogger("uvicorn")
logger.info(f"ATASKQ_SERVER_CONNECTION: {ATASKQ_SERVER_CONNECTION}")
logger.info(f"ATASKQ_TASK_PULSE_TIMEOUT: {ATASKQ_TASK_PULSE_TIMEOUT}")
logger.info(f"ATASKQ_SERVER_TASK_PULSE_TIMEOUT_MONITOR_INTERVAL: {ATASKQ_SERVER_TASK_PULSE_TIMEOUT_MONITOR_INTERVAL}")


# DB Handler


def db_handler(job_id: int = None) -> DBHandler:
    return from_connection_str(ATASKQ_SERVER_CONNECTION, job_id=job_id)


async def set_timout_tasks_task():
    dbh = db_handler()
    while True:
        logger.debug('Set Timeout Tasks')
        dbh.fail_pulse_timeout_tasks(ATASKQ_TASK_PULSE_TIMEOUT)
        await asyncio.sleep(ATASKQ_SERVER_TASK_PULSE_TIMEOUT_MONITOR_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info('enter lifspan')

    logger.info('init db')
    db_handler().init_db()

    task = asyncio.create_task(set_timout_tasks_task())

    # Load the ML model
    yield
    # Clean up the ML models and release the resources
    logger.info('cancel task')
    task.cancel()
    logger.info('exit lifspan')

app = FastAPI(lifespan=lifespan)

# allow all cors
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Custom exception handler


# async def custom_exception_handler(request: Request, exc: Exception):
#     raise exc

# # Adding the custom exception handler to the app
# app.add_exception_handler(Exception, custom_exception_handler)

# Example route with intentional exception


# static folder
app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")


# @app.on_event('startup')
# @repeat_every(seconds=3)
# async def print_hello():
#     logger.info("hello")


@app.get("/")
async def root():
    return "Welcome to A-TASK-Q Server"


@app.get("/favicon.ico")
async def favicon():
    return FileResponse(Path(__file__).parent / "static" / "favicon.ico")


@app.get("/api")
async def api():
    return {"message": "Welcome to A-TASK-Q Server"}


#########
# TASKS #
#########
@app.put("/api/tasks/{task_id}")
async def update_task(task_id: int, request: Request, dbh: DBHandler = Depends(db_handler)):
    ikwargs = await request.json()
    mkwargs = rh.i2m(Task, ikwargs)
    dbh.update_task(task_id, **mkwargs)

    return {task_id: task_id}


@app.get("/api/tasks/{task_id}/bytes/{field}")
async def get_task_byte_field(task_id: int, field: str, dbh: DBHandler = Depends(db_handler), **kwargs):
    dbh.get_task[task_id]

    return {task_id: task_id}


########
# JOBS #
########
@app.get("/api/jobs")
async def get_jobs(dbh: DBHandler = Depends(db_handler)):
    jobs = dbh.get_jobs()
    i_jobs = [rh.to_interface(j) for j in jobs]

    return i_jobs


@app.post("/api/jobs")
async def create_job(data: dict, dbh: DBHandler = Depends(db_handler)):
    job_id = dbh.create_job(name=data.get('name'), description=data.get('description')).job_id

    return {"job_id": job_id}


@app.delete("/api/jobs/{job_id}")
async def delete_job(job_id: int, dbh: DBHandler = Depends(db_handler)):
    dbh.set_job_id(job_id)
    dbh.delete_job()

    return {"job_id": job_id}


@app.get("/api/jobs/{job_id}/tasks")
async def get_job_tasks(job_id: int, dbh: DBHandler = Depends(db_handler)):
    dbh.set_job_id(job_id)

    tasks = dbh.get_tasks()
    itasks = [rh.to_interface(t) for t in tasks]

    return itasks


@app.post("/api/jobs/{job_id}/tasks")
async def post_job_tasks(job_id: int, request: Request, dbh: DBHandler = Depends(db_handler)):
    dbh.set_job_id(job_id)

    # # get form data
    # data = await request.form()
    # data = await form_data_array(data)

    # get data
    itasks = await request.json()

    # from interrface
    tasks = [rh.from_interface(Task, t) for t in itasks]
    dbh.add_tasks(tasks)

    return [t.task_id for t in tasks]


@app.get("/api/jobs/{job_id}/state_kwargs")
async def get_job_state_kwargs(job_id: int, dbh: DBHandler = Depends(db_handler)):
    dbh.set_job_id(job_id)

    state_kwargs = dbh.get_state_kwargs()
    i_state_kwargs = [rh.to_interface(t) for t in state_kwargs]

    return i_state_kwargs


@app.post("/api/jobs/{job_id}/state_kwargs")
async def post_job_state_kwargs(job_id: int, request: Request, dbh: DBHandler = Depends(db_handler)):
    dbh.set_job_id(job_id)

    # get data
    i_state_kwargs = await request.json()

    # from interrface
    state_kwargs = [rh.from_interface(StateKWArg, t) for t in i_state_kwargs]
    dbh.add_state_kwargs(state_kwargs)

    return [skw.state_kwargs_id for skw in state_kwargs]


@app.get("/api/jobs/{job_id}/next_task")
async def next_job_task(job_id: int, level_start: Union[int, None] = None, level_stop: Union[int, None] = None, dbh: DBHandler = Depends(db_handler)):
    dbh.set_job_id(job_id)

    # get level range
    if level_start is not None and level_stop is not None:
        level = range(level_start, level_stop)
    elif level_start is not None:
        level = range(level_start, level_start + 1)
    else:
        level = None

    # take next task
    action, task = dbh._take_next_task(level)
    task = rh.to_interface(task) if task is not None else None

    return dict(action=action, task=task)


@app.get("/api/jobs/{job_id}/state_kwargs")
async def get_job_state_kwargs(job_id: int, dbh: DBHandler = Depends(db_handler)):
    dbh.set_job_id(job_id)

    state_kwargs = dbh.get_state_kwargs()
    i_state_kwargs = [rh.to_interface(skw) for skw in state_kwargs]

    return i_state_kwargs


@app.get("/api/jobs/{job_id}/count_pending_tasks_below_level")
async def get_count_pending_tasks_below_level(job_id: int, level: int, dbh: DBHandler = Depends(db_handler)):
    dbh.set_job_id(job_id)

    count = dbh.count_pending_tasks_below_level(level)

    return {
        'count': count,
    }
