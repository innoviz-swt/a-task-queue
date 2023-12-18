import os
import logging
from typing import Union
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles


from ataskq.handler import from_connection_str
from ataskq.db_handler import DBHandler
from ataskq.models import Task
from .form_utils import form_data_array

app = FastAPI()
logger = logging.getLogger("uvicorn")
CONNECTION = os.getenv('ATASKQ_CONNECTION', "sqlite://ataskq.db.sqlite3")

# allow all cors
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# static folder
app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")


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
def update_task(task_id: int, **kwargs):
    hanlder: DBHandler = from_connection_str(CONNECTION)
    hanlder.update_task(task_id, kwargs)

    return {task_id: task_id}


########
# JOBS #
########
@app.post("/api/jobs")
async def create_job():
    hanlder = from_connection_str(CONNECTION)
    job_id = hanlder.create_job().job_id

    return {"job_id": job_id}


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: int):
    return {"job_id": job_id}


@app.post("/api/jobs/{job_id}/tasks")
async def get_job(job_id: int, request: Request):
    # get hanlder
    hanlder = from_connection_str(CONNECTION, job_id=job_id)

    # get form data
    data = await request.form()
    data = await form_data_array(data)

    # parse
    tasks = [Task(**t) for t in data]
    hanlder.add_tasks(tasks)

    return {}


@app.get("/api/jobs/{job_id}/state_kwargs")
async def get_job(job_id: int):
    # get hanlder
    hanlder = from_connection_str(CONNECTION, job_id=job_id)

    ret = hanlder.get_state_kwargs()
    ret = [ret.__dict__ for r in ret]

    return ret


@app.get("/api/jobs/{job_id}/next_task")
async def next_task(job_id: int, level_start: Union[int, None] = None, level_stop: Union[int, None] = None):
    # get level range
    if level_start is not None and level_stop is not None:
        level = range(level_start, level_stop)
    elif level_start is not None:
        level = range(level_start, level_start + 1)
    else:
        level = None

    # get hanlder
    hanlder: DBHandler = from_connection_str(CONNECTION, job_id=job_id)

    # take next task
    action, task = hanlder._take_next_task(level)
    task = task.__dict__ if task is not None else None

    return dict(action=action, task=task)
