import os
import logging
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from ataskq.handler import from_connection_str
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


@app.get("/api")
async def root():
    return {"message": "Welcome to A-TASK-Q Server"}

# create or update

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
