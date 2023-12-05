import os
import logging
from pathlib import Path

from fastapi import FastAPI, HTTPException, status, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from ataskq.db_handler import from_connection_str

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


@app.post("/api/jobs")
async def create_job():
    hanlder = from_connection_str(CONNECTION)
    job_id = hanlder.create_job().job_id

    return {"job_id": job_id}


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: int):
    return {"job_id": job_id}
