import os
import logging
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from ataskq.handler import from_connection_str

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


#########
# Tasks #
#########
@app.post("/api/tasks")
async def get_job(request: Request):
    data = await request.form()
    res = []
    for k, v in data.items():
        from starlette.datastructures import UploadFile
        import pickle
        if isinstance(v, UploadFile):
            v = await v.read()
            v = pickle.loads(v)
        logger.info(f'{k}: {v}')
        # expect format of index.key
        assert '.' in k
        i, *k = k.split('.')
        i = int(i)
        k = '.'.join(k)
        # expect monotonic rising items
        if i == len(res):
            res += [dict()]
        elif i == len(res) - 1:
            pass
        else:
            # should never get here
            raise RuntimeError(f"unexpected item index '{i}'. len res: {len(res)}")
        res[i][k] = v

    logger.info(res)
    return {}
