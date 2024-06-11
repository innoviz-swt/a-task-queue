import logging
import asyncio
from pathlib import Path

from fastapi import FastAPI, Request, Depends
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles


from ataskq.handler import DBHandler, from_config
from ataskq.handler.rest_handler import RESTHandler as rh
from ataskq.models import Model, __MODELS__
from ataskq.env import ATASKQ_SERVER_CONFIG

# from .form_utils import form_data_array

logger = logging.getLogger("uvicorn")


# DB Handler
def db_handler() -> DBHandler:
    return from_config(ATASKQ_SERVER_CONFIG or "server")


app = FastAPI()


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
app.mount("/www", StaticFiles(directory=Path(__file__).parent / "www"), name="www")


@app.get("/")
async def root():
    # return "Welcome to A-TASK-Q Server"
    return RedirectResponse("/custom_query/jobs_status")


@app.get("/health")
async def health():
    return "Server is running"


@app.get("/api")
async def api():
    return {"message": "Welcome to A-TASK-Q Server API"}


####################
# Custom Query API #
####################
@app.get("/api/custom_query/take_next_task")
async def take_next_task(
    request: Request,
    dbh: DBHandler = Depends(db_handler),
):
    # take next task
    action, task = dbh.take_next_task(**request.query_params)
    task = rh.to_interface(task) if task is not None else None

    return dict(action=action, task=task)


@app.get("/api/custom_query/jobs_status")
async def jobs_status(request: Request, dbh: DBHandler = Depends(db_handler)):
    ret = dbh.jobs_status(**request.query_params)

    return ret


@app.get("/api/custom_query/tasks_status")
async def tasks_status(request: Request, dbh: DBHandler = Depends(db_handler)):
    ret = dbh.tasks_status(**request.query_params)

    return ret


#############
# Model API #
#############
@app.get("/api/{model}")
async def get_model_all(model: str, request: Request, dbh: DBHandler = Depends(db_handler)):
    # logger.info(f"query_params: {request.query_params}")
    model_cls = __MODELS__[model]
    mkwargs = model_cls.get_all_dict(_handler=dbh, **request.query_params)
    ikwargs = rh.m2i(model_cls, mkwargs)

    return ikwargs


@app.get("/api/{model}/count")
async def count_model_all(model: str, request: Request, dbh: DBHandler = Depends(db_handler)):
    model_cls = __MODELS__[model]
    count = model_cls.count_all(_handler=dbh, **request.query_params)

    return count


@app.get("/api/{model}/{model_id}")
async def get_model(model: str, model_id: int, dbh: DBHandler = Depends(db_handler)):
    model_cls = __MODELS__[model]
    mkwargs = model_cls.get_dict(model_id, _handler=dbh)
    ikwargs = rh.m2i(model_cls, mkwargs)

    return ikwargs


@app.post("/api/{model}")
async def create_model(model: str, request: Request, dbh: DBHandler = Depends(db_handler)):
    model_cls: Model = __MODELS__[model]
    ikwargs = await request.json()
    mkwargs = rh.i2m(model_cls, ikwargs)
    model_id = dbh.create(model_cls, **mkwargs)

    return model_id


@app.post("/api/{model}/bulk")
async def create_model_bulk(model: str, request: Request, dbh: DBHandler = Depends(db_handler)):
    model_cls: Model = __MODELS__[model]
    ikwargs = await request.json()
    mkwargs = rh.i2m(model_cls, ikwargs)
    model_ids = dbh.create_bulk(model_cls, mkwargs)

    return model_ids


@app.put("/api/{model}/{model_id}")
async def update_model(model: str, model_id: int, request: Request, dbh: DBHandler = Depends(db_handler)):
    model_cls: Model = __MODELS__[model]
    ikwargs = await request.json()
    mkwargs = rh.i2m(model_cls, ikwargs)
    dbh.update(model_cls, model_id, **mkwargs)

    return {model_cls.id_key(): model_id}


@app.delete("/api/{model}/{model_id}")
async def delete_model(model: str, model_id: int, dbh: DBHandler = Depends(db_handler)):
    model_cls: Model = __MODELS__[model]
    dbh.delete(model_cls, model_id)

    return {model_cls.id_key(): model_id}


#######
# WWW #
#######
@app.get("/db/{model}")
async def show_db(model: str):
    return FileResponse(Path(__file__).parent / "www" / "index.html")


@app.get("/custom_query/{query}")
async def show_custom_qeury(query: str):
    return FileResponse(Path(__file__).parent / "www" / "index.html")


@app.get("/custom_query/{query}/{job_id}")
async def show_job_custom_query(query: str, job_id: int):
    return FileResponse(Path(__file__).parent / "www" / "index.html")
