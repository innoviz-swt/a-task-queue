FROM python:3.8-alpine

ENV ATASKQ_SERVER_PORT 8080
ENV ATASKQ_SERVER_HOST 0.0.0.0
ENV ATASKQ_SERVER_WORKERS 4
ENV ATASKQ_DB_INIT_ON_HANDLER_INIT 0
# ENV ATASKQ_SERVER_CONNECTION=pg://postgres:postgres@localhost:5432/postgres

WORKDIR /app

ADD dist dist
ADD requirements.txt requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt
RUN pip install dist/*.whl
CMD python -m uvicorn ataskq.server.server:app --port $ATASKQ_SERVER_PORT --host $ATASKQ_SERVER_HOST --workers $ATASKQ_SERVER_WORKERS
