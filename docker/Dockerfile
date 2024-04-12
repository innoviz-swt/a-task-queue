FROM python:3.8-alpine

ENV ATASKQ_SERVER_PORT 8080
ENV ATASKQ_SERVER_HOST 0.0.0.0
ENV ATASKQ_SERVER_WORKERS 4
WORKDIR /app
ADD . .
RUN pip install --upgrade pip && pip install -r requirements.txt
CMD python -m uvicorn ataskq.server.server:app --port $ATASKQ_SERVER_PORT --host $ATASKQ_SERVER_HOST --workers $ATASKQ_SERVER_WORKERS
