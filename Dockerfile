FROM python:3.12-alpine

ENV ATASKQ_PORT 8080
ENV ATASKQ_HOST 0.0.0.0
ENV ATASKQ_SERVER_WORKERS 4

WORKDIR /app

COPY dist dist
COPY requirements.txt requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt
RUN pip install dist/*.whl
RUN rm -r dist requirements.txt
COPY --chmod=+x scripts/run* .

CMD [ "/bin/sh" ]
