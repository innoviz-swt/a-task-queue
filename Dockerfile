FROM python:3.12-alpine

RUN apk --no-cache add curl

WORKDIR /app

COPY dist dist
COPY requirements.txt requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt
RUN pip install dist/*.whl
RUN rm -r dist requirements.txt
COPY --chmod=+x scripts/run* .

CMD [ "/bin/sh" ]
