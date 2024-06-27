FROM python:3.12-bookworm

# setup os env
RUN apt-get upgrade -y
RUN apt-get update && DEBIAN_FRONTEND="noninteractive" apt-get install -y --no-install-recommends \
    # essentials
    curl \
    vim \
    # cleanup
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# setup python env
COPY requirements.txt requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt && \
    rm -r requirements.txt

# setup code
COPY dist dist
RUN pip install dist/*.whl && \
    rm -r dist

# setup run files
COPY --chmod=+x scripts/run* .

CMD [ "/bin/sh" ]
