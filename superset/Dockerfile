ARG SUPERSET_VERSION=4.1.1-py310

FROM apache/superset:${SUPERSET_VERSION}

USER root

RUN export DEBIAN_FRONTEND=noninteractive
RUN apt-get update -yq && \
    apt-get install -yq --no-install-recommends default-libmysqlclient-dev pkg-config gcc && \
    rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    # prevents python creating .pyc files
    PYTHONDONTWRITEBYTECODE=1 \
    # pip
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100

RUN pip3 install --no-cache-dir psycopg2-binary \
    starrocks==1.0.6 \
    Pillow \
    mysqlclient \
    mysql-connector-python

USER superset
