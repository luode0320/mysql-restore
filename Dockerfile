FROM python:3.12-slim

ARG APP_VERSION=dev
ENV APP_VERSION=$APP_VERSION

RUN apt-get update \
  && apt-get install -y --no-install-recommends default-mysql-client \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY index.html /app/index.html
COPY server.py /app/server.py
RUN mkdir -p /usr/local/src/restoredb

ENTRYPOINT ["python", "/app/server.py"]
