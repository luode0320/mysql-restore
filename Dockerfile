FROM python:3.12-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends default-mysql-client \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY index.html /app/index.html
COPY server.py /app/server.py
COPY scripts/ /app/scripts/
RUN mkdir -p /usr/local/src/restoredb /usr/local/src/nginx/public/restoredb \
  && chmod +x /app/scripts/*.sh

ENTRYPOINT ["python", "/app/server.py"]
