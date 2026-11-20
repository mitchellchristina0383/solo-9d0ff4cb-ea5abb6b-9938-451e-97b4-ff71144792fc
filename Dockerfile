# ---------------------------------------------------------------------------
# Block-volume API service.  SQLite is the only persistence layer; the API and
# the database live on the same volume so a single container is sufficient.
# ---------------------------------------------------------------------------
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    BLOCKSTORE_DB=/data/blockstore.db

WORKDIR /srv

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# The database directory is owned by an unprivileged user.  The named volume
# in docker-compose.yml is mounted here; SQLite needs a writable directory for
# its -wal and -shm files.
RUN mkdir -p /data && useradd --system --uid 10001 --home-dir /srv blockstore \
    && chown -R blockstore:blockstore /data /srv
USER blockstore

EXPOSE 8000
VOLUME ["/data"]

HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=10 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=2).status == 200 else 1)"

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
