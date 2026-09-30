FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY kafka_to_minio.py .

# Non-root; SIGTERM from `docker stop` flushes buffered events before exit.
RUN useradd --create-home --uid 10001 consumer
USER consumer

CMD ["python", "kafka_to_minio.py"]
