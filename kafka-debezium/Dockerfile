FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Same pins as the root requirements.txt (kafka-debezium section)
RUN pip install python-dotenv==1.2.1 requests==2.32.5

# Only the script: credentials come from kafka-debezium/.env at runtime, never baked into the image.
COPY register_connector.py .

RUN useradd --create-home --uid 10001 register
USER register

CMD ["python", "register_connector.py"]
