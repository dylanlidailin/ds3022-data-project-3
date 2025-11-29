# Use the same base image as the repo
FROM python:3.12.5-slim-bookworm

# Set env vars to keep Python happy in Docker
ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=UTF-8 \
    PYTHONPATH="/app"

WORKDIR /app

# Install requirements (cached layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Default command: Run the producer
ENTRYPOINT ["python3", "producer.py"]