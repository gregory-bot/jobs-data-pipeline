FROM python:3.11-slim

WORKDIR /opt/airflow

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

ENV PYTHONPATH="/opt/airflow:${PYTHONPATH}"
ENV AIRFLOW_HOME="/opt/airflow"
