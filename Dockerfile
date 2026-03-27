# Expose Airflow webserver port for Render
EXPOSE 8080
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

ENV PYTHONPATH="/opt/airflow/airflow-home:${PYTHONPATH}"
ENV AIRFLOW_HOME="/opt/airflow"

# Default command to run FastAPI app
CMD ["python", "-m", "uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "10000"]
