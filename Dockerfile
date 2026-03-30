
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

ENV PYTHONPATH="/opt/airflow/airflow_home:${PYTHONPATH}"
ENV AIRFLOW_HOME="/opt/airflow"


# Expose Airflow webserver port for Render
EXPOSE 8080

# Default command to run Airflow webserver
# (You may need to override this in Render to: airflow webserver)
CMD ["python", "-m", "uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "10000"]
