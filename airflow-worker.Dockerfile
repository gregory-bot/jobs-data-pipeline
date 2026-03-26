# Airflow Dockerfile for Render background worker
FROM apache/airflow:2.8.2-python3.11

# Set Airflow home and working directory
ENV AIRFLOW_HOME=/opt/airflow
WORKDIR /opt/airflow

# Copy your Airflow DAGs and config
COPY airflow-home/dags ./dags
COPY airflow-home/config ./config
COPY airflow-home/database ./database
COPY airflow-home/airflow.cfg ./airflow.cfg

# (Optional) Copy any custom plugins or scripts
# COPY airflow-home/plugins ./plugins

# Install any extra Python dependencies (if needed)
# COPY requirements.txt ./
# RUN pip install --no-cache-dir -r requirements.txt

# Set environment variables for Render
ENV PYTHONPATH="/opt/airflow:/opt/airflow/database:${PYTHONPATH}"

# Default command: run the Airflow scheduler (background worker)
CMD ["airflow", "scheduler"]
