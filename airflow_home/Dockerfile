# Airflow Dockerfile for Render background worker
FROM apache/airflow:2.8.2-python3.11

ENV AIRFLOW_HOME=/opt/airflow
WORKDIR /opt/airflow

# Copy Airflow DAGs and config (relative to this Dockerfile)
COPY dags ./dags
COPY config ./config
COPY database ./database
COPY airflow.cfg ./airflow.cfg

# (Optional) Copy any custom plugins or scripts
# COPY plugins ./plugins

# Set environment variables for Render
ENV PYTHONPATH="/opt/airflow:/opt/airflow/database:${PYTHONPATH}"

CMD ["airflow", "scheduler"]
