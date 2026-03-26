# This script sets up the environment and runs Airflow with the correct PYTHONPATH and .env loading

import os
import sys
from dotenv import load_dotenv

# Set the project root and airflow-home directory
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
AIRFLOW_HOME = os.path.join(PROJECT_ROOT, 'airflow-home')

# Load .env file
load_dotenv(os.path.join(AIRFLOW_HOME, '.env'))

# Add airflow-home to PYTHONPATH
if AIRFLOW_HOME not in sys.path:
    sys.path.insert(0, AIRFLOW_HOME)

# Run Airflow scheduler and webserver
os.system('airflow db upgrade')
os.system('airflow scheduler &')
os.system('airflow webserver &')
