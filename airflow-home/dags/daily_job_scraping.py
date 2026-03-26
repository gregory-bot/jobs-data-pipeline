"""
Airflow DAG: Daily Job Scraping Pipeline
Scheduled to run at 2:00 PM EAT (11:00 AM UTC) daily.
"""
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator

# ============================================
# CRITICAL: Ensure project root is in sys.path
# ============================================
# Get absolute path to the directory containing this DAG file
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
# Project root is one level up from DAG directory
PROJECT_ROOT = os.path.dirname(DAG_DIR)

# Add project root to sys.path so Python can find our modules
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Also add the DAG directory itself in case modules are there
if DAG_DIR not in sys.path:
    sys.path.insert(0, DAG_DIR)

# ============================================
# DAG Configuration
# ============================================
default_args = {
    "owner": "jobs-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "daily_job_scraping",
    default_args=default_args,
    description="Scrape jobs from all sources daily at 2 PM EAT",
    schedule="0 11 * * *",  # 2 PM EAT = 11 AM UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["jobs", "scraping", "pipeline"],
)

# ============================================
# Task Functions
# ============================================
def init_database():
    try:
        from database.connection import init_db
        init_db()
        print("[Database] Initialization complete")
    except Exception as e:
        print(f"[Database] Error during init: {e}")
        raise

def scrape_linkedin(**kwargs):
    from scrapers.runner import run_scraper
    return run_scraper("linkedin", search_query="software developer", location="Kenya", max_pages=3)

def scrape_myjobsinkenya(**kwargs):
    from scrapers.runner import run_scraper
    return run_scraper("myjobsinkenya", max_pages=5)

def scrape_brightermonday(**kwargs):
    from scrapers.runner import run_scraper
    return run_scraper("brightermonday", max_pages=5)

def scrape_indeed(**kwargs):
    from scrapers.runner import run_scraper
    return run_scraper("indeed", search_query="", location="Kenya", max_pages=3)

def scrape_glassdoor(**kwargs):
    from scrapers.runner import run_scraper
    return run_scraper("glassdoor", search_query="software", location="Kenya", max_pages=2)

def scrape_fuzu(**kwargs):
    from scrapers.runner import run_scraper
    return run_scraper("fuzu", max_pages=5)

def scrape_google_search(**kwargs):
    from scrapers.runner import run_scraper
    return run_scraper("google_search", search_query="jobs hiring", location="Kenya", max_pages=2)

def scrape_adzuna(**kwargs):
    from scrapers.runner import run_scraper
    return run_scraper("adzuna", search_query="software", location="Kenya", max_pages=3)

def deactivate_old_jobs(**kwargs):
    from database.connection import SessionLocal
    from database.models import Job
    db = SessionLocal()
    try:
        cutoff = datetime.utcnow() - timedelta(days=30)
        updated = (
            db.query(Job)
            .filter(Job.scraped_at < cutoff, Job.is_active == True)
            .update({"is_active": False})
        )
        db.commit()
        print(f"[Jobs] Deactivated {updated} old jobs")
        return updated
    except Exception as e:
        print(f"[Jobs] Error deactivating: {e}")
        db.rollback()
        raise
    finally:
        db.close()

def log_summary(**kwargs):
    from database.connection import SessionLocal
    from database.models import ScrapeLog
    db = SessionLocal()
    try:
        today = datetime.utcnow() - timedelta(hours=1)
        logs = db.query(ScrapeLog).filter(ScrapeLog.started_at >= today).all()
        print("\n" + "="*50)
        print("SCRAPING SUMMARY")
        print("="*50)
        total_found = 0
        total_new = 0
        for log in logs:
            print(f"  {log.source}: {log.status} - found={log.jobs_found}, new={log.jobs_new}")
            total_found += log.jobs_found or 0
            total_new += log.jobs_new or 0
        print("-"*50)
        print(f"  TOTAL: found={total_found}, new={total_new}")
        print("="*50)
        kwargs['ti'].xcom_push(key='summary_stats', value={
            'total_found': total_found,
            'total_new': total_new,
            'logs_count': len(logs)
        })
    except Exception as e:
        print(f"[Summary] Error: {e}")
        raise
    finally:
        db.close()

# ============================================
# Task Definitions
# ============================================
t_init = PythonOperator(
    task_id="init_database",
    python_callable=init_database,
    dag=dag
)

t_linkedin = PythonOperator(
    task_id="scrape_linkedin",
    python_callable=scrape_linkedin,
    dag=dag
)

t_myjobs = PythonOperator(
    task_id="scrape_myjobsinkenya",
    python_callable=scrape_myjobsinkenya,
    dag=dag
)

t_brighter = PythonOperator(
    task_id="scrape_brightermonday",
    python_callable=scrape_brightermonday,
    dag=dag
)

t_indeed = PythonOperator(
    task_id="scrape_indeed",
    python_callable=scrape_indeed,
    dag=dag
)

t_glassdoor = PythonOperator(
    task_id="scrape_glassdoor",
    python_callable=scrape_glassdoor,
    dag=dag
)

t_fuzu = PythonOperator(
    task_id="scrape_fuzu",
    python_callable=scrape_fuzu,
    dag=dag
)

t_google = PythonOperator(
    task_id="scrape_google_search",
    python_callable=scrape_google_search,
    dag=dag
)

t_adzuna = PythonOperator(
    task_id="scrape_adzuna",
    python_callable=scrape_adzuna,
    dag=dag
)

t_deactivate = PythonOperator(
    task_id="deactivate_old_jobs",
    python_callable=deactivate_old_jobs,
    dag=dag
)

t_summary = PythonOperator(
    task_id="log_summary",
    python_callable=log_summary,
    provide_context=True,
    dag=dag
)

send_email = EmailOperator(
    task_id="send_summary_email",
    to="kipngenogregory@gmail.com",
    subject="[Job Scraper] Daily Summary Report - {{ ds }}",
    html_content="""
    <h2>Daily Job Scraping Summary</h2>
    <p><strong>Date:</strong> {{ ds }}</p>
    <p>The daily scraping pipeline has completed successfully.</p>
    <p>Please check the Airflow logs for detailed results.</p>
    <hr>
    <p>Best regards,<br>Job Scraper Pipeline</p>
    """,
    dag=dag,
)

# ============================================
# DAG Dependencies
# ============================================
scrapers = [
    t_linkedin, t_myjobs, t_brighter, t_indeed,
    t_glassdoor, t_fuzu, t_google, t_adzuna
]

t_init >> scrapers >> t_deactivate >> t_summary >> send_email
