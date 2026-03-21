"""
Airflow DAG: Daily Job Scraping Pipeline
Scheduled to run at 2:00 PM EAT (11:00 AM UTC) daily.
"""
import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator

# Add project root to path so we can import our modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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
    # 2 PM EAT = 11 AM UTC
    schedule="0 11 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["jobs", "scraping", "pipeline"],
)


def init_database():
    """Initialize database tables if they don't exist."""
    from database.connection import init_db
    init_db()


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
    """Mark jobs older than 30 days as inactive."""
    from database.connection import SessionLocal
    from database.models import Job
    from datetime import datetime, timedelta

    db = SessionLocal()
    try:
        cutoff = datetime.utcnow() - timedelta(days=30)
        updated = (
            db.query(Job)
            .filter(Job.scraped_at < cutoff, Job.is_active == True)
            .update({"is_active": False})
        )
        db.commit()
        print(f"Deactivated {updated} old jobs")
    finally:
        db.close()


def log_summary(**kwargs):
    """Log a summary of today's scraping."""
    from database.connection import SessionLocal
    from database.models import ScrapeLog
    from datetime import datetime, timedelta

    db = SessionLocal()
    try:
        today = datetime.utcnow() - timedelta(hours=1)
        logs = db.query(ScrapeLog).filter(ScrapeLog.started_at >= today).all()
        for log in logs:
            print(f"  {log.source}: {log.status} - found={log.jobs_found}, new={log.jobs_new}")
    finally:
        db.close()


# Tasks
t_init = PythonOperator(task_id="init_database", python_callable=init_database, dag=dag)

t_linkedin = PythonOperator(task_id="scrape_linkedin", python_callable=scrape_linkedin, dag=dag)
t_myjobs = PythonOperator(task_id="scrape_myjobsinkenya", python_callable=scrape_myjobsinkenya, dag=dag)
t_brighter = PythonOperator(task_id="scrape_brightermonday", python_callable=scrape_brightermonday, dag=dag)
t_indeed = PythonOperator(task_id="scrape_indeed", python_callable=scrape_indeed, dag=dag)
t_glassdoor = PythonOperator(task_id="scrape_glassdoor", python_callable=scrape_glassdoor, dag=dag)
t_fuzu = PythonOperator(task_id="scrape_fuzu", python_callable=scrape_fuzu, dag=dag)
t_google = PythonOperator(task_id="scrape_google_search", python_callable=scrape_google_search, dag=dag)
t_adzuna = PythonOperator(task_id="scrape_adzuna", python_callable=scrape_adzuna, dag=dag)

t_deactivate = PythonOperator(task_id="deactivate_old_jobs", python_callable=deactivate_old_jobs, dag=dag)

t_summary = PythonOperator(task_id="log_summary", python_callable=log_summary, dag=dag)

# EmailOperator to send summary after scraping
send_email = EmailOperator(
    task_id="send_summary_email",
    to="kipngenogregory@gmail.com",
    subject="[Job Scraper] Daily Summary Report",
    html_content="""
    <h2>Daily Job Scraping Summary</h2>
    <p>The daily scraping pipeline has completed. Please check the Airflow logs for detailed results and job counts.</p>
    <p>Best regards,<br>Job Scraper Pipeline</p>
    """,
    dag=dag,
)

# DAG structure: init -> all scrapers in parallel -> deactivate old -> summary -> email
scrapers = [t_linkedin, t_myjobs, t_brighter, t_indeed, t_glassdoor, t_fuzu, t_google, t_adzuna]

t_init >> scrapers >> t_deactivate >> t_summary >> send_email
