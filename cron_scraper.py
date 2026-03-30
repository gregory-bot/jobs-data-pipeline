#!/usr/bin/env python
"""
Standalone cron job that runs daily at 2 PM EAT
Scrapes jobs, updates database with deduplication
"""
import os
import sys
import logging
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from airflow_home.database.connection import SessionLocal, engine
from airflow_home.database.models import Base, Job, ScrapeLog
from airflow_home.scrapers.runner import run_all_scrapers

def init_database():
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables verified/created")
    except Exception as e:
        logger.error(f"Database init failed: {e}")
        raise

def is_duplicate_job(db, job_url):
    if not job_url:
        return False
    return db.query(Job).filter(Job.url == job_url).first() is not None

def save_jobs_to_db(jobs, source):
    db = SessionLocal()
    new_jobs = 0
    skipped_jobs = 0
    updated_jobs = 0
    try:
        for job in jobs:
            existing_job = db.query(Job).filter(Job.url == job.get('url')).first()
            if existing_job:
                if existing_job.title != job.get('title') or \
                   existing_job.company != job.get('company'):
                    existing_job.title = job.get('title')
                    existing_job.company = job.get('company')
                    existing_job.scraped_at = datetime.now(timezone.utc)
                    updated_jobs += 1
                    logger.debug(f"Updated job: {job.get('title')} at {job.get('company')}")
                else:
                    skipped_jobs += 1
                    logger.debug(f"Skipped duplicate: {job.get('title')}")
            else:
                new_job = Job(
                    title=job.get('title'),
                    company=job.get('company'),
                    location=job.get('location'),
                    url=job.get('url'),
                    salary=job.get('salary'),
                    description=job.get('description'),
                    posted_date=job.get('posted_date'),
                    source=source,
                    is_active=True,
                    scraped_at=datetime.now(timezone.utc)
                )
                db.add(new_job)
                new_jobs += 1
                logger.info(f"New job: {job.get('title')} at {job.get('company')}")
        db.commit()
        logger.info(f"[{source}] New: {new_jobs}, Updated: {updated_jobs}, Skipped: {skipped_jobs}")
        return {
            "new": new_jobs,
            "updated": updated_jobs,
            "skipped": skipped_jobs,
            "total": len(jobs)
        }
    except Exception as e:
        logger.error(f"Error saving jobs: {e}")
        db.rollback()
        raise
    finally:
        db.close()

def deactivate_old_jobs():
    db = SessionLocal()
    try:
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=30)
        updated = db.query(Job).filter(
            Job.scraped_at < cutoff_date,
            Job.is_active == True
        ).update({"is_active": False})
        db.commit()
        logger.info(f"Deactivated {updated} jobs older than 30 days")
        return updated
    except Exception as e:
        logger.error(f"Error deactivating old jobs: {e}")
        db.rollback()
        raise
    finally:
        db.close()

def log_scrape_result(source, status, jobs_found, new_jobs, error=None):
    db = SessionLocal()
    try:
        log = ScrapeLog(
            source=source,
            status=status,
            jobs_found=jobs_found,
            jobs_new=new_jobs,
            error_message=error,
            started_at=datetime.now(timezone.utc),
            finished_at=datetime.now(timezone.utc)
        )
        db.add(log)
        db.commit()
        logger.info(f"Logged scrape result for {source}: {status}")
    except Exception as e:
        logger.error(f"Error logging scrape: {e}")
        db.rollback()
    finally:
        db.close()

def run_full_scrape():
    logger.info("="*50)
    logger.info("Starting full job scrape")
    logger.info("="*50)
    init_database()
    all_results = {}
    total_new = 0
    total_found = 0
    try:
        from airflow_home.scrapers.runner import run_all_scrapers as scrapers
        scraped_data = scrapers()
        for result in scraped_data:
            source = result.get('source')
            jobs = result.get('jobs') or result.get('jobs_cleaned') or []
            if jobs and isinstance(jobs, list):
                total_found += len(jobs)
                db_result = save_jobs_to_db(jobs, source)
                total_new += db_result.get('new', 0)
                all_results[source] = db_result
                log_scrape_result(source, "success", len(jobs), db_result.get('new', 0))
            else:
                logger.warning(f"No data from {source}")
                log_scrape_result(source, "empty", 0, 0)
        deactivated = deactivate_old_jobs()
        logger.info("="*50)
        logger.info("SCRAPE SUMMARY")
        logger.info("="*50)
        logger.info(f"Total jobs found: {total_found}")
        logger.info(f"New jobs added: {total_new}")
        logger.info(f"Jobs deactivated: {deactivated}")
        logger.info("="*50)
        return {
            "success": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total_found": total_found,
            "total_new": total_new,
            "deactivated": deactivated,
            "details": all_results
        }
    except Exception as e:
        logger.error(f"Full scrape failed: {e}")
        return {
            "success": False,
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

def run_quick_scrape():
    logger.info("Starting quick scrape (LinkedIn, BrighterMonday, Indeed)")
    init_database()
    quick_sources = ["linkedin", "brightermonday", "indeed"]
    results = {}
    total_new = 0
    try:
        from airflow_home.scrapers.runner import run_scraper
        for source in quick_sources:
            try:
                jobs = run_scraper(source)
                if jobs:
                    result = save_jobs_to_db(jobs, source)
                    total_new += result.get('new', 0)
                    results[source] = result
                    log_scrape_result(source, "success", len(jobs), result.get('new', 0))
                else:
                    results[source] = {"error": "No jobs found"}
                    log_scrape_result(source, "empty", 0, 0)
            except Exception as e:
                logger.error(f"Quick scrape failed for {source}: {e}")
                results[source] = {"error": str(e)}
                log_scrape_result(source, "failed", 0, 0, str(e))
        logger.info(f"Quick scrape complete. New jobs: {total_new}")
        return {
            "success": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total_new": total_new,
            "details": results
        }
    except Exception as e:
        logger.error(f"Quick scrape failed: {e}")
        return {"success": False, "error": str(e)}

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Job Scraper Cron")
    parser.add_argument("--quick", action="store_true", help="Run quick scrape (top 3 sources)")
    parser.add_argument("--full", action="store_true", help="Run full scrape (all sources)")
    args = parser.parse_args()
    if args.quick:
        result = run_quick_scrape()
    elif args.full:
        result = run_full_scrape()
    else:
        result = run_full_scrape()
    sys.exit(0 if result.get('success') else 1)
