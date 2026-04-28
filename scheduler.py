"""
Job Pipeline Scheduler
Replaces Airflow DAGs with a lightweight APScheduler-based scheduler.
Runs the same scraping pipeline on the same schedules:
  - Daily full scrape at 2:00 PM EAT (all sources)
  - Quick scrape every 6 hours (top sources only)
  - Deactivate old jobs daily
"""
import sys
import os
import logging
from datetime import datetime, timedelta

# Ensure project root is on the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("scheduler")

# EAT timezone (UTC+3)
EAT = "Africa/Nairobi"


def daily_full_scrape():
    """Run all scrapers - mirrors the daily_scrape_dag."""
    logger.info("=" * 60)
    logger.info("DAILY FULL SCRAPE STARTED")
    logger.info("=" * 60)

    from airflow_home.database.connection import init_db
    from airflow_home.scrapers.runner import run_scraper

    init_db()

    sources_config = [
        {"source": "linkedin", "search_query": "software developer", "location": "Kenya", "max_pages": 3},
        {"source": "myjobsinkenya", "max_pages": 5},
        {"source": "brightermonday", "max_pages": 5},
        {"source": "indeed", "search_query": "", "location": "Kenya", "max_pages": 3},
        {"source": "glassdoor", "search_query": "software", "location": "Kenya", "max_pages": 2},
        {"source": "fuzu", "max_pages": 5},
        {"source": "google_search", "search_query": "jobs hiring", "location": "Kenya", "max_pages": 2},
        {"source": "adzuna", "search_query": "software", "location": "Kenya", "max_pages": 3},
        {"source": "jobwebkenya", "max_pages": 5},
        {"source": "corporatestaffing", "max_pages": 5},
        {"source": "kenyajob", "max_pages": 5},
        {"source": "summitrecruitment", "max_pages": 3},
    ]

    results = []
    for config in sources_config:
        source = config.pop("source")
        try:
            result = run_scraper(source, **config)
            results.append(result)
            logger.info(f"  {source}: {result.get('status')} - found={result.get('jobs_found', 0)}, new={result.get('jobs_new', 0)}")
        except Exception as e:
            logger.error(f"  {source}: FAILED - {e}")

    deactivate_old_jobs()
    log_summary()

    logger.info("DAILY FULL SCRAPE COMPLETED")
    return results


def quick_scrape():
    """Quick scrape of top sources - mirrors the quick_scrape_dag."""
    logger.info("=" * 60)
    logger.info("QUICK SCRAPE STARTED")
    logger.info("=" * 60)

    from airflow_home.scrapers.runner import run_all_scrapers

    results = run_all_scrapers(
        search_query="developer",
        location="Kenya",
        max_pages=2,
        sources=["linkedin", "brightermonday", "indeed"],
    )
    for r in results:
        logger.info(f"  {r}")

    logger.info("QUICK SCRAPE COMPLETED")
    return results


def deactivate_old_jobs():
    """Mark jobs older than 30 days as inactive."""
    from airflow_home.database.connection import SessionLocal
    from airflow_home.database.models import Job

    db = SessionLocal()
    try:
        cutoff = datetime.now() - timedelta(days=30)
        updated = (
            db.query(Job)
            .filter(Job.scraped_at < cutoff, Job.is_active == True)
            .update({"is_active": False})
        )
        db.commit()
        logger.info(f"Deactivated {updated} old jobs")
    finally:
        db.close()


def log_summary():
    """Log a summary of recent scraping."""
    from airflow_home.database.connection import SessionLocal
    from airflow_home.database.models import ScrapeLog

    db = SessionLocal()
    try:
        since = datetime.now() - timedelta(hours=1)
        logs = db.query(ScrapeLog).filter(ScrapeLog.started_at >= since).all()
        logger.info("--- Scrape Summary ---")
        for log in logs:
            logger.info(f"  {log.source}: {log.status} - found={log.jobs_found}, new={log.jobs_new}")
        logger.info("----------------------")
    finally:
        db.close()


def main():
    logger.info("Starting Job Pipeline Scheduler")
    logger.info(f"Daily full scrape: 2:00 PM EAT (daily)")
    logger.info(f"Quick scrape: every 6 hours")

    scheduler = BlockingScheduler()

    # Daily full scrape at 2:00 PM EAT
    scheduler.add_job(
        daily_full_scrape,
        CronTrigger(hour=14, minute=0, timezone=EAT),
        id="daily_full_scrape",
        name="Daily Full Scrape (2PM EAT)",
        misfire_grace_time=3600,
    )

    # Quick scrape every 6 hours
    scheduler.add_job(
        quick_scrape,
        CronTrigger(hour="*/6", minute=0, timezone=EAT),
        id="quick_scrape",
        name="Quick Scrape (every 6h)",
        misfire_grace_time=1800,
    )

    logger.info("Scheduler started. Press Ctrl+C to stop.")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")


if __name__ == "__main__":
    main()
