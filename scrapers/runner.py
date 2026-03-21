"""
Scraper Runner - Orchestrates all scrapers, cleans data, and stores in DB.
This is the main entry point for DAGs and manual runs.
"""
import logging
import datetime
from typing import Optional

from sqlalchemy.dialects.postgresql import insert as pg_insert

from database.connection import SessionLocal, init_db
from database.models import Job, ScrapeLog
from scrapers.linkedin_scraper import LinkedInScraper
from scrapers.myjobsinkenya_scraper import MyJobsInKenyaScraper
from scrapers.brightermonday_scraper import BrighterMondayScraper
from scrapers.indeed_scraper import IndeedScraper
from scrapers.glassdoor_scraper import GlassdoorScraper
from scrapers.fuzu_scraper import FuzuScraper
from scrapers.google_search_scraper import GoogleJobsSearchScraper
from scrapers.adzuna_api_scraper import AdzunaAPIScraper
from transformers.cleaner import clean_jobs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Registry of all available scrapers
SCRAPER_REGISTRY = {
    "linkedin": LinkedInScraper,
    "myjobsinkenya": MyJobsInKenyaScraper,
    "brightermonday": BrighterMondayScraper,
    "indeed": IndeedScraper,
    "glassdoor": GlassdoorScraper,
    "fuzu": FuzuScraper,
    "google_search": GoogleJobsSearchScraper,
    "adzuna": AdzunaAPIScraper,
}


def run_scraper(
    source: str,
    search_query: str = None,
    location: str = None,
    max_pages: int = 5,
) -> dict:
    """
    Run a single scraper, clean data, and store in DB.
    Returns a summary dict.
    """
    if source not in SCRAPER_REGISTRY:
        raise ValueError(f"Unknown source: {source}. Available: {list(SCRAPER_REGISTRY.keys())}")

    scraper_class = SCRAPER_REGISTRY[source]
    scraper = scraper_class()

    db = SessionLocal()
    log = ScrapeLog(source=source, status="running", started_at=datetime.datetime.now(datetime.UTC))
    db.add(log)
    db.commit()

    try:
        # 1. Scrape
        logger.info(f"Starting scrape: {source}")
        raw_jobs = scraper.scrape(search_query=search_query, location=location, max_pages=max_pages)

        # 2. Clean
        cleaned_jobs = clean_jobs(raw_jobs)

        # 3. Store in DB with upsert (insert or update on conflict)
        new_count = 0
        updated_count = 0
        for job_data in cleaned_jobs:
            job_dict = job_data.to_dict()
            job_dict["scraped_at"] = datetime.datetime.now(datetime.UTC)
            job_dict["is_active"] = True

            if job_dict.get("external_id") and job_dict.get("source"):
                # Upsert based on source + external_id
                stmt = pg_insert(Job).values(**job_dict)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["source", "external_id"],
                    set_={
                        "title": stmt.excluded.title,
                        "company": stmt.excluded.company,
                        "location": stmt.excluded.location,
                        "description": stmt.excluded.description,
                        "salary_min": stmt.excluded.salary_min,
                        "salary_max": stmt.excluded.salary_max,
                        "url": stmt.excluded.url,
                        "apply_url": stmt.excluded.apply_url,
                        "tags": stmt.excluded.tags,
                        "scraped_at": stmt.excluded.scraped_at,
                        "is_active": True,
                    },
                )
                result = db.execute(stmt)
                if result.rowcount:
                    updated_count += 1
            else:
                # No external_id - just insert
                db.add(Job(**job_dict))
                new_count += 1

        db.commit()

        # 4. Update log
        log.status = "success"
        log.jobs_found = len(raw_jobs)
        log.jobs_new = new_count
        log.jobs_updated = updated_count
        log.finished_at = datetime.datetime.now(datetime.UTC)
        db.commit()

        summary = {
            "source": source,
            "status": "success",
            "jobs_found": len(raw_jobs),
            "jobs_cleaned": len(cleaned_jobs),
            "jobs_new": new_count,
            "jobs_updated": updated_count,
        }
        logger.info(f"Scrape complete: {summary}")
        return summary

    except Exception as e:
        logger.error(f"Scrape failed for {source}: {e}")
        log.status = "failed"
        log.error_message = str(e)[:2000]
        log.finished_at = datetime.datetime.now(datetime.UTC)
        db.commit()
        return {"source": source, "status": "failed", "error": str(e)}
    finally:
        db.close()


def run_all_scrapers(
    search_query: str = None,
    location: str = None,
    max_pages: int = 5,
    sources: Optional[list[str]] = None,
) -> list[dict]:
    """Run all scrapers (or specified ones) and return summaries."""
    init_db()

    target_sources = sources or list(SCRAPER_REGISTRY.keys())
    results = []
    for source in target_sources:
        try:
            result = run_scraper(source, search_query, location, max_pages)
            results.append(result)
        except Exception as e:
            logger.error(f"Error running {source}: {e}")
            results.append({"source": source, "status": "error", "error": str(e)})

    total_found = sum(r.get("jobs_found", 0) for r in results)
    logger.info(f"All scrapers complete. Total jobs found: {total_found}")
    return results


if __name__ == "__main__":
    # Manual run for testing
    results = run_all_scrapers(
        search_query="software developer",
        location="Kenya",
        max_pages=2,
    )
    for r in results:
        print(r)
