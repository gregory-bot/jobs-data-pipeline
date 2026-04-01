"""
Backend API - FastAPI application for serving job data.
Includes built-in cron scheduler that runs daily at 2PM EAT.
"""
import math
import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, Depends, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import func, desc, text
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from airflow_home.database.connection import get_db, init_db
from airflow_home.database.models import Job, ScrapeLog

app = FastAPI(
    title="Jobs Pipeline API",
    description="API for browsing aggregated job listings from multiple sources",
    version="1.0.0",
)

# CORS - allow frontend to connect
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- Pydantic Schemas ---

class JobResponse(BaseModel):
    id: int
    title: str
    company: Optional[str] = None
    location: Optional[str] = None
    description: Optional[str] = None
    salary_min: Optional[float] = None
    salary_max: Optional[float] = None
    salary_currency: Optional[str] = None
    job_type: Optional[str] = None
    experience_level: Optional[str] = None
    remote: bool = False
    url: Optional[str] = None
    apply_url: Optional[str] = None
    source: str
    tags: Optional[str] = None
    requirements: Optional[str] = None
    posted_date: Optional[datetime] = None
    application_deadline: Optional[datetime] = None
    scraped_at: Optional[datetime] = None
    is_active: bool = True

    class Config:
        from_attributes = True


class PaginatedResponse(BaseModel):
    jobs: list[JobResponse]
    total: int
    page: int
    pages: int
    per_page: int


class StatsResponse(BaseModel):
    total_jobs: int
    active_jobs: int
    sources: dict
    recent_scrapes: list[dict]


# --- Startup ---

logger = logging.getLogger("api")
scheduler = BackgroundScheduler()


def scheduled_daily_scrape():
    """Run all scrapers daily at 2PM EAT."""
    from airflow_home.scrapers.runner import run_all_scrapers
    logger.info("=== SCHEDULED DAILY SCRAPE STARTED ===")
    try:
        results = run_all_scrapers(
            search_query="jobs",
            location="Kenya",
            max_pages=3,
        )
        total = sum(r.get("jobs_found", 0) for r in results)
        logger.info(f"=== DAILY SCRAPE DONE: {total} jobs found ===")
    except Exception as e:
        logger.error(f"Scheduled scrape failed: {e}")


@app.on_event("startup")
def startup():
    init_db()
    # Migrate: add application_deadline column if it doesn't exist
    from airflow_home.database.connection import engine
    with engine.connect() as conn:
        try:
            conn.execute(text(
                "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS application_deadline TIMESTAMP"
            ))
            conn.execute(text(
                "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS requirements TEXT"
            ))
            conn.execute(text("COMMIT"))
            logger.info("DB migration: columns ensured")
        except Exception as e:
            logger.warning(f"DB migration note: {e}")
    # Schedule daily scrape at 2:00 PM EAT (11:00 AM UTC)
    scheduler.add_job(
        scheduled_daily_scrape,
        CronTrigger(hour=11, minute=0),  # 11 UTC = 2PM EAT
        id="daily_scrape",
        name="Daily Full Scrape (2PM EAT)",
        replace_existing=True,
        misfire_grace_time=3600,
    )
    scheduler.start()
    logger.info("Scheduler started: daily scrape at 2PM EAT")


@app.on_event("shutdown")
def shutdown():
    scheduler.shutdown(wait=False)


# Global exception handler to surface real error messages
from fastapi import Request
from fastapi.responses import JSONResponse

@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    import traceback
    logger.error(f"Unhandled exception on {request.url}: {exc}\n{traceback.format_exc()}")
    return JSONResponse(status_code=500, content={"detail": str(exc)})


# --- Endpoints ---

@app.get("/")
def root():
    return {"message": "Jobs Pipeline API", "docs": "/docs"}


@app.get("/api/jobs", response_model=PaginatedResponse)
def list_jobs(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    search: Optional[str] = None,
    source: Optional[str] = None,
    location: Optional[str] = None,
    job_type: Optional[str] = None,
    remote: Optional[bool] = None,
    sort_by: str = Query("scraped_at", pattern="^(scraped_at|posted_date|title|company)$"),
    sort_order: str = Query("desc", pattern="^(asc|desc)$"),
    db: Session = Depends(get_db),
):
    """List jobs with filtering, search, and pagination."""
    query = db.query(Job).filter(Job.is_active == True)

    # Auto-filter out jobs with expired application deadlines (only when deadline is set)
    now = datetime.now(timezone.utc).replace(tzinfo=None)  # naive UTC to match DB
    query = query.filter(
        (Job.application_deadline == None) | (Job.application_deadline >= now)
    )

    if search:
        search_filter = f"%{search}%"
        query = query.filter(
            (Job.title.ilike(search_filter))
            | (Job.company.ilike(search_filter))
            | (Job.description.ilike(search_filter))
            | (Job.tags.ilike(search_filter))
        )

    if source:
        query = query.filter(Job.source == source)
    if location:
        query = query.filter(Job.location.ilike(f"%{location}%"))
    if job_type:
        query = query.filter(Job.job_type == job_type)
    if remote is not None:
        query = query.filter(Job.remote == remote)

    # Sorting
    sort_col = getattr(Job, sort_by, Job.scraped_at)
    if sort_order == "desc":
        query = query.order_by(desc(sort_col))
    else:
        query = query.order_by(sort_col)

    total = query.count()
    pages = math.ceil(total / per_page) if total > 0 else 1
    jobs = query.offset((page - 1) * per_page).limit(per_page).all()

    return PaginatedResponse(
        jobs=[JobResponse.model_validate(j) for j in jobs],
        total=total,
        page=page,
        pages=pages,
        per_page=per_page,
    )


@app.get("/api/jobs/{job_id}", response_model=JobResponse)
def get_job(job_id: int, db: Session = Depends(get_db)):
    """Get a single job by ID."""
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return JobResponse.model_validate(job)


@app.get("/api/sources")
def list_sources(db: Session = Depends(get_db)):
    """List all available job sources with counts."""
    results = (
        db.query(Job.source, func.count(Job.id))
        .filter(Job.is_active == True)
        .group_by(Job.source)
        .all()
    )
    return {"sources": {source: count for source, count in results}}


@app.get("/api/stats", response_model=StatsResponse)
def get_stats(db: Session = Depends(get_db)):
    """Get pipeline statistics."""
    total_jobs = db.query(func.count(Job.id)).scalar()
    active_jobs = db.query(func.count(Job.id)).filter(Job.is_active == True).scalar()

    source_counts = (
        db.query(Job.source, func.count(Job.id))
        .group_by(Job.source)
        .all()
    )

    recent_logs = (
        db.query(ScrapeLog)
        .order_by(desc(ScrapeLog.started_at))
        .limit(20)
        .all()
    )

    return StatsResponse(
        total_jobs=total_jobs,
        active_jobs=active_jobs,
        sources={s: c for s, c in source_counts},
        recent_scrapes=[
            {
                "source": log.source,
                "status": log.status,
                "jobs_found": log.jobs_found,
                "started_at": log.started_at.isoformat() if log.started_at else None,
            }
            for log in recent_logs
        ],
    )


@app.post("/api/scrape/{source}")
def trigger_scrape(
    source: str,
    search_query: Optional[str] = None,
    location: Optional[str] = None,
    max_pages: int = Query(3, ge=1, le=10),
):
    """Manually trigger a scrape for a specific source."""
    from airflow_home.scrapers.runner import run_scraper, SCRAPER_REGISTRY

    if source not in SCRAPER_REGISTRY:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown source: {source}. Available: {list(SCRAPER_REGISTRY.keys())}",
        )

    result = run_scraper(source, search_query=search_query, location=location, max_pages=max_pages)
    return result


@app.get("/api/health")
def health_check(db: Session = Depends(get_db)):
    """Health check endpoint for monitoring."""
    try:
        db.execute(text("SELECT 1"))
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "database": str(e)}


@app.get("/api/scheduler")
def scheduler_status():
    """Check scheduler status and next run time."""
    jobs = scheduler.get_jobs()
    return {
        "running": scheduler.running,
        "jobs": [
            {
                "id": job.id,
                "name": job.name,
                "next_run": str(job.next_run_time) if job.next_run_time else None,
            }
            for job in jobs
        ],
    }


@app.post("/api/scrape-all")
def trigger_full_scrape(
    search_query: Optional[str] = None,
    location: Optional[str] = Query("Kenya"),
    max_pages: int = Query(3, ge=1, le=10),
):
    """Manually trigger a scrape across all sources."""
    from airflow_home.scrapers.runner import run_all_scrapers
    results = run_all_scrapers(
        search_query=search_query,
        location=location,
        max_pages=max_pages,
    )
    return {"results": results, "total_found": sum(r.get("jobs_found", 0) for r in results)}
