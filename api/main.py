"""
Backend API - FastAPI application for serving job data.
Includes built-in cron scheduler that runs daily at 2PM EAT.
"""
import math
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, Depends, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr
from sqlalchemy.orm import Session
from sqlalchemy import func, desc, text
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from airflow_home.database.connection import get_db, init_db
from airflow_home.database.models import Job, ScrapeLog
from airflow_home.config.settings import settings

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
        orm_mode = True


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
            location=None,  # Scrape ALL locations, not just Kenya
            max_pages=3,
        )
        total = sum(r.get("jobs_found", 0) for r in results)
        logger.info(f"=== DAILY SCRAPE DONE: {total} jobs found from {len(results)} sources ===")
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
    per_page: int = Query(20, ge=1, le=1000),
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
        jobs=[JobResponse.from_orm(j) for j in jobs],
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
    return JobResponse.from_orm(job)


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


# --- Email subscription ---

SITE_URL = "https://annex-careers.netlify.app"


class SubscribeRequest(BaseModel):
    email: EmailStr


def build_welcome_email_html(jobs: list[Job]) -> str:
    """Build a branded HTML welcome email with featured jobs."""
    job_cards = ""
    for job in jobs:
        location = job.location or "Remote"
        company = job.company or "—"
        job_type = job.job_type or ""
        job_url = f"{SITE_URL}/jobs/{job.id}"
        type_badge = f'<span style="display:inline-block;background:#fef2f2;color:#dc2626;font-size:11px;padding:2px 8px;border-radius:12px;margin-top:6px;">{job_type}</span>' if job_type else ""
        job_cards += f"""
        <tr>
          <td style="padding:0 24px 12px;">
            <table width="100%" cellpadding="0" cellspacing="0" style="background:#ffffff;border:1px solid #e5e7eb;border-radius:10px;overflow:hidden;">
              <tr>
                <td style="padding:16px 20px;">
                  <a href="{job_url}" style="color:#dc2626;font-size:15px;font-weight:600;text-decoration:none;">{job.title}</a>
                  <div style="color:#6b7280;font-size:13px;margin-top:4px;">{company}</div>
                  <div style="color:#6b7280;font-size:12px;margin-top:4px;">📍 {location}</div>
                  {type_badge}
                  <div style="margin-top:12px;">
                    <a href="{job_url}" style="display:inline-block;background:#dc2626;color:#ffffff;font-size:13px;font-weight:500;padding:8px 20px;border-radius:6px;text-decoration:none;">View &amp; Apply</a>
                  </div>
                </td>
              </tr>
            </table>
          </td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f3f4f6;padding:24px 0;">
    <tr>
      <td align="center">
        <table width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%;background:#ffffff;border-radius:12px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,0.1);">
          <!-- Red header bar -->
          <tr>
            <td style="background:#dc2626;padding:28px 24px;text-align:center;">
              <h1 style="margin:0;color:#ffffff;font-size:26px;font-weight:700;">Annex Careers</h1>
              <p style="margin:6px 0 0;color:rgba(255,255,255,0.85);font-size:13px;">Your gateway to the best job opportunities</p>
            </td>
          </tr>

          <!-- Welcome message -->
          <tr>
            <td style="padding:32px 24px 12px;">
              <h2 style="margin:0 0 8px;color:#111827;font-size:20px;font-weight:700;">Welcome to Annex Careers</h2>
              <p style="margin:0 0 6px;color:#374151;font-size:14px;line-height:1.6;">
                You're now subscribed to daily job alerts. We'll send you the latest opportunities straight to your inbox every morning.
              </p>
              <p style="margin:0;color:#374151;font-size:14px;line-height:1.6;">
                Here are some jobs you can explore right now:
              </p>
            </td>
          </tr>

          <!-- Section header -->
          <tr>
            <td style="padding:20px 24px 12px;">
              <h3 style="margin:0;color:#dc2626;font-size:16px;font-weight:600;border-bottom:2px solid #dc2626;padding-bottom:8px;">Latest Opportunities</h3>
            </td>
          </tr>

          <!-- Job cards -->
          {job_cards}

          <!-- Browse all CTA -->
          <tr>
            <td style="padding:20px 24px 8px;text-align:center;">
              <a href="{SITE_URL}/jobs" style="display:inline-block;background:#dc2626;color:#ffffff;font-size:15px;font-weight:600;padding:12px 36px;border-radius:8px;text-decoration:none;">Browse All Jobs</a>
            </td>
          </tr>

          <!-- Footer -->
          <tr>
            <td style="padding:28px 24px;text-align:center;border-top:1px solid #e5e7eb;margin-top:16px;">
              <p style="margin:0 0 6px;color:#9ca3af;font-size:12px;">
                © {datetime.now().year} Annex Careers &bull; Aggregating opportunities from 20+ sources
              </p>
              <p style="margin:0;color:#9ca3af;font-size:11px;">
                <a href="{SITE_URL}" style="color:#dc2626;text-decoration:none;">Visit website</a>
              </p>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""


def send_email(to_email: str, subject: str, html_body: str):
    """Send an email via SMTP."""
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"{settings.EMAIL_FROM_NAME} <{settings.SMTP_USER}>"
    msg["To"] = to_email
    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT) as server:
        server.ehlo()
        server.starttls()
        server.login(settings.SMTP_USER, settings.SMTP_PASSWORD)
        server.sendmail(settings.SMTP_USER, [to_email], msg.as_string())


@app.post("/api/subscribe")
def subscribe(req: SubscribeRequest, db: Session = Depends(get_db)):
    """Subscribe to job alerts — sends a branded welcome email with featured jobs."""
    if not settings.SMTP_USER or not settings.SMTP_PASSWORD:
        raise HTTPException(status_code=503, detail="Email service not configured")

    # Fetch 5 recent active jobs
    featured_jobs = (
        db.query(Job)
        .filter(Job.is_active == True)
        .order_by(desc(Job.scraped_at))
        .limit(5)
        .all()
    )

    html = build_welcome_email_html(featured_jobs)

    try:
        send_email(req.email, "Welcome to Annex Careers; Here are today's top opportunities", html)
    except Exception as e:
        logger.error(f"Failed to send welcome email to {req.email}: {e}")
        raise HTTPException(status_code=500, detail="Failed to send email. Please try again.")

    return {"message": "Welcome"}


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
