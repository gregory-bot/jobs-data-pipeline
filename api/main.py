"""
Backend API - FastAPI application for serving job data.
Includes built-in cron scheduler that runs daily at 2PM EAT.
"""
import math
import logging
import smtplib
import threading
import httpx
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone
from typing import Optional, List

from fastapi import FastAPI, Depends, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr
from sqlalchemy.orm import Session
from sqlalchemy import func, desc, text
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from airflow_home.database.connection import get_db, init_db
from airflow_home.database.models import Job, ScrapeLog, User
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


def keep_alive_ping():
    """Ping our own health endpoint to prevent Render from sleeping."""
    try:
        httpx.get("https://jobs-data-pipeline.onrender.com/api/health", timeout=10)
        logger.debug("Keep-alive ping sent")
    except Exception:
        pass


@app.on_event("startup")
def startup():
    init_db()
    # Migrate: add columns / tables if they don't exist
    from airflow_home.database.connection import engine
    with engine.connect() as conn:
        try:
            conn.execute(text(
                "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS application_deadline TIMESTAMP"
            ))
            conn.execute(text(
                "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS requirements TEXT"
            ))
            # Create users table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    email VARCHAR(320) NOT NULL UNIQUE,
                    name VARCHAR(300),
                    source VARCHAR(50) NOT NULL DEFAULT 'subscribe',
                    job_interests TEXT,
                    subscribed_at TIMESTAMP DEFAULT NOW(),
                    last_emailed_at TIMESTAMP
                )
            """))
            conn.execute(text("COMMIT"))
            logger.info("DB migration: columns/tables ensured")
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
    # Keep-alive ping every 10 minutes to prevent Render sleep
    scheduler.add_job(
        keep_alive_ping,
        IntervalTrigger(minutes=10),
        id="keep_alive",
        name="Keep-alive Ping (10min)",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("Scheduler started: daily scrape at 2PM EAT + keep-alive every 10min")


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


@app.get("/api/test-email")
def test_email_config():
    """Diagnostic: test email configuration."""
    diag = {
        "resend_api_key_set": bool(settings.RESEND_API_KEY),
        "resend_api_key_length": len(settings.RESEND_API_KEY) if settings.RESEND_API_KEY else 0,
        "email_from": settings.EMAIL_FROM,
        "smtp_host": settings.SMTP_HOST,
        "smtp_port": settings.SMTP_PORT,
        "smtp_user": settings.SMTP_USER,
        "smtp_password_set": bool(settings.SMTP_PASSWORD),
    }
    # Test Resend
    if settings.RESEND_API_KEY:
        try:
            resp = httpx.post(
                "https://api.resend.com/emails",
                headers={
                    "Authorization": f"Bearer {settings.RESEND_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "from": settings.EMAIL_FROM,
                    "to": [settings.SMTP_USER],
                    "subject": "Annex Careers Email Test",
                    "html": "<p>Resend email is working!</p>",
                },
                timeout=15,
            )
            diag["resend_status"] = resp.status_code
            diag["resend_response"] = resp.json() if resp.status_code in (200, 201) else resp.text
        except Exception as e:
            diag["resend_error"] = str(e)
    else:
        diag["resend_status"] = "not configured"
    return diag


class SubscribeRequest(BaseModel):
    email: EmailStr


class UserRegisterRequest(BaseModel):
    email: EmailStr
    name: Optional[str] = None
    job_interests: Optional[str] = None


class BulkEmailRequest(BaseModel):
    user_ids: List[int]


def build_welcome_email_html(jobs: list, name: str = None) -> str:
    """Build a branded HTML welcome email with featured jobs."""
    greeting = f"Hello {name}," if name else "Hello,"
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
          <tr>
            <td style="background:#dc2626;padding:28px 24px;text-align:center;">
              <h1 style="margin:0;color:#ffffff;font-size:26px;font-weight:700;">Annex Careers</h1>
              <p style="margin:6px 0 0;color:rgba(255,255,255,0.85);font-size:13px;">Your gateway to the best job opportunities</p>
            </td>
          </tr>
          <tr>
            <td style="padding:32px 24px 12px;">
              <h2 style="margin:0 0 8px;color:#111827;font-size:20px;font-weight:700;">{greeting}</h2>
              <p style="margin:0 0 6px;color:#374151;font-size:14px;line-height:1.6;">
                Here are the latest job opportunities we think you'll love:
              </p>
            </td>
          </tr>
          <tr>
            <td style="padding:20px 24px 12px;">
              <h3 style="margin:0;color:#dc2626;font-size:16px;font-weight:600;border-bottom:2px solid #dc2626;padding-bottom:8px;">Latest Opportunities</h3>
            </td>
          </tr>
          {job_cards}
          <tr>
            <td style="padding:20px 24px 8px;text-align:center;">
              <a href="{SITE_URL}/jobs" style="display:inline-block;background:#dc2626;color:#ffffff;font-size:15px;font-weight:600;padding:12px 36px;border-radius:8px;text-decoration:none;">Browse All Jobs</a>
            </td>
          </tr>
          <tr>
            <td style="padding:28px 24px;text-align:center;border-top:1px solid #e5e7eb;margin-top:16px;">
              <p style="margin:0 0 6px;color:#9ca3af;font-size:12px;">
                &copy; {datetime.now().year} Annex Careers &bull; Aggregating opportunities from 20+ sources
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


def build_targeted_email_html(name: str, interest_label: str, jobs: list) -> str:
    """Build an email for targeted job recommendations based on user interests."""
    job_cards = ""
    for job in jobs:
        location = job.location or "Remote"
        company = job.company or "—"
        job_url = f"{SITE_URL}/jobs/{job.id}"
        job_cards += f"""
        <tr>
          <td style="padding:0 24px 12px;">
            <table width="100%" cellpadding="0" cellspacing="0" style="background:#ffffff;border:1px solid #e5e7eb;border-radius:10px;overflow:hidden;">
              <tr>
                <td style="padding:16px 20px;">
                  <a href="{job_url}" style="color:#dc2626;font-size:15px;font-weight:600;text-decoration:none;">{job.title}</a>
                  <div style="color:#6b7280;font-size:13px;margin-top:4px;">{company}</div>
                  <div style="color:#6b7280;font-size:12px;margin-top:4px;">📍 {location}</div>
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
          <tr>
            <td style="background:#dc2626;padding:28px 24px;text-align:center;">
              <h1 style="margin:0;color:#ffffff;font-size:26px;font-weight:700;">Annex Careers</h1>
              <p style="margin:6px 0 0;color:rgba(255,255,255,0.85);font-size:13px;">Jobs matched to your interests</p>
            </td>
          </tr>
          <tr>
            <td style="padding:32px 24px 12px;">
              <h2 style="margin:0 0 8px;color:#111827;font-size:20px;font-weight:700;">Hello {name},</h2>
              <p style="margin:0 0 6px;color:#374151;font-size:14px;line-height:1.6;">
                Check out jobs applied by other <strong>{interest_label}</strong> professionals like you:
              </p>
            </td>
          </tr>
          <tr>
            <td style="padding:20px 24px 12px;">
              <h3 style="margin:0;color:#dc2626;font-size:16px;font-weight:600;border-bottom:2px solid #dc2626;padding-bottom:8px;">Jobs for {interest_label} Professionals</h3>
            </td>
          </tr>
          {job_cards}
          <tr>
            <td style="padding:20px 24px 8px;text-align:center;">
              <a href="{SITE_URL}/jobs" style="display:inline-block;background:#dc2626;color:#ffffff;font-size:15px;font-weight:600;padding:12px 36px;border-radius:8px;text-decoration:none;">Browse All Jobs</a>
            </td>
          </tr>
          <tr>
            <td style="padding:28px 24px;text-align:center;border-top:1px solid #e5e7eb;margin-top:16px;">
              <p style="margin:0 0 6px;color:#9ca3af;font-size:12px;">
                &copy; {datetime.now().year} Annex Careers &bull; Aggregating opportunities from 20+ sources
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
    """Send an email via Resend HTTP API (primary) or SMTP (fallback)."""
    # Primary: Resend HTTP API (works on Render where SMTP is blocked)
    if settings.RESEND_API_KEY:
        resp = httpx.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {settings.RESEND_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "from": settings.EMAIL_FROM,
                "to": [to_email],
                "subject": subject,
                "html": html_body,
            },
            timeout=15,
        )
        if resp.status_code in (200, 201):
            logger.info(f"Resend: sent email to {to_email}")
            return
        logger.warning(f"Resend failed ({resp.status_code}): {resp.text}, falling back to SMTP")

    # Fallback: SMTP
    if settings.SMTP_USER and settings.SMTP_PASSWORD:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"{settings.EMAIL_FROM_NAME} <{settings.SMTP_USER}>"
        msg["To"] = to_email
        msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT, timeout=10) as server:
            server.ehlo()
            server.starttls()
            server.login(settings.SMTP_USER, settings.SMTP_PASSWORD)
            server.sendmail(settings.SMTP_USER, [to_email], msg.as_string())
        logger.info(f"SMTP: sent email to {to_email}")
        return

    raise RuntimeError("No email provider configured (set RESEND_API_KEY or SMTP_PASSWORD)")


def send_email_background(to_email: str, subject: str, html_body: str):
    """Fire-and-forget email sending in a background thread."""
    def _send():
        try:
            send_email(to_email, subject, html_body)
            logger.info(f"Sent email to {to_email}")
        except Exception as e:
            logger.error(f"Failed to send email to {to_email}: {e}")
    threading.Thread(target=_send, daemon=True).start()


def _save_user(db: Session, email: str, name: str = None, source: str = "subscribe", job_interests: str = None):
    """Upsert a user into the users table."""
    existing = db.query(User).filter(User.email == email).first()
    if existing:
        if name:
            existing.name = name
        if source == "cv_upload":
            existing.source = "cv_upload"
        if job_interests:
            existing.job_interests = job_interests
        db.commit()
        db.refresh(existing)
        return existing
    user = User(email=email, name=name, source=source, job_interests=job_interests)
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


@app.post("/api/subscribe")
def subscribe(req: SubscribeRequest, db: Session = Depends(get_db)):
    """Subscribe to job alerts — saves user and sends welcome email in background."""
    if not settings.RESEND_API_KEY and (not settings.SMTP_USER or not settings.SMTP_PASSWORD):
        raise HTTPException(status_code=503, detail="Email service not configured")

    # Save subscriber to DB
    _save_user(db, email=req.email, source="subscribe")

    # Fetch 5 recent active jobs for the email
    featured_jobs = (
        db.query(Job)
        .filter(Job.is_active == True)
        .order_by(desc(Job.scraped_at))
        .limit(5)
        .all()
    )

    html = build_welcome_email_html(featured_jobs)
    # Send in background so the response returns instantly
    send_email_background(req.email, "Welcome to Annex Careers - Here are today's top opportunities", html)

    return {"message": "Subscribed! Welcome email is on its way."}


@app.post("/api/users/register")
def register_user(req: UserRegisterRequest, db: Session = Depends(get_db)):
    """Register a user from CV upload — saves email, name, interests and sends targeted email."""
    user = _save_user(db, email=req.email, name=req.name, source="cv_upload", job_interests=req.job_interests)

    # Send targeted email if interests provided
    if req.job_interests and (settings.RESEND_API_KEY or (settings.SMTP_USER and settings.SMTP_PASSWORD)):
        from sqlalchemy import or_
        interest_keywords = [kw.strip().lower() for kw in req.job_interests.replace(",", " ").split() if len(kw.strip()) > 2]
        if not interest_keywords:
            interest_keywords = [req.job_interests.strip().lower()]
        filters = []
        for kw in interest_keywords:
            filters.append(Job.title.ilike(f"%{kw}%"))
            filters.append(Job.description.ilike(f"%{kw}%"))
        matched_jobs = (
            db.query(Job)
            .filter(Job.is_active == True)
            .filter(or_(*filters))
            .order_by(desc(Job.scraped_at))
            .limit(5)
            .all()
        )
        # Fallback to latest jobs if no keyword matches
        if not matched_jobs:
            matched_jobs = (
                db.query(Job)
                .filter(Job.is_active == True)
                .order_by(desc(Job.scraped_at))
                .limit(5)
                .all()
            )
        interest_label = req.job_interests.title()
        html = build_targeted_email_html(req.name or "there", interest_label, matched_jobs)
        send_email_background(req.email, f"Hey {req.name or 'there'}, jobs for {interest_label} professionals like you - Annex Careers", html)
        user.last_emailed_at = datetime.now(timezone.utc)
        db.commit()

    return {"message": "User registered", "user_id": user.id}


@app.get("/api/users")
def list_users(
    source: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """List all registered users (admin)."""
    query = db.query(User).order_by(desc(User.subscribed_at))
    if source:
        query = query.filter(User.source == source)
    users = query.all()
    return {
        "users": [
            {
                "id": u.id,
                "email": u.email,
                "name": u.name,
                "source": u.source,
                "job_interests": u.job_interests,
                "subscribed_at": u.subscribed_at.isoformat() if u.subscribed_at else None,
                "last_emailed_at": u.last_emailed_at.isoformat() if u.last_emailed_at else None,
            }
            for u in users
        ],
        "total": len(users),
    }


@app.post("/api/users/send-alerts")
def send_bulk_alerts(req: BulkEmailRequest, db: Session = Depends(get_db)):
    """Send daily job alert emails to selected users (admin trigger)."""
    if not settings.RESEND_API_KEY and (not settings.SMTP_USER or not settings.SMTP_PASSWORD):
        raise HTTPException(status_code=503, detail="Email service not configured")

    users = db.query(User).filter(User.id.in_(req.user_ids)).all()
    if not users:
        raise HTTPException(status_code=404, detail="No users found")

    featured_jobs = (
        db.query(Job)
        .filter(Job.is_active == True)
        .order_by(desc(Job.scraped_at))
        .limit(5)
        .all()
    )

    sent_count = 0
    for user in users:
        # If user has interests, send targeted; otherwise send general
        if user.job_interests:
            from sqlalchemy import or_
            interest_keywords = [kw.strip().lower() for kw in user.job_interests.replace(",", " ").split() if len(kw.strip()) > 2]
            if not interest_keywords:
                interest_keywords = [user.job_interests.strip().lower()]
            filters = []
            for kw in interest_keywords:
                filters.append(Job.title.ilike(f"%{kw}%"))
                filters.append(Job.description.ilike(f"%{kw}%"))
            matched = (
                db.query(Job)
                .filter(Job.is_active == True)
                .filter(or_(*filters))
                .order_by(desc(Job.scraped_at))
                .limit(5)
                .all()
            )
            jobs_to_send = matched if matched else featured_jobs
            interest_label = user.job_interests.title()
            html = build_targeted_email_html(user.name or "there", interest_label, jobs_to_send)
            subject = f"Jobs for {interest_label} professionals - Annex Careers"
        else:
            html = build_welcome_email_html(featured_jobs, name=user.name)
            subject = "Your Daily Job Alerts - Annex Careers"

        send_email_background(user.email, subject, html)
        user.last_emailed_at = datetime.now(timezone.utc)
        sent_count += 1

    db.commit()
    return {"message": f"Sending emails to {sent_count} users", "sent": sent_count}


@app.get("/api/scrape-logs")
def get_scrape_logs(
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """Get recent scrape logs for admin dashboard."""
    logs = (
        db.query(ScrapeLog)
        .order_by(desc(ScrapeLog.started_at))
        .limit(limit)
        .all()
    )
    return {
        "logs": [
            {
                "id": log.id,
                "source": log.source,
                "status": log.status,
                "jobs_found": log.jobs_found,
                "jobs_new": log.jobs_new,
                "jobs_updated": log.jobs_updated,
                "error_message": log.error_message,
                "started_at": log.started_at.isoformat() if log.started_at else None,
                "finished_at": log.finished_at.isoformat() if log.finished_at else None,
            }
            for log in logs
        ],
        "total": len(logs),
    }


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
