"""
Data Cleaning & Transformation
Cleans scraped job data before inserting into the database.
"""
import re
import html
import logging
from typing import Optional

from airflow_home.scrapers.base_scraper import JobData

logger = logging.getLogger(__name__)


def clean_text(text: Optional[str]) -> Optional[str]:
    """Clean HTML entities, excess whitespace, and special chars from text."""
    if not text:
        return None
    text = html.unescape(text)
    text = re.sub(r"<[^>]+>", " ", text)  # strip HTML tags
    text = re.sub(r"\s+", " ", text).strip()
    return text if text else None


def normalize_job_type(raw: Optional[str]) -> Optional[str]:
    """Normalize job type to standard values."""
    if not raw:
        return None
    raw_lower = raw.lower().strip()
    mapping = {
        "full-time": "full-time",
        "full time": "full-time",
        "fulltime": "full-time",
        "part-time": "part-time",
        "part time": "part-time",
        "parttime": "part-time",
        "contract": "contract",
        "freelance": "freelance",
        "temporary": "temporary",
        "temp": "temporary",
        "internship": "internship",
        "intern": "internship",
        "volunteer": "volunteer",
    }
    for key, value in mapping.items():
        if key in raw_lower:
            return value
    return raw_lower


def normalize_experience_level(raw: Optional[str]) -> Optional[str]:
    """Normalize experience level."""
    if not raw:
        return None
    raw_lower = raw.lower().strip()
    if any(w in raw_lower for w in ["entry", "junior", "graduate", "jr"]):
        return "entry"
    if any(w in raw_lower for w in ["mid", "intermediate", "associate"]):
        return "mid"
    if any(w in raw_lower for w in ["senior", "sr", "lead", "principal"]):
        return "senior"
    if any(w in raw_lower for w in ["executive", "director", "vp", "c-level", "head"]):
        return "executive"
    return raw_lower


def detect_remote(job: JobData) -> bool:
    """Detect if a job is remote based on title, location, description."""
    search_text = " ".join(
        filter(None, [job.title, job.location, job.description])
    ).lower()
    remote_keywords = ["remote", "work from home", "wfh", "anywhere", "distributed"]
    return any(kw in search_text for kw in remote_keywords)


def extract_tags_from_description(description: Optional[str]) -> Optional[str]:
    """Extract technology/skill tags from job description."""
    if not description:
        return None
    tech_keywords = [
        "python", "java", "javascript", "typescript", "react", "angular", "vue",
        "node.js", "django", "flask", "fastapi", "spring", "docker", "kubernetes",
        "aws", "azure", "gcp", "sql", "postgresql", "mongodb", "redis",
        "git", "ci/cd", "agile", "scrum", "rest", "graphql", "api",
        "machine learning", "data science", "ai", "devops", "linux",
        "excel", "accounting", "marketing", "sales", "finance",
        "project management", "communication", "leadership",
    ]
    desc_lower = description.lower()
    found = [kw for kw in tech_keywords if kw in desc_lower]
    return ",".join(found) if found else None


def clean_job(job: JobData) -> JobData:
    """Apply all cleaning and transformation to a job."""
    job.title = clean_text(job.title)
    job.company = clean_text(job.company)
    job.location = clean_text(job.location)
    job.description = clean_text(job.description)

    if not job.title:
        return None

    # Normalize fields
    job.job_type = normalize_job_type(job.job_type)
    job.experience_level = normalize_experience_level(job.experience_level)

    # Detect remote
    if not job.remote:
        job.remote = detect_remote(job)

    # Extract tags if not already set
    if not job.tags and job.description:
        job.tags = extract_tags_from_description(job.description)

    # Clean URL
    if job.url:
        job.url = job.url.strip()
    if job.apply_url:
        job.apply_url = job.apply_url.strip()

    return job


def clean_jobs(jobs: list[JobData]) -> list[JobData]:
    """Clean a list of jobs, filtering out invalid ones."""
    cleaned = []
    for job in jobs:
        result = clean_job(job)
        if result and result.title:
            cleaned.append(result)
        else:
            logger.debug(f"Filtered out invalid job: {job}")
    logger.info(f"Cleaned {len(cleaned)} jobs from {len(jobs)} raw jobs")
    return cleaned
