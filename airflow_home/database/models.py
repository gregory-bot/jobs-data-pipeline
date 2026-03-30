"""
Database models for the jobs pipeline.
"""
import datetime
from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    DateTime,
    Boolean,
    Float,
    Index,
)
from airflow_home.database.connection import Base


class Job(Base):
    __tablename__ = "jobs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    external_id = Column(String(255), nullable=True)
    title = Column(String(500), nullable=False)
    company = Column(String(300), nullable=True)
    location = Column(String(300), nullable=True)
    description = Column(Text, nullable=True)
    salary_min = Column(Float, nullable=True)
    salary_max = Column(Float, nullable=True)
    salary_currency = Column(String(10), nullable=True)
    job_type = Column(String(50), nullable=True)  # full-time, part-time, contract, etc.
    experience_level = Column(String(50), nullable=True)  # entry, mid, senior
    remote = Column(Boolean, default=False)
    url = Column(String(1000), nullable=True)
    apply_url = Column(String(1000), nullable=True)
    source = Column(String(100), nullable=False)  # linkedin, myjobsinkenya, etc.
    tags = Column(Text, nullable=True)  # comma-separated tags/skills
    posted_date = Column(DateTime, nullable=True)
    scraped_at = Column(DateTime, default=datetime.datetime.utcnow)
    is_active = Column(Boolean, default=True)

    __table_args__ = (
        Index("ix_jobs_source", "source"),
        Index("ix_jobs_title", "title"),
        Index("ix_jobs_company", "company"),
        Index("ix_jobs_location", "location"),
        Index("ix_jobs_scraped_at", "scraped_at"),
        Index("ix_jobs_source_external_id", "source", "external_id", unique=True),
    )

    def __repr__(self):
        return f"<Job(id={self.id}, title='{self.title}', company='{self.company}', source='{self.source}')>"


class ScrapeLog(Base):
    __tablename__ = "scrape_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String(100), nullable=False)
    status = Column(String(20), nullable=False)  # success, failed, partial
    jobs_found = Column(Integer, default=0)
    jobs_new = Column(Integer, default=0)
    jobs_updated = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    started_at = Column(DateTime, default=datetime.datetime.utcnow)
    finished_at = Column(DateTime, nullable=True)

    def __repr__(self):
        return f"<ScrapeLog(source='{self.source}', status='{self.status}', jobs_found={self.jobs_found})>"
