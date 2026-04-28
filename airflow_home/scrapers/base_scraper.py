"""
Base scraper class - all scrapers inherit from this.
"""
import logging
import datetime
from abc import ABC, abstractmethod
from typing import Optional

import requests
from bs4 import BeautifulSoup

from airflow_home.config.settings import settings

logger = logging.getLogger(__name__)


class JobData:
    """Standard job data container used across all scrapers."""

    def __init__(
        self,
        title: str,
        source: str,
        company: Optional[str] = None,
        location: Optional[str] = None,
        description: Optional[str] = None,
        salary_min: Optional[float] = None,
        salary_max: Optional[float] = None,
        salary_currency: Optional[str] = None,
        job_type: Optional[str] = None,
        experience_level: Optional[str] = None,
        remote: bool = False,
        url: Optional[str] = None,
        apply_url: Optional[str] = None,
        tags: Optional[str] = None,
        requirements: Optional[str] = None,
        posted_date: Optional[datetime.datetime] = None,
        application_deadline: Optional[datetime.datetime] = None,
        external_id: Optional[str] = None,
    ):
        self.title = title
        self.source = source
        self.company = company
        self.location = location
        self.description = description
        self.salary_min = salary_min
        self.salary_max = salary_max
        self.salary_currency = salary_currency
        self.job_type = job_type
        self.experience_level = experience_level
        self.remote = remote
        self.url = url
        self.apply_url = apply_url
        self.tags = tags
        self.requirements = requirements
        self.posted_date = posted_date
        self.application_deadline = application_deadline
        self.external_id = external_id

    def to_dict(self) -> dict:
        return {
            "title": self.title,
            "source": self.source,
            "company": self.company,
            "location": self.location,
            "description": self.description,
            "salary_min": self.salary_min,
            "salary_max": self.salary_max,
            "salary_currency": self.salary_currency,
            "job_type": self.job_type,
            "experience_level": self.experience_level,
            "remote": self.remote,
            "url": self.url,
            "apply_url": self.apply_url,
            "tags": self.tags,
            "requirements": self.requirements,
            "posted_date": self.posted_date,
            "application_deadline": self.application_deadline,
            "external_id": self.external_id,
        }


class BaseScraper(ABC):
    """Base class for all job scrapers."""

    SOURCE_NAME: str = "unknown"

    # Rotating user agents to reduce bot detection
    _USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    ]

    def __init__(self):
        import random
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": random.choice(self._USER_AGENTS),
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            }
        )

    def _rotate_ua(self):
        """Rotate user agent between requests to reduce blocking."""
        import random
        self.session.headers["User-Agent"] = random.choice(self._USER_AGENTS)

    def fetch_page(self, url: str, params: dict = None) -> Optional[BeautifulSoup]:
        """Fetch a page and return parsed HTML."""
        try:
            self._rotate_ua()
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return BeautifulSoup(response.text, "lxml")
        except requests.RequestException as e:
            logger.error(f"[{self.SOURCE_NAME}] Failed to fetch {url}: {e}")
            return None

    def fetch_json(self, url: str, params: dict = None, headers: dict = None) -> Optional[dict]:
        """Fetch JSON data from an API endpoint."""
        try:
            self._rotate_ua()
            resp = self.session.get(url, params=params, headers=headers, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logger.error(f"[{self.SOURCE_NAME}] Failed to fetch JSON {url}: {e}")
            return None

    @abstractmethod
    def scrape(self, search_query: str = None, location: str = None, max_pages: int = 5) -> list[JobData]:
        """Scrape jobs. Must be implemented by each scraper."""
        pass
