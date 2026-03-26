"""
Adzuna API Scraper
Uses the Adzuna API to fetch job listings (free tier available).
Sign up at https://developer.adzuna.com/ for API keys.
"""
import logging
import os
from typing import Optional
from datetime import datetime

from scrapers.base_scraper import BaseScraper, JobData

logger = logging.getLogger(__name__)


class AdzunaAPIScraper(BaseScraper):
    """
    Adzuna provides a proper REST API for job data.
    Free tier: 250 requests/month.
    """

    SOURCE_NAME = "adzuna"
    BASE_URL = "https://api.adzuna.com/v1/api/jobs"

    def __init__(self):
        super().__init__()
        self.app_id = os.getenv("ADZUNA_APP_ID", "")
        self.app_key = os.getenv("ADZUNA_APP_KEY", "")

    def scrape(
        self, search_query: str = None, location: str = None, max_pages: int = 5
    ) -> list[JobData]:
        if not self.app_id or not self.app_key:
            logger.warning("[Adzuna] No API keys configured. Set ADZUNA_APP_ID and ADZUNA_APP_KEY.")
            return []

        jobs = []
        query = search_query or "software"
        country = "ke"  # Kenya, change as needed

        for page in range(1, max_pages + 1):
            url = f"{self.BASE_URL}/{country}/search/{page}"
            params = {
                "app_id": self.app_id,
                "app_key": self.app_key,
                "what": query,
                "results_per_page": 50,
                "max_days_old": 7,
                "sort_by": "date",
            }
            if location:
                params["where"] = location

            data = self.fetch_json(url, params=params)
            if not data or "results" not in data:
                break

            results = data["results"]
            if not results:
                break

            for item in results:
                try:
                    job = self._parse_result(item)
                    if job:
                        jobs.append(job)
                except Exception as e:
                    logger.error(f"[Adzuna] Error parsing result: {e}")

            logger.info(f"[Adzuna] Page {page}: found {len(results)} results")

        logger.info(f"[Adzuna] Total jobs: {len(jobs)}")
        return jobs

    def _parse_result(self, item: dict) -> Optional[JobData]:
        title = item.get("title")
        if not title:
            return None

        posted_date = None
        created = item.get("created")
        if created:
            try:
                posted_date = datetime.fromisoformat(created.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                pass

        salary_min = item.get("salary_min")
        salary_max = item.get("salary_max")

        return JobData(
            title=title,
            company=item.get("company", {}).get("display_name"),
            location=item.get("location", {}).get("display_name"),
            description=item.get("description"),
            salary_min=salary_min,
            salary_max=salary_max,
            url=item.get("redirect_url"),
            apply_url=item.get("redirect_url"),
            source=self.SOURCE_NAME,
            external_id=item.get("id"),
            posted_date=posted_date,
            tags=",".join(item.get("category", {}).get("tag", "").split()),
        )
