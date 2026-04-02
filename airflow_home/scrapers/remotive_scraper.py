"""
Remotive Scraper - remotive.com
Fully remote job board with JSON API.
"""
import time
import logging
import datetime
from typing import Optional

from airflow_home.scrapers.base_scraper import BaseScraper, JobData

logger = logging.getLogger(__name__)

REMOTIVE_CATEGORIES = [
    "software-dev",
    "devops",
    "design",
    "marketing",
    "product",
    "data",
    "finance-legal",
    "hr",
    "customer-support",
]


class RemotiveScraper(BaseScraper):
    SOURCE_NAME = "remotive"
    API_URL = "https://remotive.com/api/remote-jobs"

    def scrape(self, search_query: str = None, location: str = None, max_pages: int = 5) -> list[JobData]:
        """Remotive exposes a public REST API — no scraping needed."""
        jobs = []
        limit = max_pages * 20  # approximate

        # If search query, use it; otherwise cycle through top categories
        if search_query:
            data = self.fetch_json(self.API_URL, params={"search": search_query, "limit": limit})
            jobs.extend(self._parse_response(data))
        else:
            categories = REMOTIVE_CATEGORIES[:max(1, min(max_pages, len(REMOTIVE_CATEGORIES)))]
            for cat in categories:
                data = self.fetch_json(self.API_URL, params={"category": cat, "limit": 50})
                jobs.extend(self._parse_response(data))
                time.sleep(1)

        logger.info(f"[{self.SOURCE_NAME}] Total: {len(jobs)} jobs")
        return jobs

    def _parse_response(self, data: Optional[dict]) -> list[JobData]:
        if not data or "jobs" not in data:
            return []
        result = []
        for item in data["jobs"]:
            try:
                job = self._parse_item(item)
                if job:
                    result.append(job)
            except Exception as e:
                logger.debug(f"[{self.SOURCE_NAME}] Parse error: {e}")
        return result

    def _parse_item(self, item: dict) -> Optional[JobData]:
        title = item.get("title")
        if not title:
            return None

        company = item.get("company_name")
        url = item.get("url")
        candidate_required_location = item.get("candidate_required_location") or "Worldwide"
        description = item.get("description", "")
        tags_list = item.get("tags", [])
        tags = ",".join(tags_list) if isinstance(tags_list, list) else ""
        job_type = item.get("job_type") or "full_time"
        salary = item.get("salary") or ""

        posted_date = None
        pub_date = item.get("publication_date")
        if pub_date:
            try:
                posted_date = datetime.datetime.strptime(pub_date[:19], "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                pass

        # Parse salary
        import re
        salary_min = salary_max = None
        salary_currency = "USD"
        if salary:
            nums = re.findall(r"\d[\d,]*", salary.replace(",", ""))
            if len(nums) >= 2:
                try:
                    salary_min, salary_max = float(nums[0]), float(nums[1])
                except ValueError:
                    pass
            elif len(nums) == 1:
                try:
                    salary_min = salary_max = float(nums[0])
                except ValueError:
                    pass
            if "£" in salary or "GBP" in salary.upper():
                salary_currency = "GBP"
            elif "€" in salary or "EUR" in salary.upper():
                salary_currency = "EUR"

        return JobData(
            title=title,
            company=company,
            location=candidate_required_location,
            description=description,
            salary_min=salary_min,
            salary_max=salary_max,
            salary_currency=salary_currency,
            job_type=job_type.replace("_", "-"),
            remote=True,
            url=url,
            apply_url=url,
            tags=tags,
            posted_date=posted_date,
            source=self.SOURCE_NAME,
            external_id=str(item.get("id", "")),
        )
