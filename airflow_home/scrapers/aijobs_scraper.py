"""
AI Jobs Scraper - ai-jobs.net
Dedicated board for AI/ML/Data Science roles worldwide.
Uses public JSON API.
"""
import time
import logging
import datetime
from typing import Optional

from airflow_home.scrapers.base_scraper import BaseScraper, JobData

logger = logging.getLogger(__name__)


class AIJobsScraper(BaseScraper):
    SOURCE_NAME = "aijobs"
    API_URL = "https://ai-jobs.net/api/jobs.json"

    def scrape(self, search_query: str = None, location: str = None, max_pages: int = 5) -> list[JobData]:
        """ai-jobs.net provides a public JSON feed."""
        jobs = []
        data = self.fetch_json(self.API_URL)
        if not data or not isinstance(data, list):
            logger.warning(f"[{self.SOURCE_NAME}] No data from API")
            return jobs

        for item in data:
            try:
                job = self._parse_item(item)
                if job:
                    # Optional filter by query
                    if search_query and search_query.lower() not in (job.title or "").lower():
                        continue
                    jobs.append(job)
            except Exception as e:
                logger.debug(f"[{self.SOURCE_NAME}] Parse error: {e}")

        logger.info(f"[{self.SOURCE_NAME}] Total: {len(jobs)} jobs")
        time.sleep(1)
        return jobs

    def _parse_item(self, item: dict) -> Optional[JobData]:
        import re
        title = item.get("job_title")
        if not title:
            return None

        company = item.get("company_name")
        url = item.get("url") or item.get("apply_url")
        location = item.get("job_location") or "Remote"
        description = item.get("job_description") or ""
        remote = item.get("remote") in (True, 1, "true", "1", "yes")
        if location.lower() == "remote" or "anywhere" in location.lower():
            remote = True

        salary_min = item.get("salary_from")
        salary_max = item.get("salary_to")
        salary_currency = item.get("salary_currency") or "USD"
        if salary_min:
            try:
                salary_min = float(salary_min)
            except (ValueError, TypeError):
                salary_min = None
        if salary_max:
            try:
                salary_max = float(salary_max)
            except (ValueError, TypeError):
                salary_max = None

        posted_date = None
        pub_str = item.get("published") or item.get("created_at")
        if pub_str:
            for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
                try:
                    posted_date = datetime.datetime.strptime(pub_str[:19], fmt)
                    break
                except ValueError:
                    pass

        tags_list = item.get("job_skills") or []
        tags = ",".join(tags_list) if isinstance(tags_list, list) else str(tags_list)

        job_type_raw = item.get("job_type") or ""
        job_type = job_type_raw.lower().replace("_", "-") if job_type_raw else None

        return JobData(
            title=title,
            company=company,
            location=location,
            description=description,
            salary_min=salary_min,
            salary_max=salary_max,
            salary_currency=salary_currency,
            job_type=job_type,
            remote=remote,
            url=url,
            apply_url=url,
            tags=tags,
            posted_date=posted_date,
            source=self.SOURCE_NAME,
            external_id=str(item.get("id") or ""),
        )
