"""
RemoteOK Scraper - remoteok.com
Uses official JSON API endpoint.  Rate-limit friendly — returns all jobs in one call.
"""
import time
import logging
from typing import Optional
import datetime

from airflow_home.scrapers.base_scraper import BaseScraper, JobData

logger = logging.getLogger(__name__)


class RemoteOKScraper(BaseScraper):
    SOURCE_NAME = "remoteok"
    API_URL = "https://remoteok.com/api"

    def scrape(self, search_query: str = None, location: str = None, max_pages: int = 5) -> list[JobData]:
        """RemoteOK provides a public JSON API — one call returns all listings."""
        jobs = []
        # API requires a non-browser user agent but still honors robots.txt
        headers = {
            "User-Agent": "Annex Jobs Aggregator/1.0 (contact: admin@annex.jobs)",
            "Accept": "application/json",
        }
        data = self.fetch_json(self.API_URL, headers=headers)
        if not data or not isinstance(data, list):
            logger.warning(f"[{self.SOURCE_NAME}] No data from API")
            return jobs

        # First element is metadata, skip it
        for item in data[1:]:
            try:
                job = self._parse_item(item)
                if job:
                    jobs.append(job)
            except Exception as e:
                logger.debug(f"[{self.SOURCE_NAME}] Parse error: {e}")

        # Optional: filter by query
        if search_query:
            q = search_query.lower()
            jobs = [j for j in jobs if q in (j.title or "").lower() or q in (j.tags or "").lower()]

        logger.info(f"[{self.SOURCE_NAME}] Total: {len(jobs)} jobs")
        time.sleep(1)
        return jobs

    def _parse_item(self, item: dict) -> Optional[JobData]:
        title = item.get("position") or item.get("title")
        if not title:
            return None

        company = item.get("company")
        url = item.get("url") or f"https://remoteok.com/l/{item.get('slug', '')}"
        apply_url = item.get("apply_url") or url

        tags_list = item.get("tags", [])
        tags = ",".join(tags_list) if isinstance(tags_list, list) else str(tags_list)

        description = item.get("description", "")

        # posted epoch
        posted_date = None
        epoch = item.get("date") or item.get("epoch")
        if epoch:
            try:
                posted_date = datetime.datetime.utcfromtimestamp(int(epoch))
            except (ValueError, TypeError):
                pass

        salary_min = salary_max = None
        salary_currency = "USD"
        salary_str = item.get("salary", "") or ""
        import re
        nums = re.findall(r"\d[\d,]*", salary_str.replace(",", ""))
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

        return JobData(
            title=title,
            company=company,
            location="Remote",
            description=description,
            salary_min=salary_min,
            salary_max=salary_max,
            salary_currency=salary_currency,
            job_type="remote",
            remote=True,
            url=url,
            apply_url=apply_url,
            tags=tags,
            posted_date=posted_date,
            source=self.SOURCE_NAME,
            external_id=str(item.get("id") or item.get("slug") or ""),
        )
