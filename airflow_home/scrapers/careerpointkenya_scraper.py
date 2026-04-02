"""
CareerPoint Kenya Scraper - careerpointkenya.co.ke
Kenyan HR & recruitment firm with job listings.
"""
import re
import time
import logging
from typing import Optional

from airflow_home.scrapers.base_scraper import BaseScraper, JobData

logger = logging.getLogger(__name__)


class CareerPointKenyaScraper(BaseScraper):
    SOURCE_NAME = "careerpointkenya"
    BASE_URL = "https://www.careerpointkenya.co.ke"

    def scrape(self, search_query: str = None, location: str = None, max_pages: int = 5) -> list[JobData]:
        jobs = []
        for page in range(1, max_pages + 1):
            url = f"{self.BASE_URL}/jobs"
            params = {"page": page}
            if search_query:
                params["s"] = search_query

            soup = self.fetch_page(url, params=params)
            if not soup:
                break

            listings = soup.select(".job-listing, .job, article")
            if not listings:
                listings = soup.find_all(["div", "article"], class_=re.compile(r"job.?(listing|card|item|post)", re.I))
            if not listings:
                logger.info(f"[{self.SOURCE_NAME}] No listings on page {page}")
                break

            for item in listings:
                try:
                    job = self._parse_item(item)
                    if job:
                        jobs.append(job)
                except Exception as e:
                    logger.debug(f"[{self.SOURCE_NAME}] Parse error: {e}")

            logger.info(f"[{self.SOURCE_NAME}] Page {page}: {len(listings)} listings")
            time.sleep(2)

        logger.info(f"[{self.SOURCE_NAME}] Total: {len(jobs)} jobs")
        return jobs

    def _parse_item(self, item) -> Optional[JobData]:
        title_el = item.find(["h2", "h3", "h4"], class_=re.compile(r"title|heading", re.I))
        if not title_el:
            title_el = item.find("a")
        if not title_el:
            return None

        title = title_el.get_text(strip=True)
        if not title or len(title) < 3:
            return None

        link = title_el if title_el.name == "a" else title_el.find("a")
        if not link:
            link = item.find("a")
        href = link.get("href", "") if link else ""
        url = href if href.startswith("http") else self.BASE_URL + href if href else None

        company_el = item.find(class_=re.compile(r"company|employer|client", re.I))
        company = company_el.get_text(strip=True) if company_el else "CareerPoint Kenya"

        loc_el = item.find(class_=re.compile(r"location|city", re.I))
        location = loc_el.get_text(strip=True) if loc_el else "Kenya"

        type_el = item.find(class_=re.compile(r"type|contract|employment", re.I))
        job_type = type_el.get_text(strip=True) if type_el else None

        external_id = url.rstrip("/").split("/")[-1] if url else None

        return JobData(
            title=title,
            company=company,
            location=location,
            url=url,
            apply_url=url,
            job_type=job_type,
            source=self.SOURCE_NAME,
            external_id=external_id,
        )
