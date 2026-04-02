"""
Advance Africa Kenya Jobs Scraper - advance-africa.com/jobs-in-kenya.html
Africa-focused job board with Kenya listings.
"""
import re
import time
import logging
from typing import Optional

from airflow_home.scrapers.base_scraper import BaseScraper, JobData

logger = logging.getLogger(__name__)


class AdvanceAfricaScraper(BaseScraper):
    SOURCE_NAME = "advanceafrica"
    BASE_URL = "https://www.advance-africa.com"
    START_URL = "https://www.advance-africa.com/jobs-in-kenya.html"

    def scrape(self, search_query: str = None, location: str = None, max_pages: int = 5) -> list[JobData]:
        jobs = []
        for page in range(1, max_pages + 1):
            url = self.START_URL if page == 1 else f"{self.BASE_URL}/jobs-in-kenya-{page}.html"
            soup = self.fetch_page(url)
            if not soup:
                break

            listings = soup.find_all(["div", "li", "article"], class_=re.compile(r"job|vacancy|listing", re.I))
            if not listings:
                # Try generic link lists
                listings = [a for a in soup.find_all("a", href=re.compile(r"jobs-in-kenya|/job/|/vacancy/")) if a.get_text(strip=True)]
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
        if item.name == "a":
            title = item.get_text(strip=True)
            url = item.get("href", "")
            url = url if url.startswith("http") else self.BASE_URL + url
            if not title or len(title) < 3:
                return None
            return JobData(
                title=title,
                company=None,
                location="Kenya",
                url=url,
                apply_url=url,
                source=self.SOURCE_NAME,
                external_id=url.rstrip("/").split("/")[-1],
            )

        title_el = item.find(["h2", "h3", "h4", "a"], class_=re.compile(r"title|job", re.I))
        if not title_el:
            title_el = item.find("a")
        if not title_el:
            return None

        title = title_el.get_text(strip=True)
        if not title or len(title) < 3:
            return None

        link = title_el if title_el.name == "a" else title_el.find("a")
        href = link.get("href", "") if link else ""
        url = href if href.startswith("http") else self.BASE_URL + href if href else None

        company_el = item.find(class_=re.compile(r"company|employer|org", re.I))
        company = company_el.get_text(strip=True) if company_el else None

        loc_el = item.find(class_=re.compile(r"location|city", re.I))
        location = loc_el.get_text(strip=True) if loc_el else "Kenya"

        return JobData(
            title=title,
            company=company,
            location=location,
            url=url,
            apply_url=url,
            source=self.SOURCE_NAME,
            external_id=url.rstrip("/").split("/")[-1] if url else None,
        )
