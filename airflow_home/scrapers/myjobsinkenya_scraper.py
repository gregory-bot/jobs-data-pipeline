"""
MyJobsInKenya Scraper
Scrapes job listings from myjobsinkenya.com
"""
import logging
import re
import time
from typing import Optional
from datetime import datetime

from airflow_home.scrapers.base_scraper import BaseScraper, JobData

logger = logging.getLogger(__name__)


class MyJobsInKenyaScraper(BaseScraper):
    SOURCE_NAME = "myjobsinkenya"
    BASE_URL = "https://www.myjobsinkenya.com"

    def scrape(
        self, search_query: str = None, location: str = None, max_pages: int = 5
    ) -> list[JobData]:
        jobs = []

        # Jobs are listed on the homepage; search uses /search
        for page in range(1, max_pages + 1):
            if search_query:
                url = f"{self.BASE_URL}/search"
                params = {"q": search_query, "page": page}
            else:
                url = self.BASE_URL
                params = {"page": page} if page > 1 else {}

            soup = self.fetch_page(url, params=params)
            if not soup:
                logger.warning(f"[MyJobsInKenya] No response for page {page}, stopping.")
                break

            # Job cards use class "job-list"
            job_cards = soup.find_all("div", class_="job-list")

            if not job_cards:
                logger.info(f"[MyJobsInKenya] No job cards found on page {page}, stopping.")
                break

            for card in job_cards:
                try:
                    job = self._parse_card(card)
                    if job:
                        jobs.append(job)
                except Exception as e:
                    logger.error(f"[MyJobsInKenya] Error parsing card: {e}")
                    continue

            logger.info(f"[MyJobsInKenya] Page {page}: found {len(job_cards)} cards")
            time.sleep(2)

        logger.info(f"[MyJobsInKenya] Total jobs scraped: {len(jobs)}")
        return jobs

    def _parse_card(self, card) -> Optional[JobData]:
        # Title is in h4 > a
        title_el = card.find("h4")
        if title_el:
            link = title_el.find("a", href=True)
        else:
            link = card.find("a", href=True)
        title = link.get_text(strip=True) if link else None
        if not title:
            return None

        # URL
        url = None
        if link:
            href = link["href"]
            url = href if href.startswith("http") else self.BASE_URL + href

        # Company - span.company
        company_el = card.find("span", class_="company")
        company = None
        if company_el:
            company_link = company_el.find("a")
            company = (company_link or company_el).get_text(strip=True)

        # Location - span.office-location
        location_el = card.find("span", class_="office-location")
        location_text = "Kenya"
        if location_el:
            loc_link = location_el.find("a")
            location_text = (loc_link or location_el).get_text(strip=True)

        # Job type - span.job-type
        job_type_el = card.find("span", class_=re.compile(r"job-type"))
        job_type = None
        if job_type_el:
            jt_link = job_type_el.find("a")
            job_type = (jt_link or job_type_el).get_text(strip=True)

        # External ID from URL slug
        external_id = None
        if url:
            match = re.search(r"/jobs/([^/]+)/view", url)
            if match:
                external_id = match.group(1)

        return JobData(
            title=title,
            company=company,
            location=location_text,
            job_type=job_type,
            url=url,
            apply_url=url,
            source=self.SOURCE_NAME,
            external_id=external_id,
        )
