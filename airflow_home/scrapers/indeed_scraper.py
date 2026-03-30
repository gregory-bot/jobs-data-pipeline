"""
Indeed Jobs Scraper
Scrapes job listings from Indeed Kenya / global.
"""
import logging
import re
import time
from typing import Optional
from datetime import datetime

from airflow_home.scrapers.base_scraper import BaseScraper, JobData

logger = logging.getLogger(__name__)


class IndeedScraper(BaseScraper):
    SOURCE_NAME = "indeed"
    BASE_URL = "https://www.indeed.com"  # No Kenya subdomain; use location filter

    def scrape(
        self, search_query: str = None, location: str = None, max_pages: int = 5
    ) -> list[JobData]:
        jobs = []
        query = search_query or ""
        loc = location or "Kenya"

        for page in range(max_pages):
            start = page * 10
            params = {
                "q": query,
                "l": loc,
                "start": start,
                "fromage": 7,  # last 7 days
            }

            soup = self.fetch_page(f"{self.BASE_URL}/jobs", params=params)
            if not soup:
                break

            job_cards = soup.find_all("div", class_=re.compile(r"job_seen_beacon|cardOutline|result"))
            if not job_cards:
                job_cards = soup.find_all("td", id=re.compile(r"resultsCol"))
            if not job_cards:
                logger.info(f"[Indeed] No cards found on page {page + 1}, stopping.")
                break

            for card in job_cards:
                try:
                    job = self._parse_card(card)
                    if job:
                        jobs.append(job)
                except Exception as e:
                    logger.error(f"[Indeed] Error parsing card: {e}")

            logger.info(f"[Indeed] Page {page + 1}: found {len(job_cards)} cards")
            time.sleep(3)

        logger.info(f"[Indeed] Total jobs scraped: {len(jobs)}")
        return jobs

    def _parse_card(self, card) -> Optional[JobData]:
        title_el = card.find("h2", class_=re.compile(r"jobTitle|title"))
        if not title_el:
            title_el = card.find("a", class_=re.compile(r"title|jcs-JobTitle"))
        title = title_el.get_text(strip=True) if title_el else None
        if not title:
            return None

        link = card.find("a", href=True)
        url = None
        if link:
            href = link.get("href", "")
            if href.startswith("/"):
                url = f"{self.BASE_URL}{href}"
            elif href.startswith("http"):
                url = href

        company_el = card.find("span", class_=re.compile(r"company|companyName"))
        company = company_el.get_text(strip=True) if company_el else None

        location_el = card.find("div", class_=re.compile(r"companyLocation|location"))
        location_text = location_el.get_text(strip=True) if location_el else None

        snippet_el = card.find("div", class_=re.compile(r"job-snippet|summary"))
        description = snippet_el.get_text(strip=True) if snippet_el else None

        # Salary
        salary_el = card.find("div", class_=re.compile(r"salary|estimated-salary"))
        salary_min, salary_max = None, None
        if salary_el:
            salary_text = salary_el.get_text(strip=True)
            numbers = re.findall(r"[\d,]+", salary_text)
            if len(numbers) >= 2:
                salary_min = float(numbers[0].replace(",", ""))
                salary_max = float(numbers[1].replace(",", ""))
            elif len(numbers) == 1:
                salary_min = float(numbers[0].replace(",", ""))

        # Job ID
        external_id = card.get("data-jk") or card.get("id")

        return JobData(
            title=title,
            company=company,
            location=location_text,
            description=description,
            salary_min=salary_min,
            salary_max=salary_max,
            salary_currency="KES",
            url=url,
            apply_url=url,
            source=self.SOURCE_NAME,
            external_id=external_id,
        )
