"""
Glassdoor Jobs Scraper
Scrapes job listings via Glassdoor search.
"""
import logging
import re
import time
from typing import Optional

from airflow_home.scrapers.base_scraper import BaseScraper, JobData

logger = logging.getLogger(__name__)


class GlassdoorScraper(BaseScraper):
    SOURCE_NAME = "glassdoor"
    BASE_URL = "https://www.glassdoor.com"

    def scrape(
        self, search_query: str = None, location: str = None, max_pages: int = 3
    ) -> list[JobData]:
        jobs = []
        query = search_query or "software engineer"
        loc = location or "Kenya"

        for page in range(1, max_pages + 1):
            url = f"{self.BASE_URL}/Job/jobs.htm"
            params = {
                "sc.keyword": query,
                "locT": "N",
                "locKeyword": loc,
                "p": page,
            }

            soup = self.fetch_page(url, params=params)
            if not soup:
                break

            job_cards = soup.find_all("li", class_=re.compile(r"react-job-listing|jl"))
            if not job_cards:
                job_cards = soup.find_all("div", class_=re.compile(r"jobCard"))
            if not job_cards:
                logger.info(f"[Glassdoor] No cards found on page {page}")
                break

            for card in job_cards:
                try:
                    job = self._parse_card(card)
                    if job:
                        jobs.append(job)
                except Exception as e:
                    logger.error(f"[Glassdoor] Error parsing card: {e}")

            logger.info(f"[Glassdoor] Page {page}: found {len(job_cards)} cards")
            time.sleep(3)

        logger.info(f"[Glassdoor] Total jobs scraped: {len(jobs)}")
        return jobs

    def _parse_card(self, card) -> Optional[JobData]:
        title_el = card.find("a", class_=re.compile(r"jobTitle|job-title"))
        title = title_el.get_text(strip=True) if title_el else None
        if not title:
            return None

        link = card.find("a", href=True)
        url = None
        if link:
            href = link.get("href", "")
            url = href if href.startswith("http") else self.BASE_URL + href

        company_el = card.find("div", class_=re.compile(r"employer|companyName"))
        company = company_el.get_text(strip=True) if company_el else None

        location_el = card.find("span", class_=re.compile(r"loc|location"))
        location_text = location_el.get_text(strip=True) if location_el else None

        salary_el = card.find("span", class_=re.compile(r"salary|compensation"))
        salary_min, salary_max = None, None
        if salary_el:
            salary_text = salary_el.get_text(strip=True)
            numbers = re.findall(r"[\d,]+", salary_text)
            if len(numbers) >= 2:
                salary_min = float(numbers[0].replace(",", ""))
                salary_max = float(numbers[1].replace(",", ""))

        external_id = card.get("data-id") or card.get("data-job-id")

        return JobData(
            title=title,
            company=company,
            location=location_text,
            salary_min=salary_min,
            salary_max=salary_max,
            url=url,
            apply_url=url,
            source=self.SOURCE_NAME,
            external_id=external_id,
        )
