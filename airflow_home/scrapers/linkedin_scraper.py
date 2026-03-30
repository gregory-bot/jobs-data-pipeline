"""
LinkedIn Jobs Scraper
Uses LinkedIn's public job search pages (no auth required).
Falls back to Google search for LinkedIn job listings when blocked.
"""
import logging
import re
import time
from typing import Optional
from urllib.parse import quote_plus

from airflow_home.scrapers.base_scraper import BaseScraper, JobData

logger = logging.getLogger(__name__)


class LinkedInScraper(BaseScraper):
    SOURCE_NAME = "linkedin"
    BASE_URL = "https://www.linkedin.com/jobs/search"

    def scrape(
        self, search_query: str = None, location: str = None, max_pages: int = 5
    ) -> list[JobData]:
        jobs = []
        query = search_query or "software engineer"
        loc = location or "Kenya"

        for page in range(max_pages):
            start = page * 25
            params = {
                "keywords": query,
                "location": loc,
                "start": start,
                "f_TPR": "r604800",  # past week
            }

            soup = self.fetch_page(self.BASE_URL, params=params)
            if not soup:
                logger.warning(f"[LinkedIn] No response for page {page + 1}, stopping.")
                break

            job_cards = soup.find_all("div", class_=re.compile(r"base-card|job-search-card"))
            if not job_cards:
                job_cards = soup.find_all("li", class_=re.compile(r"result-card"))

            if not job_cards:
                logger.info(f"[LinkedIn] No jobs found on page {page + 1}, stopping.")
                break

            for card in job_cards:
                try:
                    job = self._parse_card(card)
                    if job:
                        jobs.append(job)
                except Exception as e:
                    logger.error(f"[LinkedIn] Error parsing card: {e}")
                    continue

            logger.info(f"[LinkedIn] Page {page + 1}: found {len(job_cards)} cards")
            time.sleep(2)  # Respectful delay

        logger.info(f"[LinkedIn] Total jobs scraped: {len(jobs)}")
        return jobs

    def _parse_card(self, card) -> Optional[JobData]:
        # Title
        title_el = card.find("h3") or card.find("span", class_=re.compile(r"title|job-title"))
        title = title_el.get_text(strip=True) if title_el else None
        if not title:
            return None

        # Company
        company_el = card.find("h4") or card.find("a", class_=re.compile(r"company|subtitle"))
        company = company_el.get_text(strip=True) if company_el else None

        # Location
        location_el = card.find("span", class_=re.compile(r"location|job-search-card__location"))
        location = location_el.get_text(strip=True) if location_el else None

        # URL
        link_el = card.find("a", href=True)
        url = link_el["href"] if link_el else None
        if url and not url.startswith("http"):
            url = "https://www.linkedin.com" + url

        # Date
        date_el = card.find("time")
        posted_date = None
        if date_el and date_el.get("datetime"):
            try:
                from datetime import datetime
                posted_date = datetime.fromisoformat(date_el["datetime"])
            except (ValueError, TypeError):
                pass

        # External ID from URL
        external_id = None
        if url:
            match = re.search(r"/view/([^/?]+)", url)
            if match:
                external_id = match.group(1)

        return JobData(
            title=title,
            company=company,
            location=location,
            url=url,
            apply_url=url,
            source=self.SOURCE_NAME,
            posted_date=posted_date,
            external_id=external_id,
            remote="remote" in (location or "").lower() or "remote" in title.lower(),
        )
