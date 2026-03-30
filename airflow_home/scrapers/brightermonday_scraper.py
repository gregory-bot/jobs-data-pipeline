"""
BrighterMonday Kenya Scraper
Scrapes job listings from brightermonday.co.ke - one of East Africa's largest job boards.
"""
import logging
import re
import time
from typing import Optional

from airflow_home.scrapers.base_scraper import BaseScraper, JobData

logger = logging.getLogger(__name__)


class BrighterMondayScraper(BaseScraper):
    SOURCE_NAME = "brightermonday"
    BASE_URL = "https://www.brightermonday.co.ke"

    def scrape(
        self, search_query: str = None, location: str = None, max_pages: int = 5
    ) -> list[JobData]:
        jobs = []

        for page in range(1, max_pages + 1):
            url = f"{self.BASE_URL}/jobs"
            params = {"page": page}
            if search_query:
                params["q"] = search_query

            soup = self.fetch_page(url, params=params)
            if not soup:
                break

            # BrighterMonday uses data-cy="listing-cards-components" on each card
            job_cards = soup.find_all(attrs={"data-cy": "listing-cards-components"})
            if not job_cards:
                # Fallback: cards are divs with aria-labelledby containing "job"
                job_cards = soup.find_all("div", attrs={"aria-labelledby": re.compile(r"job")})
            if not job_cards:
                logger.info(f"[BrighterMonday] No cards found on page {page}, stopping.")
                break

            for card in job_cards:
                try:
                    job = self._parse_card(card)
                    if job:
                        jobs.append(job)
                except Exception as e:
                    logger.error(f"[BrighterMonday] Error parsing card: {e}")

            logger.info(f"[BrighterMonday] Page {page}: found {len(job_cards)} cards")
            time.sleep(2)

        logger.info(f"[BrighterMonday] Total jobs scraped: {len(jobs)}")
        return jobs

    def _parse_card(self, card) -> Optional[JobData]:
        # Title link uses data-cy="listing-title-link"
        title_link = card.find("a", attrs={"data-cy": "listing-title-link"})
        if not title_link:
            title_link = card.find("a", href=re.compile(r"/listings/"))
        if not title_link:
            return None

        title_p = title_link.find("p")
        title = (title_p or title_link).get_text(strip=True)
        if not title:
            return None

        # URL
        href = title_link.get("href", "")
        url = href if href.startswith("http") else self.BASE_URL + href

        # Company - first <p> with blue-700 text after the title, or use generic heuristic
        company = None
        company_el = card.find("p", class_=re.compile(r"text-blue-700"))
        if company_el and company_el != (title_link.find("p") if title_link else None):
            company = company_el.get_text(strip=True)

        # Location and job type from tag spans
        location_text = "Kenya"
        job_type = None
        tag_spans = card.find_all("span", class_=re.compile(r"rounded.*bg-brand|bg-brand.*rounded"))
        if not tag_spans:
            tag_spans = card.find_all("span", class_=re.compile(r"rounded"))
        for span in tag_spans:
            text = span.get_text(strip=True)
            if text.lower() in ("full time", "part time", "contract", "freelance", "internship", "temporary"):
                job_type = text
            elif text:
                location_text = text

        # Category/description
        desc_candidates = card.find_all("p", class_=re.compile(r"text-gray-500"))
        description = None
        for p in desc_candidates:
            text = p.get_text(strip=True)
            if text and text != location_text:
                description = text
                break

        # External ID from URL slug
        external_id = None
        if url:
            match = re.search(r"/listings/([^/?]+)", url)
            if match:
                external_id = match.group(1)

        return JobData(
            title=title,
            company=company,
            location=location_text,
            description=description,
            job_type=job_type,
            url=url,
            apply_url=url,
            source=self.SOURCE_NAME,
            external_id=external_id,
        )
