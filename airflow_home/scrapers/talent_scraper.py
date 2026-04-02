"""
Talent.com Scraper - talent.com
Global job aggregator with strong Kenya/Africa coverage.
Uses public search results pages.
"""
import re
import time
import logging
from typing import Optional

from airflow_home.scrapers.base_scraper import BaseScraper, JobData

logger = logging.getLogger(__name__)


class TalentScraper(BaseScraper):
    SOURCE_NAME = "talent"
    BASE_URL = "https://www.talent.com"
    SEARCH_URL = "https://www.talent.com/jobs"

    def scrape(self, search_query: str = None, location: str = None, max_pages: int = 5) -> list[JobData]:
        jobs = []
        loc = location or "Kenya"

        for page in range(1, max_pages + 1):
            params = {
                "k": search_query or "",
                "l": loc,
                "p": page,
            }
            soup = self.fetch_page(self.SEARCH_URL, params=params)
            if not soup:
                break

            # Talent.com cards: div with class "card"
            cards = soup.find_all("div", class_=re.compile(r"\bcard\b", re.I))
            if not cards:
                cards = soup.find_all("div", attrs={"data-job-id": True})
            if not cards:
                cards = soup.select(".js-job-list .js-job-item")
            if not cards:
                logger.info(f"[{self.SOURCE_NAME}] No cards on page {page}")
                break

            for card in cards:
                try:
                    job = self._parse_card(card)
                    if job:
                        jobs.append(job)
                except Exception as e:
                    logger.debug(f"[{self.SOURCE_NAME}] Parse error: {e}")

            logger.info(f"[{self.SOURCE_NAME}] Page {page}: {len(cards)} cards")
            time.sleep(2)

        logger.info(f"[{self.SOURCE_NAME}] Total: {len(jobs)} jobs")
        return jobs

    def _parse_card(self, card) -> Optional[JobData]:
        # Title
        title_el = card.find(class_=re.compile(r"title|job.?title", re.I))
        if not title_el:
            title_el = card.find(["h2", "h3"])
        if not title_el:
            return None
        title = title_el.get_text(strip=True)
        if not title or len(title) < 3:
            return None

        # URL
        link = card.find("a", href=re.compile(r"/jobs?/|/position/|/search\?"))
        if not link:
            link = card.find("a")
        href = link.get("href", "") if link else ""
        url = href if href.startswith("http") else self.BASE_URL + href if href else None

        # Company
        company_el = card.find(class_=re.compile(r"company|employer", re.I))
        company = company_el.get_text(strip=True) if company_el else None

        # Location
        loc_el = card.find(class_=re.compile(r"location|city", re.I))
        location = loc_el.get_text(strip=True) if loc_el else None

        # Salary
        salary_el = card.find(class_=re.compile(r"salary|pay|wage", re.I))
        salary_text = salary_el.get_text(strip=True) if salary_el else None
        salary_min = salary_max = None
        salary_currency = "KES"
        if salary_text:
            if "$" in salary_text:
                salary_currency = "USD"
            nums = re.findall(r"\d[\d,]*", salary_text.replace(",", ""))
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

        # Job type
        type_el = card.find(class_=re.compile(r"type|contract|employment", re.I))
        job_type = type_el.get_text(strip=True) if type_el else None

        # Description snippet
        desc_el = card.find(class_=re.compile(r"desc|snippet|summary", re.I))
        description = desc_el.get_text(strip=True) if desc_el else None

        return JobData(
            title=title,
            company=company,
            location=location,
            description=description,
            salary_min=salary_min,
            salary_max=salary_max,
            salary_currency=salary_currency,
            job_type=job_type,
            url=url,
            apply_url=url,
            source=self.SOURCE_NAME,
            external_id=card.get("data-job-id") or (href.rstrip("/").split("/")[-1] if href else None),
        )
