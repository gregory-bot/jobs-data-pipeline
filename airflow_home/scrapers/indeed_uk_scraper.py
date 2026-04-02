"""
Indeed UK Scraper - indeed.co.uk
Remote jobs from UK version of Indeed, which often lists globally available remote roles.
"""
import re
import time
import logging
from typing import Optional

from airflow_home.scrapers.base_scraper import BaseScraper, JobData

logger = logging.getLogger(__name__)


class IndeedUKScraper(BaseScraper):
    SOURCE_NAME = "indeed_uk"
    BASE_URL = "https://www.indeed.co.uk"

    def scrape(self, search_query: str = None, location: str = None, max_pages: int = 3) -> list[JobData]:
        jobs = []
        query = search_query or "remote developer"
        loc = location or "Remote"

        for page in range(max_pages):
            start = page * 10
            params = {"q": query, "l": loc, "start": start, "sort": "date"}

            soup = self.fetch_page(f"{self.BASE_URL}/jobs", params=params)
            if not soup:
                break

            cards = soup.find_all("div", class_=re.compile(r"job_seen_beacon|jobsearch-SerpJobCard|result"))
            if not cards:
                cards = soup.find_all("a", attrs={"data-jk": True})
            if not cards:
                logger.info(f"[{self.SOURCE_NAME}] No cards on page {page + 1}")
                break

            for card in cards:
                try:
                    job = self._parse_card(card)
                    if job:
                        jobs.append(job)
                except Exception as e:
                    logger.debug(f"[{self.SOURCE_NAME}] Parse error: {e}")

            logger.info(f"[{self.SOURCE_NAME}] Page {page + 1}: {len(cards)} cards")
            time.sleep(2)

        logger.info(f"[{self.SOURCE_NAME}] Total: {len(jobs)} jobs")
        return jobs

    def _parse_card(self, card) -> Optional[JobData]:
        title_el = card.find(["h2", "h3"], class_=re.compile(r"title|jobTitle", re.I))
        if not title_el:
            title_el = card.find("span", attrs={"title": True})
        if not title_el:
            return None

        title = title_el.get_text(strip=True) if hasattr(title_el, 'get_text') else title_el.get("title", "")
        if not title or len(title) < 3:
            return None

        # URL
        link = card.find("a", href=re.compile(r"/rc/clk|/viewjob|/pagead"))
        if not link:
            link = card.find("a", href=True)
        href = link.get("href", "") if link else ""
        url = href if href.startswith("http") else self.BASE_URL + href if href else None

        # Company
        company_el = card.find(class_=re.compile(r"company|companyName", re.I))
        company = company_el.get_text(strip=True) if company_el else None

        # Location
        loc_el = card.find(class_=re.compile(r"location|companyLocation", re.I))
        location = loc_el.get_text(strip=True) if loc_el else None

        # Salary
        salary_el = card.find(class_=re.compile(r"salary|estimated|pay", re.I))
        salary_text = salary_el.get_text(strip=True) if salary_el else None
        salary_min = salary_max = None
        salary_currency = "GBP"
        if salary_text:
            if "$" in salary_text:
                salary_currency = "USD"
            nums = re.findall(r"\d[\d,]*", salary_text.replace(",", ""))
            if len(nums) >= 2:
                try:
                    salary_min, salary_max = float(nums[0]), float(nums[1])
                except ValueError:
                    pass

        jk = card.get("data-jk") or (link.get("data-jk") if link else None)
        external_id = jk or (href.rstrip("/").split("/")[-1] if href else None)

        return JobData(
            title=title,
            company=company,
            location=location,
            salary_min=salary_min,
            salary_max=salary_max,
            salary_currency=salary_currency,
            url=url,
            apply_url=url,
            source=self.SOURCE_NAME,
            external_id=external_id,
        )
