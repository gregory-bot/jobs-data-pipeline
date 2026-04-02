"""
Pigiame Jobs Scraper - pigiame.co.ke/jobs
Kenya classifieds site with jobs section.
"""
import re
import time
import logging
from typing import Optional

from airflow_home.scrapers.base_scraper import BaseScraper, JobData

logger = logging.getLogger(__name__)


class PigiameScraper(BaseScraper):
    SOURCE_NAME = "pigiame"
    BASE_URL = "https://www.pigiame.co.ke"

    def scrape(self, search_query: str = None, location: str = None, max_pages: int = 5) -> list[JobData]:
        jobs = []
        for page in range(1, max_pages + 1):
            url = f"{self.BASE_URL}/jobs"
            params = {"page": page}
            if search_query:
                params["q"] = search_query

            soup = self.fetch_page(url, params=params)
            if not soup:
                break

            # Pigiame listings: article tags or divs with ad-listing class
            listings = soup.find_all("article", class_=re.compile(r"listing|ad", re.I))
            if not listings:
                listings = soup.find_all("div", class_=re.compile(r"listing.?item|ad.?card", re.I))
            if not listings:
                listings = soup.select(".advert-list li, .listings li")
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
        title_el = item.find(["h2", "h3", "h4"])
        if not title_el:
            title_el = item.find("a", class_=re.compile(r"title", re.I))
        if not title_el:
            return None

        title = title_el.get_text(strip=True)
        if not title or len(title) < 3:
            return None

        # Often the title itself is a link or wrapped in one
        link = item.find("a", href=re.compile(r"/jobs?/|/advert/"))
        if not link:
            link = item.find("a")
        href = link.get("href", "") if link else ""
        url = href if href.startswith("http") else self.BASE_URL + href if href else None

        # Pigiame typically shows price/salary in .price
        salary_el = item.find(class_=re.compile(r"price|salary", re.I))
        salary_text = salary_el.get_text(strip=True) if salary_el else None

        # Location
        loc_el = item.find(class_=re.compile(r"location|area|city", re.I))
        location = loc_el.get_text(strip=True) if loc_el else "Kenya"

        # Description snippet
        desc_el = item.find(class_=re.compile(r"desc|snippet|summary", re.I))
        description = desc_el.get_text(strip=True) if desc_el else None

        external_id = url.rstrip("/").split("/")[-1] if url else None

        # Parse salary
        salary_min = salary_max = None
        if salary_text:
            nums = re.findall(r"[\d,]+", salary_text.replace(",", ""))
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
            company=None,
            location=location,
            description=description,
            url=url,
            apply_url=url,
            salary_min=salary_min,
            salary_max=salary_max,
            salary_currency="KES",
            source=self.SOURCE_NAME,
            external_id=external_id,
        )
