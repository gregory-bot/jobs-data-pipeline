"""
MyJobMag Kenya Scraper - myjobmag.co.ke
Major Kenyan job board with rich structured data.
"""
import re
import time
import logging
from typing import Optional

from airflow_home.scrapers.base_scraper import BaseScraper, JobData

logger = logging.getLogger(__name__)


class MyJobMagScraper(BaseScraper):
    SOURCE_NAME = "myjobmag"
    BASE_URL = "https://www.myjobmag.co.ke"

    def scrape(self, search_query: str = None, location: str = None, max_pages: int = 5) -> list[JobData]:
        jobs = []
        for page in range(1, max_pages + 1):
            url = f"{self.BASE_URL}/jobs-in-kenya"
            params = {"page": page}
            if search_query:
                params["q"] = search_query

            soup = self.fetch_page(url, params=params)
            if not soup:
                break

            # MyJobMag lists jobs in <li> cards inside .job-list or similar
            listings = soup.select("article.job-list, li.job-list-li, div.job-item")
            if not listings:
                listings = soup.find_all("article", class_=re.compile(r"job", re.I))
            if not listings:
                listings = soup.find_all("li", attrs={"data-job-id": True})
            if not listings:
                logger.info(f"[{self.SOURCE_NAME}] No listings on page {page}, stopping")
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
        # Title
        title_el = item.find(["h2", "h3", "h4"], class_=re.compile(r"title|job-name", re.I))
        if not title_el:
            title_el = item.find("a", class_=re.compile(r"title", re.I))
        if not title_el:
            title_el = item.find("a")
        if not title_el:
            return None
        title = title_el.get_text(strip=True)
        if not title or len(title) < 3:
            return None

        # URL
        link = title_el if title_el.name == "a" else title_el.find("a")
        if not link:
            link = item.find("a", href=re.compile(r"/job/"))
        href = link.get("href", "") if link else ""
        url = href if href.startswith("http") else self.BASE_URL + href if href else None

        # Company
        company_el = item.find(class_=re.compile(r"company|employer|recruiter", re.I))
        company = company_el.get_text(strip=True) if company_el else None

        # Location
        loc_el = item.find(class_=re.compile(r"location|city", re.I))
        location = loc_el.get_text(strip=True) if loc_el else "Kenya"

        # Job type
        type_el = item.find(class_=re.compile(r"job.?type|employment.?type", re.I))
        job_type = type_el.get_text(strip=True) if type_el else None

        # Deadline
        deadline_el = item.find(class_=re.compile(r"deadline|closing|expir", re.I))
        deadline_text = deadline_el.get_text(strip=True) if deadline_el else None
        deadline = None
        if deadline_text:
            import datetime
            for fmt in ("%d %b %Y", "%B %d, %Y", "%d/%m/%Y", "%Y-%m-%d"):
                try:
                    deadline = datetime.datetime.strptime(
                        re.sub(r"[Dd]eadline[:\s]*", "", deadline_text).strip(), fmt
                    )
                    break
                except ValueError:
                    continue

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
            application_deadline=deadline,
        )
