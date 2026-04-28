"""
WeWorkRemotely Scraper - weworkremotely.com
One of the largest dedicated remote job boards.
"""
import re
import time
import logging
import datetime
from typing import Optional

from airflow_home.scrapers.base_scraper import BaseScraper, JobData

logger = logging.getLogger(__name__)

# Categories on WWR that are worth scraping
WWR_CATEGORIES = [
    "/categories/remote-programming-jobs",
    "/categories/remote-devops-sysadmin-jobs",
    "/categories/remote-design-jobs",
    "/categories/remote-marketing-jobs",
    "/categories/remote-management-finance-jobs",
    "/categories/remote-customer-support-jobs",
    "/categories/remote-writing-editing-jobs",
    "/categories/remote-data-analytics-jobs",
]


class WeWorkRemotelyScraper(BaseScraper):
    SOURCE_NAME = "weworkremotely"
    BASE_URL = "https://weworkremotely.com"

    def scrape(self, search_query: str = None, location: str = None, max_pages: int = 5) -> list[JobData]:
        jobs = []

        if search_query:
            # Use search endpoint
            url = f"{self.BASE_URL}/remote-jobs/search"
            soup = self.fetch_page(url, params={"term": search_query})
            if soup:
                jobs.extend(self._extract_jobs_from_page(soup))
        else:
            # Scrape top categories
            categories_to_scrape = WWR_CATEGORIES[:max(1, min(max_pages, len(WWR_CATEGORIES)))]
            for cat_path in categories_to_scrape:
                soup = self.fetch_page(self.BASE_URL + cat_path)
                if soup:
                    page_jobs = self._extract_jobs_from_page(soup)
                    jobs.extend(page_jobs)
                    logger.info(f"[{self.SOURCE_NAME}] {cat_path}: {len(page_jobs)} jobs")
                time.sleep(2)

        logger.info(f"[{self.SOURCE_NAME}] Total: {len(jobs)} jobs")
        return jobs

    def _extract_jobs_from_page(self, soup) -> list[JobData]:
        jobs = []
        # WWR uses <li> inside <ul class="jobs">
        job_items = soup.select("ul.jobs li")
        if not job_items:
            job_items = soup.find_all("li", class_=re.compile(r"feature|listing", re.I))

        for item in job_items:
            try:
                job = self._parse_item(item)
                if job:
                    jobs.append(job)
            except Exception as e:
                logger.debug(f"[{self.SOURCE_NAME}] Parse error: {e}")
        return jobs

    def _parse_item(self, item) -> Optional[JobData]:
        # Skip section headers
        if "flag" in (item.get("class") or []) or item.find("a", class_="flag"):
            return None

        link = item.find("a", href=re.compile(r"/remote-jobs/"))
        if not link:
            return None

        href = link.get("href", "")
        url = href if href.startswith("http") else self.BASE_URL + href

        # Title is in span with class "title"
        title_el = item.find(class_=re.compile(r"title", re.I))
        if not title_el:
            title_el = item.find(["h2", "h3", "span"])
        title = title_el.get_text(strip=True) if title_el else link.get_text(strip=True)
        if not title or len(title) < 3:
            return None

        # Company in <span class="company">
        company_el = item.find(class_=re.compile(r"company", re.I))
        company = company_el.get_text(strip=True) if company_el else None

        # Region/location
        region_el = item.find(class_=re.compile(r"region|location", re.I))
        location = region_el.get_text(strip=True) if region_el else "Remote"

        external_id = href.rstrip("/").split("/")[-1] if href else None

        return JobData(
            title=title,
            company=company,
            location=location or "Remote",
            job_type="remote",
            remote=True,
            url=url,
            apply_url=url,
            source=self.SOURCE_NAME,
            external_id=external_id,
        )
