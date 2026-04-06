"""
BambooHR Scraper - Scrapes jobs from BambooHR public career pages.
Uses /careers/list JSON for listing + /careers/{id}/detail JSON for full descriptions.
"""
import re
import time
import logging
import datetime
from typing import Optional

from bs4 import BeautifulSoup

from airflow_home.scrapers.base_scraper import BaseScraper, JobData

logger = logging.getLogger(__name__)

# Companies with BambooHR career pages (add more as discovered)
BAMBOOHR_COMPANIES = [
    "delta40",
]


class BambooHRScraper(BaseScraper):
    SOURCE_NAME = "bamboohr"

    def scrape(self, search_query: str = None, location: str = None, max_pages: int = 5) -> list[JobData]:
        """Scrape jobs from all configured BambooHR company pages."""
        all_jobs = []

        for company_slug in BAMBOOHR_COMPANIES:
            try:
                jobs = self._scrape_company(company_slug)
                all_jobs.extend(jobs)
            except Exception as e:
                logger.error(f"[{self.SOURCE_NAME}] Error scraping {company_slug}: {e}")
            time.sleep(1)

        logger.info(f"[{self.SOURCE_NAME}] Total: {len(all_jobs)} jobs from {len(BAMBOOHR_COMPANIES)} companies")
        return all_jobs

    def _scrape_company(self, company_slug: str) -> list[JobData]:
        """Scrape all jobs from a single BambooHR company."""
        list_url = f"https://{company_slug}.bamboohr.com/careers/list"
        data = self.fetch_json(list_url)
        if not data or "result" not in data:
            logger.warning(f"[{self.SOURCE_NAME}] No data from {company_slug}")
            return []

        jobs = []
        for item in data["result"]:
            try:
                job = self._parse_job(company_slug, item)
                if job:
                    jobs.append(job)
                time.sleep(0.5)
            except Exception as e:
                logger.debug(f"[{self.SOURCE_NAME}] Parse error for {item.get('id')}: {e}")

        logger.info(f"[{self.SOURCE_NAME}] {company_slug}: {len(jobs)} jobs")
        return jobs

    @staticmethod
    def _html_to_text(html: str) -> str:
        """Convert HTML content to clean plain text."""
        if not html:
            return ""
        soup = BeautifulSoup(html, "lxml")
        return soup.get_text("\n", strip=True)

    def _parse_job(self, company_slug: str, item: dict) -> Optional[JobData]:
        """Parse a single job from the list + fetch full description via detail API."""
        job_id = item.get("id")
        title = (item.get("jobOpeningName") or "").strip()
        if not title or not job_id:
            return None

        company = (item.get("departmentLabel") or "").strip() or company_slug.title()
        job_type = item.get("employmentStatusLabel", "")
        loc = item.get("location", {})
        city = loc.get("city", "")
        state = loc.get("state", "")
        location_str = f"{city}, {state}".strip(", ") if city else state or ""

        location_type = str(item.get("locationType", "0"))
        is_remote = location_type == "1"
        if location_type == "2" and location_str:
            location_str += " (Hybrid)"

        # Fetch full details from /careers/{id}/detail JSON API
        detail_url = f"https://{company_slug}.bamboohr.com/careers/{job_id}/detail"
        detail_data = self.fetch_json(detail_url)

        description = ""
        requirements = ""
        posted_date = None

        if detail_data and "result" in detail_data:
            opening = detail_data["result"].get("jobOpening", {})
            raw_html = opening.get("description", "")
            full_text = self._html_to_text(raw_html)

            # Split into description and requirements
            desc_parts = []
            req_parts = []
            is_req = False
            for line in full_text.split("\n"):
                line = line.strip()
                if not line:
                    continue
                lower = line.lower()
                if any(kw in lower for kw in [
                    "requirement", "qualification", "what we're looking for",
                    "what you'll need", "you should have", "what we need",
                ]):
                    is_req = True
                if is_req:
                    req_parts.append(line)
                else:
                    desc_parts.append(line)

            description = "\n".join(desc_parts)
            requirements = "\n".join(req_parts)

            date_str = opening.get("datePosted")
            if date_str:
                try:
                    posted_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
                except ValueError:
                    pass

        if not description or len(description) < 30:
            logger.debug(f"[{self.SOURCE_NAME}] Skipping {title} - no description")
            return None

        career_url = f"https://{company_slug}.bamboohr.com/careers/{job_id}"

        return JobData(
            title=title,
            source=self.SOURCE_NAME,
            company=company,
            location=location_str,
            description=description,
            job_type=job_type,
            remote=is_remote,
            url=career_url,
            apply_url=career_url,
            requirements=requirements,
            external_id=f"bamboohr_{company_slug}_{job_id}",
            posted_date=posted_date,
        )
