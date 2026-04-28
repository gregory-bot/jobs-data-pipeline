"""
WorkAtAStartup (Y Combinator) Scraper — workatastartup.com
YC startup jobs board. Uses public HTML pages.
"""
import re
import json
import time
import logging
from typing import Optional

from airflow_home.scrapers.base_scraper import BaseScraper, JobData

logger = logging.getLogger(__name__)


class WorkAtAStartupScraper(BaseScraper):
    SOURCE_NAME = "workatastartup"
    BASE_URL = "https://www.workatastartup.com"
    SEARCH_URL = "https://www.workatastartup.com/jobs"

    def scrape(self, search_query: str = None, location: str = None, max_pages: int = 3) -> list[JobData]:
        jobs = []
        params = {}
        if search_query:
            params["query"] = search_query
        if location:
            params["location"] = location

        for page in range(1, max_pages + 1):
            params["page"] = page
            soup = self.fetch_page(self.SEARCH_URL, params=params)
            if not soup:
                break

            # Try embedded JSON data (Next.js __NEXT_DATA__)
            next_data = soup.find("script", id="__NEXT_DATA__")
            if next_data:
                try:
                    data = json.loads(next_data.string)
                    page_jobs = self._parse_nextdata(data)
                    if page_jobs:
                        jobs.extend(page_jobs)
                        logger.info(f"[{self.SOURCE_NAME}] Page {page}: {len(page_jobs)} jobs from __NEXT_DATA__")
                        time.sleep(2)
                        continue
                except (json.JSONDecodeError, KeyError):
                    pass

            # Fallback: HTML parsing
            cards = soup.find_all("div", class_=re.compile(r"job.?card|company.?job|listing", re.I))
            if not cards:
                cards = soup.find_all("a", href=re.compile(r"/jobs/"))
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

    def _parse_nextdata(self, data: dict) -> list[JobData]:
        """Extract jobs from Next.js page props."""
        jobs_list = []
        # Navigate typical structure
        props = data.get("props", {}).get("pageProps", {})
        raw_jobs = props.get("jobs") or props.get("results") or []
        for item in raw_jobs:
            title = item.get("title") or item.get("job_title")
            if not title:
                continue
            company = item.get("company_name") or item.get("company", {}).get("name")
            loc = item.get("location") or "Remote"
            url = item.get("url")
            if not url and item.get("slug"):
                url = f"{self.BASE_URL}/jobs/{item['slug']}"
            remote = item.get("remote", False)
            jobs_list.append(JobData(
                title=title,
                company=company,
                location=loc,
                remote=remote,
                url=url,
                apply_url=url,
                source=self.SOURCE_NAME,
                external_id=str(item.get("id") or item.get("slug") or ""),
            ))
        return jobs_list

    def _parse_card(self, card) -> Optional[JobData]:
        if card.name == "a":
            title = card.get_text(strip=True)
            href = card.get("href", "")
        else:
            title_el = card.find(["h2", "h3", "h4", "a"])
            if not title_el:
                return None
            title = title_el.get_text(strip=True)
            link = card.find("a", href=True)
            href = link.get("href", "") if link else ""

        if not title or len(title) < 3:
            return None

        url = href if href.startswith("http") else self.BASE_URL + href if href else None

        company_el = card.find(class_=re.compile(r"company", re.I))
        company = company_el.get_text(strip=True) if company_el else None

        loc_el = card.find(class_=re.compile(r"location|city", re.I))
        location = loc_el.get_text(strip=True) if loc_el else "Remote"

        return JobData(
            title=title,
            company=company,
            location=location,
            url=url,
            apply_url=url,
            source=self.SOURCE_NAME,
            external_id=href.rstrip("/").split("/")[-1] if href else None,
        )
