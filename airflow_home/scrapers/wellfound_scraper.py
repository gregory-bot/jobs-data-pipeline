"""
Wellfound (AngelList Talent) Scraper - wellfound.com
Startup focused job board — uses public job listing pages.
"""
import re
import time
import logging
from typing import Optional

from airflow_home.scrapers.base_scraper import BaseScraper, JobData

logger = logging.getLogger(__name__)


class WellfoundScraper(BaseScraper):
    SOURCE_NAME = "wellfound"
    BASE_URL = "https://wellfound.com"

    # Wellfound uses Next.js / GraphQL internally.
    # We scrape the public listing pages using their search endpoint.
    SEARCH_URL = "https://wellfound.com/jobs"

    def scrape(self, search_query: str = None, location: str = None, max_pages: int = 3) -> list[JobData]:
        jobs = []
        params = {"remote": "true"}
        if search_query:
            params["q"] = search_query
        if location:
            params["l"] = location

        for page in range(1, max_pages + 1):
            params["page"] = page
            soup = self.fetch_page(self.SEARCH_URL, params=params)
            if not soup:
                break

            # Wellfound renders job cards in divs with data-test="StartupResult"
            cards = soup.find_all("div", attrs={"data-test": re.compile(r"StartupResult|JobResult", re.I)})
            if not cards:
                cards = soup.find_all("div", class_=re.compile(r"job-listing|styles_jobList", re.I))
            if not cards:
                # Try JSON-LD
                import json
                for script in soup.find_all("script", type="application/ld+json"):
                    try:
                        data = json.loads(script.string)
                        if isinstance(data, list):
                            for d in data:
                                if d.get("@type") == "JobPosting":
                                    job = self._from_jsonld(d)
                                    if job:
                                        jobs.append(job)
                        elif data.get("@type") == "JobPosting":
                            job = self._from_jsonld(data)
                            if job:
                                jobs.append(job)
                    except Exception:
                        pass
                if not jobs:
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
        title_el = card.find(["h2", "h3", "span"], class_=re.compile(r"title|role", re.I))
        if not title_el:
            title_el = card.find("a")
        if not title_el:
            return None

        title = title_el.get_text(strip=True)
        if not title or len(title) < 3:
            return None

        link = card.find("a", href=re.compile(r"/jobs/|/l/"))
        href = link.get("href", "") if link else ""
        url = href if href.startswith("http") else self.BASE_URL + href if href else None

        company_el = card.find(class_=re.compile(r"company|startup", re.I))
        company = company_el.get_text(strip=True) if company_el else None

        loc_el = card.find(class_=re.compile(r"location|remote", re.I))
        location = loc_el.get_text(strip=True) if loc_el else "Remote"

        return JobData(
            title=title,
            company=company,
            location=location,
            job_type="full-time",
            remote=True,
            url=url,
            apply_url=url,
            source=self.SOURCE_NAME,
            external_id=href.rstrip("/").split("/")[-1] if href else None,
        )

    def _from_jsonld(self, data: dict) -> Optional[JobData]:
        title = data.get("title")
        if not title:
            return None
        company = None
        hiring_org = data.get("hiringOrganization")
        if isinstance(hiring_org, dict):
            company = hiring_org.get("name")
        loc = data.get("jobLocation")
        location = "Remote"
        if isinstance(loc, dict):
            addr = loc.get("address", {})
            location = addr.get("addressLocality") or addr.get("addressCountry") or "Remote"
        url = data.get("url") or data.get("sameAs")
        return JobData(
            title=title,
            company=company,
            location=location,
            remote=True,
            url=url,
            apply_url=url,
            source=self.SOURCE_NAME,
            external_id=url.rstrip("/").split("/")[-1] if url else None,
        )
