"""
Fuzu Kenya Scraper
Scrapes job listings from fuzu.com (popular in East Africa).
"""
import logging
import re
import time
from typing import Optional

from airflow_home.scrapers.base_scraper import BaseScraper, JobData

logger = logging.getLogger(__name__)


class FuzuScraper(BaseScraper):
    SOURCE_NAME = "fuzu"
    BASE_URL = "https://www.fuzu.com"

    def scrape(
        self, search_query: str = None, location: str = None, max_pages: int = 5
    ) -> list[JobData]:
        jobs = []

        for page in range(1, max_pages + 1):
            # Correct URL: /kenya/job (singular)
            url = f"{self.BASE_URL}/kenya/job"
            params = {"page": page}
            if search_query:
                params["q"] = search_query

            soup = self.fetch_page(url, params=params)
            if not soup:
                break

            # Fuzu embeds job data as JSON-LD structured data
            json_ld_jobs = self._parse_json_ld(soup)
            if json_ld_jobs:
                jobs.extend(json_ld_jobs)
                logger.info(f"[Fuzu] Page {page}: found {len(json_ld_jobs)} jobs via JSON-LD")
            else:
                # Fallback: try HTML parsing
                job_cards = soup.find_all("div", class_=re.compile(r"job-card|job-listing"))
                if not job_cards:
                    job_cards = soup.find_all("a", href=re.compile(r"/kenya/jobs/"))
                if not job_cards:
                    logger.info(f"[Fuzu] No cards found on page {page}")
                    break

                for card in job_cards:
                    try:
                        job = self._parse_card(card)
                        if job:
                            jobs.append(job)
                    except Exception as e:
                        logger.error(f"[Fuzu] Error parsing card: {e}")

                logger.info(f"[Fuzu] Page {page}: found {len(job_cards)} cards via HTML")

            time.sleep(2)

        logger.info(f"[Fuzu] Total jobs scraped: {len(jobs)}")
        return jobs

    def _parse_json_ld(self, soup) -> list[JobData]:
        """Parse JSON-LD structured data embedded in the page."""
        import json
        jobs = []
        scripts = soup.find_all("script", type="application/ld+json")
        for script in scripts:
            try:
                data = json.loads(script.string)
                if data.get("@type") == "ItemList" and "itemListElement" in data:
                    for item in data["itemListElement"]:
                        name = item.get("name")
                        item_url = item.get("url")
                        if name and item_url:
                            external_id = item_url.rstrip("/").split("/")[-1] if item_url else None
                            jobs.append(JobData(
                                title=name,
                                url=item_url,
                                apply_url=item_url,
                                source=self.SOURCE_NAME,
                                location="Kenya",
                                external_id=external_id,
                            ))
            except (json.JSONDecodeError, TypeError, KeyError):
                continue
        return jobs

    def _parse_card(self, card) -> Optional[JobData]:
        title_el = card.find("h2") or card.find("h3") or card.find("span", class_=re.compile(r"title"))
        if card.name == "a":
            title = card.get_text(strip=True)
        else:
            title = title_el.get_text(strip=True) if title_el else None
        if not title:
            return None

        link = card.find("a", href=True) if card.name != "a" else card
        url = None
        if link:
            href = link.get("href", "")
            url = href if href.startswith("http") else self.BASE_URL + href

        company_el = card.find("span", class_=re.compile(r"company|employer"))
        company = company_el.get_text(strip=True) if company_el else None

        location_el = card.find("span", class_=re.compile(r"location"))
        location_text = location_el.get_text(strip=True) if location_el else "Kenya"

        external_id = None
        if url:
            external_id = url.rstrip("/").split("/")[-1]

        return JobData(
            title=title,
            company=company,
            location=location_text,
            url=url,
            apply_url=url,
            source=self.SOURCE_NAME,
            external_id=external_id,
        )
