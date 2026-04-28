"""
JobWebKenya Scraper - jobwebkenya.com
Major Kenyan job aggregator with deadline info.
"""
import re
import time
import logging
import datetime
from typing import Optional

from airflow_home.scrapers.base_scraper import BaseScraper, JobData

logger = logging.getLogger(__name__)


class JobWebKenyaScraper(BaseScraper):
    SOURCE_NAME = "jobwebkenya"
    BASE_URL = "https://www.jobwebkenya.com"

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

            listings = soup.select("article.job-listing, .job-item, .listing-item, div[class*='job']")
            if not listings:
                listings = soup.find_all("div", class_=re.compile(r"job|listing|vacancy", re.I))
            if not listings:
                logger.info(f"[{self.SOURCE_NAME}] No listings on page {page}")
                break

            for item in listings:
                try:
                    title_el = item.find(["h2", "h3", "h4", "a"], class_=re.compile(r"title|heading", re.I))
                    if not title_el:
                        title_el = item.find("a")
                    if not title_el:
                        continue

                    title = title_el.get_text(strip=True)
                    if not title or len(title) < 3:
                        continue

                    link = title_el.get("href") or (title_el.find("a") or {}).get("href", "")
                    if link and not link.startswith("http"):
                        link = self.BASE_URL + link

                    company_el = item.find(class_=re.compile(r"company|employer|org", re.I))
                    company = company_el.get_text(strip=True) if company_el else None

                    loc_el = item.find(class_=re.compile(r"location|place|city", re.I))
                    loc = loc_el.get_text(strip=True) if loc_el else "Kenya"

                    deadline_el = item.find(class_=re.compile(r"deadline|closing|expir", re.I))
                    deadline = self._parse_date(deadline_el.get_text(strip=True)) if deadline_el else None

                    desc_el = item.find(class_=re.compile(r"desc|summary|excerpt", re.I))
                    desc = desc_el.get_text(strip=True) if desc_el else None

                    ext_id = re.search(r"/(\d+)", link).group(1) if link and re.search(r"/(\d+)", link) else title[:80]

                    jobs.append(JobData(
                        title=title,
                        source=self.SOURCE_NAME,
                        company=company,
                        location=loc,
                        description=desc,
                        url=link,
                        apply_url=link,
                        application_deadline=deadline,
                        external_id=ext_id,
                    ))
                except Exception as e:
                    logger.debug(f"[{self.SOURCE_NAME}] Failed to parse listing: {e}")

            time.sleep(2)

        logger.info(f"[{self.SOURCE_NAME}] Scraped {len(jobs)} jobs")
        return jobs

    @staticmethod
    def _parse_date(text: str) -> Optional[datetime.datetime]:
        text = re.sub(r"(deadline|closing\s*date|expires?)\s*:?\s*", "", text, flags=re.I).strip()
        for fmt in ("%d %B %Y", "%B %d, %Y", "%d/%m/%Y", "%Y-%m-%d", "%d %b %Y", "%d-%m-%Y"):
            try:
                return datetime.datetime.strptime(text, fmt)
            except ValueError:
                continue
        return None
