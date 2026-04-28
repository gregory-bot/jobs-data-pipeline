"""
Corporate Staffing Scraper - corporatestaffing.co.ke
Top recruitment agency in Kenya, frequently lists deadlines.
"""
import re
import time
import logging
import datetime
from typing import Optional

from airflow_home.scrapers.base_scraper import BaseScraper, JobData

logger = logging.getLogger(__name__)


class CorporateStaffingScraper(BaseScraper):
    SOURCE_NAME = "corporatestaffing"
    BASE_URL = "https://www.corporatestaffing.co.ke"

    def scrape(self, search_query: str = None, location: str = None, max_pages: int = 5) -> list[JobData]:
        jobs = []
        for page in range(1, max_pages + 1):
            url = f"{self.BASE_URL}/jobs"
            params = {"page": page}
            if search_query:
                params["search"] = search_query

            soup = self.fetch_page(url, params=params)
            if not soup:
                break

            listings = soup.select("div.job_listing, .job-item, article.post, .type-job_listing")
            if not listings:
                listings = soup.find_all("div", class_=re.compile(r"job|listing|vacancy", re.I))
            if not listings:
                logger.info(f"[{self.SOURCE_NAME}] No listings found on page {page}")
                break

            for item in listings:
                try:
                    title_el = item.find(["h2", "h3", "a"], class_=re.compile(r"title|heading|entry", re.I))
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

                    company_el = item.find(class_=re.compile(r"company|employer", re.I))
                    company = company_el.get_text(strip=True) if company_el else None

                    loc_el = item.find(class_=re.compile(r"location|place", re.I))
                    loc = loc_el.get_text(strip=True) if loc_el else location or "Kenya"

                    deadline_el = item.find(class_=re.compile(r"deadline|closing|date", re.I))
                    deadline = self._parse_date(deadline_el.get_text(strip=True)) if deadline_el else None

                    job_type_el = item.find(class_=re.compile(r"type|employment", re.I))
                    job_type = job_type_el.get_text(strip=True).lower() if job_type_el else None

                    desc_el = item.find(class_=re.compile(r"desc|summary|content", re.I))
                    desc = desc_el.get_text(strip=True)[:1000] if desc_el else None

                    slug = re.search(r"/([^/]+)/?$", link)
                    ext_id = slug.group(1) if slug else title[:80]

                    jobs.append(JobData(
                        title=title,
                        source=self.SOURCE_NAME,
                        company=company,
                        location=loc,
                        description=desc,
                        job_type=job_type,
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
        text = re.sub(r"(deadline|closing\s*date|expires?|date)\s*:?\s*", "", text, flags=re.I).strip()
        for fmt in ("%d %B %Y", "%B %d, %Y", "%d/%m/%Y", "%Y-%m-%d", "%d %b %Y", "%d-%m-%Y"):
            try:
                return datetime.datetime.strptime(text, fmt)
            except ValueError:
                continue
        return None
