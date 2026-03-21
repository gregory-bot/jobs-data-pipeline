"""
Google Jobs Search Scraper
Uses Google search with site-specific queries to find jobs across many sites.
This acts as the "agentic search" - finding jobs from any source via Google.
"""
import logging
import re
import time
from typing import Optional
from urllib.parse import quote_plus

from scrapers.base_scraper import BaseScraper, JobData

logger = logging.getLogger(__name__)


class GoogleJobsSearchScraper(BaseScraper):
    """
    Searches Google for job listings across various sites.
    This is the 'agentic search' approach - using Google to aggregate jobs.
    """

    SOURCE_NAME = "google_search"
    SEARCH_URL = "https://www.google.com/search"

    # Sites to search across
    TARGET_SITES = [
        "linkedin.com/jobs",
        "brightermonday.co.ke",
        "indeed.com",
        "glassdoor.com",
        "kariuki.co.ke",
        "jobwebkenya.com",
        "corporatestaffing.co.ke",
        "summitrecruitment-search.com",
        "kenyajob.com",
    ]

    def scrape(
        self, search_query: str = None, location: str = None, max_pages: int = 3
    ) -> list[JobData]:
        jobs = []
        query = search_query or "jobs hiring"
        loc = location or "Kenya"

        for site in self.TARGET_SITES:
            search_q = f"site:{site} {query} {loc}"
            params = {
                "q": search_q,
                "num": 20,
            }

            soup = self.fetch_page(self.SEARCH_URL, params=params)
            if not soup:
                continue

            # Parse Google search results
            results = soup.find_all("div", class_="g")
            if not results:
                results = soup.find_all("div", class_=re.compile(r"tF2Cxc|g"))

            for result in results:
                try:
                    job = self._parse_google_result(result, site)
                    if job:
                        jobs.append(job)
                except Exception as e:
                    logger.error(f"[GoogleSearch] Error parsing result from {site}: {e}")

            logger.info(f"[GoogleSearch] {site}: found {len(results)} results")
            time.sleep(3)  # Respectful delay for Google

        logger.info(f"[GoogleSearch] Total jobs found: {len(jobs)}")
        return jobs

    def _parse_google_result(self, result, site: str) -> Optional[JobData]:
        # Title from link
        link_el = result.find("a", href=True)
        if not link_el:
            return None

        title_el = link_el.find("h3")
        title = title_el.get_text(strip=True) if title_el else None
        if not title:
            return None

        url = link_el.get("href", "")
        if not url.startswith("http"):
            return None

        # Snippet for description
        snippet_el = result.find("div", class_=re.compile(r"VwiC3b|snippet"))
        description = snippet_el.get_text(strip=True) if snippet_el else None

        # Determine source from site
        source_name = site.split(".")[0] if "." in site else site

        return JobData(
            title=title,
            description=description,
            url=url,
            apply_url=url,
            source=f"google_{source_name}",
            location="Kenya",
        )
