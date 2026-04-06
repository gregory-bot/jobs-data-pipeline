"""
KEMRI Scraper - Scrapes jobs from Kenya Medical Research Institute e-recruitment portal.
Each job links directly to the KEMRI application page where users can apply online.
"""
import re
import time
import logging
import datetime
from typing import Optional

from bs4 import BeautifulSoup

from airflow_home.scrapers.base_scraper import BaseScraper, JobData

logger = logging.getLogger(__name__)

BASE_URL = "https://erecruitment.kemri.go.ke:7070"


class KEMRIScraper(BaseScraper):
    SOURCE_NAME = "kemri"

    def __init__(self):
        super().__init__()
        # KEMRI uses a self-signed/internal cert
        self.session.verify = False

    def scrape(self, search_query: str = None, location: str = None, max_pages: int = 5) -> list[JobData]:
        """Scrape active jobs from KEMRI e-recruitment portal."""
        all_jobs = []

        # Scrape both active and contract job listing pages
        pages = [
            f"{BASE_URL}/",
            f"{BASE_URL}/Home/ContractJobs",
            f"{BASE_URL}/Home/PermanentJobs",
        ]

        for page_url in pages:
            try:
                job_refs = self._get_job_refs(page_url)
                logger.info(f"[{self.SOURCE_NAME}] {page_url}: {len(job_refs)} job refs")

                for ref, listing_data in job_refs:
                    try:
                        job = self._parse_job_detail(ref, listing_data)
                        if job:
                            all_jobs.append(job)
                        time.sleep(0.5)
                    except Exception as e:
                        logger.debug(f"[{self.SOURCE_NAME}] Error parsing {ref}: {e}")

            except Exception as e:
                logger.error(f"[{self.SOURCE_NAME}] Error fetching {page_url}: {e}")
            time.sleep(1)

        # Deduplicate by external_id
        seen = set()
        unique = []
        for j in all_jobs:
            if j.external_id not in seen:
                seen.add(j.external_id)
                unique.append(j)

        logger.info(f"[{self.SOURCE_NAME}] Total: {len(unique)} unique jobs")
        return unique

    def _get_job_refs(self, list_url: str) -> list[tuple[str, dict]]:
        """Extract job references and basic data from listing page table."""
        soup = self.fetch_page(list_url)
        if not soup:
            return []

        refs = []
        table = soup.find("table")
        if not table:
            return []

        rows = table.find_all("tr")
        for row in rows[1:]:  # Skip header
            cells = row.find_all("td")
            if len(cells) < 7:
                continue

            ref = cells[0].get_text(strip=True)
            title = cells[1].get_text(strip=True)
            emp_type = cells[2].get_text(strip=True)
            positions = cells[3].get_text(strip=True)
            deadline = cells[4].get_text(strip=True)
            grade = cells[5].get_text(strip=True)
            status = cells[6].get_text(strip=True)

            if not ref or status.lower() != "active":
                continue

            listing_data = {
                "title": title,
                "emp_type": emp_type,
                "positions": positions,
                "deadline": deadline,
                "grade": grade,
            }
            refs.append((ref, listing_data))

        return refs

    def _parse_job_detail(self, ref: str, listing_data: dict) -> Optional[JobData]:
        """Parse a single job detail page."""
        detail_url = f"{BASE_URL}/Home/SingleJobView/{ref}"
        soup = self.fetch_page(detail_url)
        if not soup:
            return None

        full_html = str(soup)

        # Title from listing (more reliable) or detail page
        title = listing_data.get("title", "")
        if not title or len(title) < 5:
            return None

        # Extract location from title (pattern: "Job Title - Location")
        location_str = ""
        if " - " in title:
            parts = title.rsplit(" - ", 1)
            job_title = parts[0].strip()
            location_str = parts[1].strip()
            # Add Kenya context
            if location_str and "kenya" not in location_str.lower():
                location_str = f"{location_str}, Kenya"
        else:
            job_title = title

        # Extract description from Key Responsibilities section
        description = self._extract_section(full_html, "Key Responsibilities:", "Vacancy Requirements:")

        # Extract requirements from table
        requirements = self._extract_requirements(soup)

        # If no description from responsibilities, use the full page text
        if not description:
            description = self._extract_section(full_html, "Job Tite/Designation", "Vacancy Requirements:")

        if not description or len(description) < 30:
            logger.debug(f"[{self.SOURCE_NAME}] Skipping {job_title} - no description")
            return None

        # Job type from listing
        job_type = listing_data.get("emp_type", "")

        # Extract dates
        posted_date = None
        deadline_date = None

        adv_match = re.search(r"Advertised Date:\s*([\d/]+)", full_html)
        if adv_match:
            try:
                posted_date = datetime.datetime.strptime(adv_match.group(1), "%m/%d/%y")
            except ValueError:
                pass

        close_match = re.search(r"Closing Date:\s*([\d/]+)", full_html)
        if close_match:
            try:
                deadline_date = datetime.datetime.strptime(close_match.group(1), "%m/%d/%y")
            except ValueError:
                pass

        if not posted_date:
            posted_date = datetime.datetime.now(datetime.UTC)

        # The apply URL is the detail page itself - it has the "Apply Online" button
        apply_url = detail_url

        return JobData(
            title=job_title,
            source=self.SOURCE_NAME,
            company="Kenya Medical Research Institute (KEMRI)",
            location=location_str,
            description=description,
            job_type=job_type,
            remote=False,
            url=detail_url,
            apply_url=apply_url,
            requirements=requirements,
            external_id=f"kemri_{ref}",
            posted_date=posted_date,
            application_deadline=deadline_date,
        )

    def _extract_section(self, html: str, start_marker: str, end_marker: str) -> str:
        """Extract text between two section markers in the HTML."""
        start_idx = html.find(start_marker)
        end_idx = html.find(end_marker)

        if start_idx == -1:
            return ""

        # Move past the marker and its closing tag
        start_idx = start_idx + len(start_marker)
        # Skip past the closing </h4> or similar tag
        close_tag = html.find(">", start_idx)
        if close_tag != -1 and close_tag - start_idx < 20:
            start_idx = close_tag + 1

        if end_idx == -1:
            end_idx = start_idx + 5000

        section_html = html[start_idx:end_idx]
        soup = BeautifulSoup(section_html, "html.parser")
        text = soup.get_text(separator="\n", strip=True)

        # Clean up
        lines = [line.strip() for line in text.split("\n") if line.strip()]
        return "\n".join(lines)

    def _extract_requirements(self, soup: BeautifulSoup) -> str:
        """Extract requirements from the Vacancy Requirements table."""
        for h4 in soup.find_all("h4"):
            if "Requirements" in h4.get_text():
                table = h4.find_next("table")
                if table:
                    reqs = []
                    rows = table.find_all("tr")
                    for row in rows[1:]:  # Skip header
                        cells = row.find_all("td")
                        if len(cells) >= 2:
                            req_text = cells[1].get_text(strip=True)
                            priority = cells[2].get_text(strip=True) if len(cells) > 2 else ""
                            if req_text:
                                prefix = f"[{priority}] " if priority else ""
                                reqs.append(f"{prefix}{req_text}")
                    return "\n".join(reqs)
        return ""
