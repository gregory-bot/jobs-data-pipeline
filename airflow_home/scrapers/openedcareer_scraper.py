"""
OpenedCareer Scraper - Scrapes jobs from openedcareer.com using WordPress REST API.
Only includes jobs with DIRECT apply links to employer portals (not back to openedcareer).
"""
import re
import time
import logging
import datetime
from html import unescape
from typing import Optional

from bs4 import BeautifulSoup

from airflow_home.scrapers.base_scraper import BaseScraper, JobData

logger = logging.getLogger(__name__)

# WordPress REST API endpoint
API_URL = "https://openedcareer.com/wp-json/wp/v2/posts"
JOBS_CATEGORY_ID = 3  # "Jobs" category has ID 3


class OpenedCareerScraper(BaseScraper):
    SOURCE_NAME = "openedcareer"

    def __init__(self):
        super().__init__()
        # Remove brotli from Accept-Encoding as requests doesn't decode it by default
        self.session.headers["Accept-Encoding"] = "gzip, deflate"

    def scrape(self, search_query: str = None, location: str = None, max_pages: int = 5) -> list[JobData]:
        """Scrape jobs from OpenedCareer using WordPress REST API."""
        all_jobs = []
        per_page = 20  # WP default

        for page_num in range(1, max_pages + 1):
            try:
                params = {
                    "categories": JOBS_CATEGORY_ID,
                    "per_page": per_page,
                    "page": page_num,
                    "orderby": "date",
                    "order": "desc",
                }

                data = self.fetch_json(API_URL, params=params)
                if not data or len(data) == 0:
                    logger.info(f"[{self.SOURCE_NAME}] No posts on page {page_num}, stopping")
                    break

                logger.info(f"[{self.SOURCE_NAME}] Page {page_num}: {len(data)} posts")

                for post in data:
                    try:
                        job = self._parse_post(post)
                        if job:
                            all_jobs.append(job)
                    except Exception as e:
                        logger.debug(f"[{self.SOURCE_NAME}] Error parsing post {post.get('id')}: {e}")

                # If we got fewer than per_page, we've hit the end
                if len(data) < per_page:
                    break

                time.sleep(0.5)

            except Exception as e:
                logger.error(f"[{self.SOURCE_NAME}] Error on page {page_num}: {e}")
                break

        logger.info(f"[{self.SOURCE_NAME}] Total: {len(all_jobs)} jobs with direct apply links")
        return all_jobs

    def _parse_post(self, post: dict) -> Optional[JobData]:
        """Parse a single WordPress post into JobData."""
        # Get title (HTML entities need unescaping)
        raw_title = post.get("title", {}).get("rendered", "")
        full_title = unescape(raw_title).strip()

        if not full_title or len(full_title) < 5:
            return None

        # Extract company from title (after " at ")
        if " at " in full_title:
            parts = full_title.rsplit(" at ", 1)
            job_title = parts[0].strip()
            company = parts[1].strip()
        else:
            job_title = full_title
            company = ""

        # Get URL
        url = post.get("link", "")

        # Get content (HTML)
        content_html = post.get("content", {}).get("rendered", "")
        if not content_html:
            return None

        # Parse content to find the direct apply link
        soup = BeautifulSoup(content_html, "lxml")
        apply_url = None

        for a in soup.find_all("a", href=True):
            text = a.get_text(strip=True).lower()
            href = a["href"]
            # Look for "apply" links that go to external sites
            if ("apply" in text or "read more" in text) and "openedcareer" not in href and href.startswith("http"):
                apply_url = href
                break

        # Skip jobs without direct apply links - this is the KEY quality filter
        if not apply_url:
            logger.debug(f"[{self.SOURCE_NAME}] Skipping {job_title} - no direct apply link")
            return None

        # Extract text content
        full_text = soup.get_text(separator="\n", strip=True)

        # Extract description and requirements
        description, requirements = self._extract_content(full_text)

        if not description or len(description) < 50:
            logger.debug(f"[{self.SOURCE_NAME}] Skipping {job_title} - no description")
            return None

        # Extract location
        location_str = self._extract_location(full_text)

        # Extract job type
        job_type = self._extract_job_type(full_text)

        # Check for remote
        is_remote = "remote" in full_text.lower() and "not remote" not in full_text.lower()

        # Extract deadline
        deadline = self._extract_deadline(full_text)

        # Get posted date from WordPress
        posted_date = None
        date_str = post.get("date_gmt")
        if date_str:
            try:
                posted_date = datetime.datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            except ValueError:
                posted_date = datetime.datetime.now(datetime.UTC)

        # Generate external ID
        external_id = f"openedcareer_{post.get('id', post.get('slug', ''))}"

        return JobData(
            title=job_title,
            source=self.SOURCE_NAME,
            company=company,
            location=location_str,
            description=description,
            job_type=job_type,
            remote=is_remote,
            url=url,
            apply_url=apply_url,
            requirements=requirements,
            external_id=external_id,
            posted_date=posted_date,
            application_deadline=deadline,
        )

    def _extract_content(self, text: str) -> tuple[str, str]:
        """Split text into description and requirements."""
        lines = [line.strip() for line in text.split("\n") if line.strip()]

        # Filter out navigation/footer content
        skip_patterns = [
            "skip to content", "share this", "whatsapp", "opened career",
            "copyright", "privacy policy", "terms", "read more & apply",
        ]
        filtered = [ln for ln in lines if not any(p in ln.lower() for p in skip_patterns)]

        # Split into description and requirements
        desc_parts = []
        req_parts = []
        in_requirements = False

        requirement_keywords = [
            "requirement", "qualification", "minimum position",
            "what we're looking for", "what you'll need", "the person",
            "skills required", "education", "must have", "ideal candidate",
        ]

        for line in filtered:
            lower = line.lower()
            if any(kw in lower for kw in requirement_keywords):
                in_requirements = True
            if in_requirements:
                req_parts.append(line)
            else:
                desc_parts.append(line)

        return "\n".join(desc_parts), "\n".join(req_parts)

    def _extract_location(self, text: str) -> str:
        """Extract location from text content."""
        # Look for explicit location patterns
        loc_match = re.search(
            r"(?:Location|Workplace|Based in)[:\s]*([A-Za-z\s,\-]+(?:Kenya|Nigeria|Uganda|Tanzania))",
            text, re.IGNORECASE
        )
        if loc_match:
            return loc_match.group(1).strip()

        # Fallback: common cities
        cities = ["Nairobi", "Mombasa", "Kisumu", "Nakuru", "Lagos", "Kampala", "Dar es Salaam"]
        for city in cities:
            if city in text:
                if "Kenya" in text:
                    return f"{city}, Kenya"
                elif "Nigeria" in text:
                    return f"{city}, Nigeria"
                return city
        return ""

    def _extract_job_type(self, text: str) -> str:
        """Extract job type from text."""
        text_lower = text.lower()
        if "full-time" in text_lower or "fulltime" in text_lower or "full time" in text_lower:
            return "Full-Time"
        elif "part-time" in text_lower or "parttime" in text_lower:
            return "Part-Time"
        elif "contract" in text_lower:
            return "Contract"
        elif "internship" in text_lower:
            return "Internship"
        return ""

    def _extract_deadline(self, text: str) -> Optional[datetime.datetime]:
        """Extract application deadline from text."""
        deadline_match = re.search(
            r"(?:Deadline|Application Deadline)[:\s]*(\d{1,2}(?:st|nd|rd|th)?\s+\w+\s+\d{4})",
            text, re.IGNORECASE
        )
        if deadline_match:
            try:
                date_str = deadline_match.group(1)
                date_str = re.sub(r"(\d+)(?:st|nd|rd|th)", r"\1", date_str)
                return datetime.datetime.strptime(date_str, "%d %B %Y")
            except ValueError:
                pass
        return None
