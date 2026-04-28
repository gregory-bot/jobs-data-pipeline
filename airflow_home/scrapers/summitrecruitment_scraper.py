"""
Summit Recruitment Scraper - summitrecruitment-search.com
East Africa recruitment firm.
Fetches full job descriptions from detail pages.
"""
import re
import time
import logging
import datetime
from typing import Optional

from airflow_home.scrapers.base_scraper import BaseScraper, JobData

logger = logging.getLogger(__name__)


class SummitRecruitmentScraper(BaseScraper):
    SOURCE_NAME = "summitrecruitment"
    BASE_URL = "https://www.summitrecruitment-search.com"

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

            listings = soup.select(".job-listing, .job-item, article, .vacancy, .job_listing")
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

                    company_el = item.find(class_=re.compile(r"company|employer", re.I))
                    company = company_el.get_text(strip=True) if company_el else None

                    loc_el = item.find(class_=re.compile(r"location|place", re.I))
                    loc = loc_el.get_text(strip=True) if loc_el else location or "Kenya"

                    deadline_el = item.find(string=re.compile(r"deadline|closing", re.I))
                    deadline = None
                    if deadline_el:
                        deadline = self._parse_date(deadline_el.get_text(strip=True) if hasattr(deadline_el, 'get_text') else str(deadline_el))

                    slug = re.search(r"/([^/]+)/?$", link)
                    ext_id = slug.group(1) if slug else title[:80]

                    # Fetch full description from detail page
                    detail = {}
                    if link:
                        try:
                            time.sleep(1)
                            detail = self._fetch_detail(link)
                        except Exception as e:
                            logger.warning(f"[{self.SOURCE_NAME}] Detail fetch failed for {link}: {e}")

                    jobs.append(JobData(
                        title=title,
                        source=self.SOURCE_NAME,
                        company=detail.get("company") or company,
                        location=detail.get("location") or loc,
                        description=detail.get("description"),
                        requirements=detail.get("requirements"),
                        job_type=detail.get("job_type"),
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

    def _fetch_detail(self, url: str) -> dict:
        """Fetch full job description from a SummitRecruitment detail page."""
        soup = self.fetch_page(url)
        if not soup:
            return {}

        for tag in soup(["script", "style", "nav", "header", "footer", "aside"]):
            tag.decompose()

        result = {}

        # --- Company ---
        for sel in [".company-name", ".employer-name", '[class*="company"]', '[class*="employer"]']:
            el = soup.select_one(sel)
            if el:
                text = el.get_text(strip=True)
                if text and len(text) < 150:
                    result["company"] = text
                    break
        # Fallback: look for "Company:" label
        company_label = soup.find(string=re.compile(r"company\s*:", re.I))
        if company_label and "company" not in result:
            parent = company_label.find_parent(["td", "div", "li", "p"])
            if parent:
                text = re.sub(r"company\s*:\s*", "", parent.get_text(strip=True), flags=re.I).strip()
                if text and len(text) < 150:
                    result["company"] = text

        # --- Job type ---
        jt = soup.find(string=re.compile(r"\bfull[- ]time\b|\bpart[- ]time\b|\bcontract\b|\binternship\b", re.I))
        if jt:
            jt_text = jt.strip() if isinstance(jt, str) else jt.get_text(strip=True)
            jt_match = re.search(r"full[- ]time|part[- ]time|contract|internship|freelance", jt_text, re.I)
            if jt_match:
                result["job_type"] = jt_match.group(0).lower().replace(" ", "-")

        # --- Description and Requirements ---
        main = soup.find("main") or soup.find("article") or soup.find(class_=re.compile(r"job-detail|job-content|entry-content", re.I)) or soup.body
        if not main:
            return result

        description_parts = []
        requirements_parts = []
        current_section = None

        for elem in main.find_all(["h1", "h2", "h3", "h4", "p", "ul", "ol"]):
            tag = elem.name
            text = elem.get_text(strip=True)
            if not text:
                continue

            if tag in ("h1", "h2", "h3", "h4"):
                tl = text.lower()
                if re.search(r"description|summary|about|role|position|overview", tl):
                    current_section = "desc"
                elif re.search(r"requirement|qualification|what we (need|look|want)|must have|competenc|skills required", tl):
                    current_section = "req"
                elif re.search(r"what we offer|benefit|package|compensation", tl):
                    current_section = "offer"
                else:
                    current_section = "other"
                continue

            if tag in ("ul", "ol"):
                items = [li.get_text(strip=True) for li in elem.find_all("li") if li.get_text(strip=True)]
                if current_section == "req":
                    requirements_parts.extend(items)
                elif current_section in ("desc", "offer", "other", None):
                    description_parts.extend(items)
                continue

            if len(text) > 20:
                if current_section == "req":
                    requirements_parts.append(text)
                elif current_section in ("desc", None):
                    description_parts.append(text)

        if description_parts:
            result["description"] = "\n".join(description_parts)[:5000]
        if requirements_parts:
            result["requirements"] = "\n".join(requirements_parts)[:3000]

        return result

    @staticmethod
    def _parse_date(text: str) -> Optional[datetime.datetime]:
        text = re.sub(r"(deadline|closing\s*date|expires?)\s*:?\s*", "", text, flags=re.I).strip()
        for fmt in ("%d %B %Y", "%B %d, %Y", "%d/%m/%Y", "%Y-%m-%d", "%d %b %Y"):
            try:
                return datetime.datetime.strptime(text, fmt)
            except ValueError:
                continue
        return None
