"""
BrighterMonday Kenya Scraper
Scrapes job listings from brightermonday.co.ke - one of East Africa's largest job boards.
Fetches full job descriptions from detail pages.
"""
import logging
import re
import time
from typing import Optional

from airflow_home.scrapers.base_scraper import BaseScraper, JobData

logger = logging.getLogger(__name__)


class BrighterMondayScraper(BaseScraper):
    SOURCE_NAME = "brightermonday"
    BASE_URL = "https://www.brightermonday.co.ke"

    def scrape(
        self, search_query: str = None, location: str = None, max_pages: int = 5
    ) -> list[JobData]:
        jobs = []

        for page in range(1, max_pages + 1):
            url = f"{self.BASE_URL}/jobs"
            params = {"page": page}
            if search_query:
                params["q"] = search_query

            soup = self.fetch_page(url, params=params)
            if not soup:
                break

            # BrighterMonday uses data-cy="listing-cards-components" on each card
            job_cards = soup.find_all(attrs={"data-cy": "listing-cards-components"})
            if not job_cards:
                # Fallback: cards are divs with aria-labelledby containing "job"
                job_cards = soup.find_all("div", attrs={"aria-labelledby": re.compile(r"job")})
            if not job_cards:
                logger.info(f"[BrighterMonday] No cards found on page {page}, stopping.")
                break

            for card in job_cards:
                try:
                    job = self._parse_card(card)
                    if job:
                        jobs.append(job)
                except Exception as e:
                    logger.error(f"[BrighterMonday] Error parsing card: {e}")

            logger.info(f"[BrighterMonday] Page {page}: found {len(job_cards)} cards")
            time.sleep(2)

        logger.info(f"[BrighterMonday] Total jobs scraped: {len(jobs)}")
        return jobs

    def _parse_card(self, card) -> Optional[JobData]:
        # Title link uses data-cy="listing-title-link"
        title_link = card.find("a", attrs={"data-cy": "listing-title-link"})
        if not title_link:
            title_link = card.find("a", href=re.compile(r"/listings/"))
        if not title_link:
            return None

        title_p = title_link.find("p")
        title = (title_p or title_link).get_text(strip=True)
        if not title:
            return None

        # URL
        href = title_link.get("href", "")
        url = href if href.startswith("http") else self.BASE_URL + href

        # Company - first <p> with blue-700 text after the title, or use generic heuristic
        company = None
        company_el = card.find("p", class_=re.compile(r"text-blue-700"))
        if company_el and company_el != (title_link.find("p") if title_link else None):
            company = company_el.get_text(strip=True)

        # Location and job type from tag spans
        location_text = "Kenya"
        job_type = None
        tag_spans = card.find_all("span", class_=re.compile(r"rounded.*bg-brand|bg-brand.*rounded"))
        if not tag_spans:
            tag_spans = card.find_all("span", class_=re.compile(r"rounded"))
        for span in tag_spans:
            text = span.get_text(strip=True)
            if text.lower() in ("full time", "part time", "contract", "freelance", "internship", "temporary"):
                job_type = text
            elif text:
                location_text = text

        # External ID from URL slug
        external_id = None
        if url:
            match = re.search(r"/listings/([^/?]+)", url)
            if match:
                external_id = match.group(1)

        # Fetch full description and requirements from detail page
        detail = {}
        if url:
            try:
                time.sleep(1)
                detail = self._fetch_detail(url)
            except Exception as e:
                logger.warning(f"[BrighterMonday] Detail fetch failed for {url}: {e}")

        return JobData(
            title=title,
            company=detail.get("company") or company,
            location=detail.get("location") or location_text,
            description=detail.get("description"),
            requirements=detail.get("requirements"),
            job_type=detail.get("job_type") or job_type,
            salary_min=detail.get("salary_min"),
            salary_max=detail.get("salary_max"),
            salary_currency=detail.get("salary_currency"),
            url=url,
            apply_url=url,
            source=self.SOURCE_NAME,
            external_id=external_id,
        )

    def _fetch_detail(self, url: str) -> dict:
        """Fetch full job description + requirements from a BrighterMonday detail page."""
        soup = self.fetch_page(url)
        if not soup:
            return {}

        # Remove navigation, header, footer, script, style elements
        for tag in soup(["script", "style", "nav", "header", "footer", "aside"]):
            tag.decompose()

        result = {}

        # --- Company ---
        # data-cy="company-name" or link to /companies/
        for sel in ['[data-cy="company-name"]', 'a[href*="/companies/"]', 'p[class*="blue"]']:
            el = soup.select_one(sel)
            if el:
                text = el.get_text(strip=True)
                if text and len(text) < 150:
                    result["company"] = text
                    break

        # --- Salary ---
        salary_el = soup.find(string=re.compile(r"KSh|KES|salary|ksh", re.I))
        if salary_el:
            salary_text = salary_el.strip() if isinstance(salary_el, str) else salary_el.get_text(strip=True)
            sal_match = re.search(r"([\d,]+)\s*[-–]\s*([\d,]+)", salary_text)
            if sal_match:
                try:
                    result["salary_min"] = float(sal_match.group(1).replace(",", ""))
                    result["salary_max"] = float(sal_match.group(2).replace(",", ""))
                    result["salary_currency"] = "KES"
                except ValueError:
                    pass

        # --- Job type ---
        jt_match = soup.find(string=re.compile(r"\bfull[- ]time\b|\bpart[- ]time\b|\bcontract\b|\binternship\b|\bfreelance\b", re.I))
        if jt_match:
            jt_text = jt_match.strip() if isinstance(jt_match, str) else jt_match.get_text(strip=True)
            jt = re.search(r"full[- ]time|part[- ]time|contract|internship|freelance", jt_text, re.I)
            if jt:
                result["job_type"] = jt.group(0).lower().replace(" ", "-")

        # --- Location ---
        loc_el = soup.find(string=re.compile(r"nairobi|mombasa|kisumu|nakuru|kenya|kampala|dar es salaam", re.I))
        if loc_el:
            parent = loc_el.find_parent(["span", "p", "div"])
            if parent:
                loc_text = parent.get_text(strip=True)
                if loc_text and len(loc_text) < 100:
                    result["location"] = loc_text

        # --- Description and Requirements ---
        # Strategy: walk the main content area collecting sections
        main = soup.find("main") or soup.find("article") or soup.body
        if not main:
            return result

        description_parts = []
        requirements_parts = []
        current_section = None  # "desc", "req", "other"

        for elem in main.find_all(["h1", "h2", "h3", "h4", "p", "ul", "ol"]):
            tag = elem.name
            text = elem.get_text(strip=True)
            if not text:
                continue

            if tag in ("h1", "h2", "h3", "h4"):
                tl = text.lower()
                if re.search(r"description|summary|about the (job|role|position)", tl):
                    current_section = "desc"
                elif re.search(r"requirement|qualification|what we (need|look|want)|must have|skills", tl):
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

            # paragraph
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
