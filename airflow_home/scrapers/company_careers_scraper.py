"""
Company Career Pages Crawler
Crawls career pages for major Kenyan companies, global tech companies, UN, and NGOs.
Uses JSON-LD, structured data, and HTML parsing depending on site.
"""
import re
import json
import time
import logging
import datetime
from typing import Optional

from airflow_home.scrapers.base_scraper import BaseScraper, JobData

logger = logging.getLogger(__name__)

# ============================================================
# Each career site is a config dict:
#   name        – slug for source field
#   company     – display company name
#   url         – career page URL
#   strategy    – "html", "json_ld", "api", "sitemap"
#   selectors   – CSS/regex selectors per strategy
# ============================================================
CAREER_SITES = [
    {
        "name": "safaricom",
        "company": "Safaricom PLC",
        "url": "https://www.safaricom.co.ke/about/careers",
        "strategy": "html",
        "selectors": {
            "listing": ".job-listing, .vacancies li, .vacancy-card, .career-item",
            "title": "h2, h3, h4, a",
            "link_pattern": r"/careers/|/vacancies/|/job/",
        },
    },
    {
        "name": "equity_bank",
        "company": "Equity Bank",
        "url": "https://equitybank.taleo.net/careersection/ext/joblist.ftl",
        "strategy": "html",
        "selectors": {
            "listing": "tr.job, .job-listing, .listSingleContainer",
            "title": "a, span.jobTitle",
            "link_pattern": r"/jobdetail|/requisition/",
        },
    },
    {
        "name": "kcb_bank",
        "company": "KCB Bank",
        "url": "https://kcb.taleo.net/careersection/ext/joblist.ftl",
        "strategy": "html",
        "selectors": {
            "listing": "tr.job, .job-listing, .listSingleContainer",
            "title": "a, span.jobTitle",
            "link_pattern": r"/jobdetail|/requisition/",
        },
    },
    {
        "name": "un_careers",
        "company": "United Nations",
        "url": "https://careers.un.org/lbw/home.aspx?viewtype=SJ&vacancy=All&lang=en-US",
        "strategy": "html",
        "selectors": {
            "listing": "table.view tr, .vacancy-row, tbody tr",
            "title": "a, td:first-child",
            "link_pattern": r"/lbw/jobdetail|viewtype=",
        },
    },
    {
        "name": "who_careers",
        "company": "World Health Organization",
        "url": "https://careers.who.int/careersection/ex/joblist.ftl",
        "strategy": "html",
        "selectors": {
            "listing": "tr.job, .job-listing, .listSingleContainer",
            "title": "a, span.jobTitle",
            "link_pattern": r"/jobdetail|/req",
        },
    },
    {
        "name": "givedirectly",
        "company": "GiveDirectly",
        "url": "https://boards.greenhouse.io/givedirectly",
        "strategy": "greenhouse",
        "selectors": {},
    },
    {
        "name": "amref",
        "company": "Amref Health Africa",
        "url": "https://amref.org/vacancies/",
        "strategy": "html",
        "selectors": {
            "listing": ".vacancy, .job-posting, article",
            "title": "h2, h3, h4, a",
            "link_pattern": r"/vacancies?/|/jobs?/",
        },
    },
    {
        "name": "savethechildren",
        "company": "Save the Children",
        "url": "https://hcri.fa.em2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1/requisitions",
        "strategy": "html",
        "selectors": {
            "listing": ".job-card, .requisition, [role='listitem']",
            "title": "h2, h3, a",
            "link_pattern": r"/requisition|/job/",
        },
    },
    {
        "name": "worldvision",
        "company": "World Vision",
        "url": "https://careers.wvi.org/jobs",
        "strategy": "html",
        "selectors": {
            "listing": ".job-listing, .views-row, .job-teaser",
            "title": "h2, h3, a",
            "link_pattern": r"/jobs/|/careers/",
        },
    },
    {
        "name": "turing",
        "company": "Turing",
        "url": "https://www.turing.com/jobs",
        "strategy": "html",
        "selectors": {
            "listing": ".job-card, .job-listing, [data-testid='job-card']",
            "title": "h2, h3, h4, a",
            "link_pattern": r"/jobs/|/remote-",
        },
    },
    {
        "name": "builtin",
        "company": "BuiltIn",
        "url": "https://builtin.com/jobs/remote",
        "strategy": "html",
        "selectors": {
            "listing": ".job-card, [data-id], .job-item",
            "title": "h2, h3, a",
            "link_pattern": r"/job/|/company/.*?/jobs/",
        },
    },
]


class CompanyCareersScraper(BaseScraper):
    """
    Generic company-careers crawler that iterates over CAREER_SITES configs.
    Each site+company becomes its own source tag in the DB.
    """
    SOURCE_NAME = "company_careers"

    def scrape(self, search_query: str = None, location: str = None, max_pages: int = 5) -> list[JobData]:
        all_jobs: list[JobData] = []

        for site in CAREER_SITES:
            try:
                logger.info(f"[Careers] Crawling {site['company']} ({site['url']})")
                jobs = self._crawl_site(site)
                all_jobs.extend(jobs)
                logger.info(f"[Careers] {site['company']}: {len(jobs)} jobs")
            except Exception as e:
                logger.error(f"[Careers] Failed {site['company']}: {e}")
            time.sleep(2)  # Respectful delay between sites

        logger.info(f"[Careers] Total: {len(all_jobs)} jobs from {len(CAREER_SITES)} sites")
        return all_jobs

    def _crawl_site(self, site: dict) -> list[JobData]:
        strategy = site.get("strategy", "html")

        if strategy == "greenhouse":
            return self._crawl_greenhouse(site)
        elif strategy == "json_ld":
            return self._crawl_json_ld(site)
        else:
            return self._crawl_html(site)

    # ------------------------------------------------------------------
    # Greenhouse boards (e.g. GiveDirectly, many startups)
    # ------------------------------------------------------------------
    def _crawl_greenhouse(self, site: dict) -> list[JobData]:
        """Greenhouse boards serve public JSON at /boards/<company>/jobs."""
        company_slug = site["url"].rstrip("/").split("/")[-1]
        api_url = f"https://boards-api.greenhouse.io/v1/boards/{company_slug}/jobs"
        data = self.fetch_json(api_url)
        if not data or "jobs" not in data:
            # Fallback to HTML
            return self._crawl_html(site)

        jobs = []
        for item in data["jobs"]:
            title = item.get("title")
            if not title:
                continue
            loc = item.get("location", {}).get("name", "") if isinstance(item.get("location"), dict) else "Remote"
            url = item.get("absolute_url")
            departments = [d.get("name") for d in item.get("departments", []) if d.get("name")]
            tags = ",".join(departments)

            posted_date = None
            updated_at = item.get("updated_at") or item.get("created_at")
            if updated_at:
                try:
                    posted_date = datetime.datetime.strptime(updated_at[:19], "%Y-%m-%dT%H:%M:%S")
                except ValueError:
                    pass

            jobs.append(JobData(
                title=title,
                company=site["company"],
                location=loc,
                url=url,
                apply_url=url,
                tags=tags,
                posted_date=posted_date,
                source=site["name"],
                external_id=str(item.get("id", "")),
            ))
        return jobs

    # ------------------------------------------------------------------
    # JSON-LD parser  – for sites that embed structured data
    # ------------------------------------------------------------------
    def _crawl_json_ld(self, site: dict) -> list[JobData]:
        soup = self.fetch_page(site["url"])
        if not soup:
            return []

        jobs = []
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string)
                items = data if isinstance(data, list) else [data]
                for d in items:
                    if d.get("@type") == "JobPosting":
                        j = self._jobdata_from_jsonld(d, site)
                        if j:
                            jobs.append(j)
            except (json.JSONDecodeError, TypeError):
                continue
        return jobs

    def _jobdata_from_jsonld(self, data: dict, site: dict) -> Optional[JobData]:
        title = data.get("title")
        if not title:
            return None
        company = site["company"]
        hiring_org = data.get("hiringOrganization")
        if isinstance(hiring_org, dict) and hiring_org.get("name"):
            company = hiring_org["name"]
        loc = "Remote"
        job_loc = data.get("jobLocation")
        if isinstance(job_loc, dict):
            addr = job_loc.get("address", {})
            loc = addr.get("addressLocality") or addr.get("addressCountry") or "Remote"
        url = data.get("url") or data.get("sameAs") or site["url"]
        description = data.get("description") or ""
        posted_date = None
        dp = data.get("datePosted")
        if dp:
            try:
                posted_date = datetime.datetime.strptime(dp[:10], "%Y-%m-%d")
            except ValueError:
                pass
        return JobData(
            title=title,
            company=company,
            location=loc,
            description=description,
            url=url,
            apply_url=url,
            posted_date=posted_date,
            source=site["name"],
            external_id=url.rstrip("/").split("/")[-1] if url else None,
        )

    # ------------------------------------------------------------------
    # Generic HTML parser
    # ------------------------------------------------------------------
    def _crawl_html(self, site: dict) -> list[JobData]:
        soup = self.fetch_page(site["url"])
        if not soup:
            return []

        # First try JSON-LD embedded in the page
        jsonld_jobs = self._crawl_json_ld_from_soup(soup, site)
        if jsonld_jobs:
            return jsonld_jobs

        sel = site.get("selectors", {})
        listing_sel = sel.get("listing", ".job-listing, .job-card, article")
        title_sel = sel.get("title", "h2, h3, h4, a")
        link_pattern = sel.get("link_pattern", r"/jobs?/|/careers?/|/vacancies?/")

        listings = soup.select(listing_sel)
        if not listings:
            # Broad fallback
            listings = soup.find_all(["div", "li", "article"], class_=re.compile(r"job|vacancy|career|position", re.I))

        jobs = []
        for item in listings[:50]:  # Cap per-site to avoid spam
            try:
                title_el = item.select_one(title_sel)
                if not title_el:
                    continue
                title = title_el.get_text(strip=True)
                if not title or len(title) < 3:
                    continue

                link = item.find("a", href=re.compile(link_pattern, re.I))
                if not link:
                    link = item.find("a", href=True)
                href = link.get("href", "") if link else ""
                base_url = site["url"].rsplit("/", 1)[0]
                url = href if href.startswith("http") else base_url + "/" + href.lstrip("/") if href else None

                loc_el = item.find(class_=re.compile(r"location|city|country", re.I))
                location = loc_el.get_text(strip=True) if loc_el else None

                type_el = item.find(class_=re.compile(r"type|contract|employment", re.I))
                job_type = type_el.get_text(strip=True) if type_el else None

                jobs.append(JobData(
                    title=title,
                    company=site["company"],
                    location=location,
                    job_type=job_type,
                    url=url,
                    apply_url=url,
                    source=site["name"],
                    external_id=href.rstrip("/").split("/")[-1] if href else None,
                ))
            except Exception as e:
                logger.debug(f"[Careers] Parse error for {site['company']}: {e}")

        return jobs

    def _crawl_json_ld_from_soup(self, soup, site: dict) -> list[JobData]:
        """Try extracting JSON-LD from an already-parsed page."""
        jobs = []
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string)
                items = data if isinstance(data, list) else [data]
                for d in items:
                    if d.get("@type") == "JobPosting":
                        j = self._jobdata_from_jsonld(d, site)
                        if j:
                            jobs.append(j)
            except (json.JSONDecodeError, TypeError):
                continue
        return jobs
