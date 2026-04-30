from __future__ import annotations

from typing import Dict, Iterable, List
from urllib.parse import urljoin, urlparse, urlunparse
import os
import re
import requests
import yaml
from bs4 import BeautifulSoup

from .models import Job
from .utils import clean_text

HEADERS = {
    "User-Agent": "JobFitRadar/0.3 (+personal job search tool; contact: local)",
    "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
}

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
}

GENERIC_TITLES = {
    "jobsdb role", "apply now", "overview", "finance", "risk", "compliance", "consulting",
    "advisory", "tax", "audit and assurance", "business process solutions", "customer",
    "cyber", "sustainability", "hong kong", "campus recruitment | pwc china",
    "campus recruitment | pwc hong kong", "skip to main content", "our businesses and expertise",
    "business model", "commercial, personal banking & services", "corporate & institutional banking",
    "investment & protection services", "bnp paribas personal finance",
}

OFFICIAL_CAREER_ALLOWLIST = [
    "graduate program", "graduate programme", "general graduates", "students & graduates",
    "student", "graduates", "campus hire", "campus hires", "campus recruitment",
    "internship program", "internship programme", "summer internship", "spring recruitment",
    "trainee / internship", "trainee", "early career", "early careers",
    "international volunteer", "volunteer program", "register your interest", "join us",
    "permanent ",  # BNP job pages often prefix actual openings with Permanent
]

OFFICIAL_CAREER_URL_HINTS = [
    "career", "careers", "job", "jobs", "campus", "graduate", "intern", "trainee", "student",
    "all-job-offers", "apply", "recruitment",
]

BAD_OFFICIAL_CONTENT_HINTS = [
    "budget summary", "insights", "commentary", "outlook", "discover more", "article",
    "how a ", "top 10", "ceo agenda", "entrepreneur of the year", "geostrategy",
]


def load_sources(path: str = "sources.yaml") -> List[Dict]:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return [s for s in data.get("sources", []) if s.get("enabled")]


def _strip_tracking(url: str) -> str:
    """Remove most query params so the same job is deduped more reliably."""
    try:
        parsed = urlparse(url)
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))
    except Exception:
        return url


def _render_html_with_playwright(url: str, timeout_ms: int = 25000) -> str | None:
    """Render a public page with a normal browser if Playwright is installed.

    This does not log in, use proxies, solve CAPTCHAs, or bypass access controls.
    If a site blocks access, we return None and fall back to requests.
    """
    if os.getenv("USE_BROWSER_RENDER", "true").strip().lower() not in {"1", "true", "yes", "y"}:
        return None
    try:
        from playwright.sync_api import sync_playwright  # type: ignore
    except Exception:
        return None

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(user_agent=BROWSER_HEADERS["User-Agent"], locale="en-US")
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            # Give client-rendered job cards a short moment to appear.
            try:
                page.wait_for_timeout(2500)
            except Exception:
                pass
            html = page.content()
            browser.close()
            return html
    except Exception as e:
        msg = str(e)
        if "Executable doesn't exist" in msg or "playwright install" in msg.lower():
            print("[WARN] Browser rendering is enabled but Chromium is not installed. Run: python -m playwright install chromium")
        else:
            print(f"[WARN] Browser rendering failed for {url}: {e}")
        return None


def fetch_greenhouse(source: Dict, max_jobs: int = 80) -> List[Job]:
    board = source["board_token"]
    company = source.get("company") or source.get("name") or board
    url = f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs?content=true"
    r = requests.get(url, headers=HEADERS, timeout=25)
    r.raise_for_status()
    data = r.json()
    jobs: List[Job] = []
    for item in data.get("jobs", [])[:max_jobs]:
        location = (item.get("location") or {}).get("name", "")
        jobs.append(Job(
            company=company,
            title=item.get("title", ""),
            location=location,
            url=item.get("absolute_url", ""),
            description=clean_text(item.get("content", ""), 12000),
            source=f"greenhouse:{board}",
            posted_at=item.get("updated_at", ""),
            raw=item,
        ))
    return jobs


def fetch_lever(source: Dict, max_jobs: int = 80) -> List[Job]:
    slug = source["company_slug"]
    company = source.get("company") or source.get("name") or slug
    url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
    r = requests.get(url, headers=HEADERS, timeout=25)
    r.raise_for_status()
    data = r.json()
    jobs: List[Job] = []
    for item in data[:max_jobs]:
        cats = item.get("categories") or {}
        location = cats.get("location", "")
        parts = [item.get("descriptionPlain", ""), item.get("additionalPlain", "")]
        for section in item.get("lists", []) or []:
            parts.append(section.get("text", ""))
            for content in section.get("content", []) or []:
                parts.append(content.get("text", ""))
        jobs.append(Job(
            company=company,
            title=item.get("text", ""),
            location=location,
            url=item.get("hostedUrl", ""),
            description=clean_text("\n".join(parts), 12000),
            source=f"lever:{slug}",
            posted_at=str(item.get("createdAt", "")),
            raw=item,
        ))
    return jobs


def fetch_ashby(source: Dict, max_jobs: int = 80) -> List[Job]:
    org = source["org_slug"]
    company = source.get("company") or source.get("name") or org
    url = f"https://api.ashbyhq.com/posting-api/job-board/{org}"
    r = requests.get(url, headers=HEADERS, timeout=25)
    r.raise_for_status()
    data = r.json()
    jobs: List[Job] = []
    for item in data.get("jobs", [])[:max_jobs]:
        location = item.get("locationName") or item.get("location", "") or ""
        job_url = item.get("jobUrl") or item.get("externalLink") or f"https://jobs.ashbyhq.com/{org}/{item.get('id','')}"
        jobs.append(Job(
            company=company,
            title=item.get("title", ""),
            location=location,
            url=job_url,
            description=clean_text(item.get("descriptionHtml", "") or item.get("description", ""), 12000),
            source=f"ashby:{org}",
            posted_at=item.get("publishedDate", ""),
            raw=item,
        ))
    return jobs


def _official_title_allowed(text: str, href: str) -> bool:
    blob = f"{text} {href}".lower()
    title = text.strip().lower()
    if not title or len(title) < 4:
        return False
    if title in GENERIC_TITLES:
        return False
    if any(bad in blob for bad in BAD_OFFICIAL_CONTENT_HINTS):
        return False
    if any(term in blob for term in OFFICIAL_CAREER_ALLOWLIST):
        return True
    # For BNP actual jobs, titles often start with Permanent and contain a location.
    if re.search(r"\b(permanent|trainee|internship|graduate)\b", text, re.I):
        return True
    return False


def fetch_webpage(source: Dict, max_jobs: int = 80) -> List[Job]:
    url = source["url"]
    company = source.get("company") or source.get("name") or url
    html = _render_html_with_playwright(url)
    if html is None:
        r = requests.get(url, headers=BROWSER_HEADERS, timeout=25)
        r.raise_for_status()
        html = r.text
    soup = BeautifulSoup(html, "lxml")
    jobs: List[Job] = []
    seen = set()
    for a in soup.find_all("a", href=True):
        text = clean_text(a.get_text(" "), 300)
        href = _strip_tracking(urljoin(url, a["href"]))
        if href in seen or not _official_title_allowed(text, href):
            continue
        seen.add(href)
        jobs.append(Job(
            company=company,
            title=text,
            location=source.get("location", ""),
            url=href,
            description=text,
            source=f"webpage:{url}",
            posted_at="",
            raw={"href": href, "text": text},
        ))
        if len(jobs) >= max_jobs:
            break
    return jobs


def _looks_like_linkedin_job(href: str) -> bool:
    return "linkedin.com/jobs/view" in href or "/jobs/view/" in href


def _looks_like_jobsdb_job(href: str) -> bool:
    h = href.lower()
    return (("jobsdb.com" in h or "jobstreet.com" in h) and "/job/" in h)


def _card_text(el) -> str:
    try:
        return clean_text(el.get_text(" "), 1500)
    except Exception:
        return ""


def _clean_job_title(text: str, desc: str = "") -> str:
    text = clean_text(text, 300)
    if text and text.lower() not in GENERIC_TITLES:
        return text
    # Try common patterns from visible card text.
    desc = clean_text(desc, 500)
    for pattern in [
        r"^(.{5,120}?)\s+at\s+",
        r"Job title\s*[:：]\s*(.{5,120}?)(?:\s{2,}|Company|$)",
        r"Title\s*[:：]\s*(.{5,120}?)(?:\s{2,}|Company|$)",
    ]:
        m = re.search(pattern, desc, re.I)
        if m:
            return clean_text(m.group(1), 180)
    return text


def fetch_search_page(source: Dict, max_jobs: int = 80) -> List[Job]:
    """Fetch a public job-search results page and extract visible job links.

    This does not log in, solve CAPTCHAs, use proxies, or bypass anti-bot measures.
    If a site blocks normal requests, this source will warn and skip.
    """
    url = source["url"]
    platform = (source.get("platform") or "generic").lower()
    default_company = source.get("company") or source.get("name") or platform or "Job board"
    default_location = source.get("location") or ""

    html = _render_html_with_playwright(url) if platform in {"jobsdb", "linkedin"} else None
    if html is None:
        r = requests.get(url, headers=BROWSER_HEADERS, timeout=25)
        if r.status_code in {401, 403, 429, 999}:
            print(f"[WARN] {source.get('name')} returned HTTP {r.status_code}. Open the search URL manually or try fewer sources.")
            return []
        r.raise_for_status()
        html = r.text

    soup = BeautifulSoup(html, "lxml")
    jobs: List[Job] = []
    seen = set()

    if platform == "linkedin":
        cards = soup.select("div.base-card, li, div.job-search-card")
        for card in cards:
            a = card.find("a", href=lambda h: bool(h and _looks_like_linkedin_job(urljoin(url, h))))
            if not a:
                continue
            href = _strip_tracking(urljoin(url, a.get("href", "")))
            if href in seen:
                continue
            title_el = card.select_one(".base-search-card__title") or card.select_one("h3") or a
            company_el = card.select_one(".base-search-card__subtitle") or card.select_one("h4")
            loc_el = card.select_one(".job-search-card__location")
            time_el = card.find("time")
            title = clean_text(title_el.get_text(" "), 300)
            company = clean_text(company_el.get_text(" "), 120) if company_el else default_company
            location = clean_text(loc_el.get_text(" "), 120) if loc_el else default_location
            posted = time_el.get("datetime", "") if time_el else ""
            desc = _card_text(card)
            if not title or title.lower() in GENERIC_TITLES or title.lower() in {"linkedin", "view job"}:
                continue
            seen.add(href)
            jobs.append(Job(company=company or default_company, title=title, location=location, url=href,
                            description=desc, source=f"search_page:linkedin:{source.get('name')}", posted_at=posted,
                            raw={"source_url": url, "card_text": desc}))
            if len(jobs) >= max_jobs:
                break

    elif platform == "jobsdb":
        # Prefer visible job-card links after browser rendering. Fallback HTML may have only skeleton links.
        for a in soup.find_all("a", href=True):
            href = _strip_tracking(urljoin(url, a["href"]))
            if not _looks_like_jobsdb_job(href) or href in seen:
                continue
            parent = a.find_parent(["article", "div", "section", "li"]) or a
            desc = _card_text(parent)
            title = _clean_job_title(a.get_text(" "), desc)
            if not title or title.lower() in GENERIC_TITLES or len(title) < 4:
                # Avoid fake placeholder rows such as "JobsDB role".
                continue
            company = default_company
            m = re.search(r"\bat\s+(.+?)(?:\s+This is|\s+Hong Kong|\s+Central|\s+Listed|\s+subClassification|$)", desc, re.I)
            if m:
                company = clean_text(m.group(1), 120)
            loc_match = re.search(r"(Hong Kong(?: SAR| Island)?|Central and Western District|Kowloon|New Territories|Tsim Sha Tsui|Wan Chai|Causeway Bay|Quarry Bay|Kwun Tong)", desc, re.I)
            location = loc_match.group(1) if loc_match else default_location
            posted_match = re.search(r"(Listed\s+(?:one|\d+)\s+(?:hour|hours|day|days)\s+ago|\d+h\s+ago|\d+d\s+ago)", desc, re.I)
            posted = posted_match.group(1) if posted_match else ""
            seen.add(href)
            jobs.append(Job(company=company or default_company, title=title, location=location, url=href,
                            description=desc, source=f"search_page:jobsdb:{source.get('name')}", posted_at=posted,
                            raw={"source_url": url, "card_text": desc}))
            if len(jobs) >= max_jobs:
                break

    else:
        for a in soup.find_all("a", href=True):
            href = _strip_tracking(urljoin(url, a["href"]))
            title = clean_text(a.get_text(" "), 300)
            if href in seen or not title:
                continue
            seen.add(href)
            jobs.append(Job(company=default_company, title=title, location=default_location, url=href,
                            description=title, source=f"search_page:{platform}:{source.get('name')}", posted_at="",
                            raw={"source_url": url}))
            if len(jobs) >= max_jobs:
                break

    return jobs


def fetch_jobs_from_source(source: Dict, max_jobs: int = 80) -> List[Job]:
    source_type = source.get("type")
    if source_type == "greenhouse":
        return fetch_greenhouse(source, max_jobs=max_jobs)
    if source_type == "lever":
        return fetch_lever(source, max_jobs=max_jobs)
    if source_type == "ashby":
        return fetch_ashby(source, max_jobs=max_jobs)
    if source_type == "webpage":
        return fetch_webpage(source, max_jobs=max_jobs)
    if source_type == "search_page":
        return fetch_search_page(source, max_jobs=max_jobs)
    raise ValueError(f"Unsupported source type: {source_type}")


def fetch_all_jobs(sources: Iterable[Dict], max_jobs_per_source: int = 80) -> List[Job]:
    all_jobs: List[Job] = []
    for source in sources:
        try:
            jobs = fetch_jobs_from_source(source, max_jobs=max_jobs_per_source)
            print(f"  - {source.get('name')}: {len(jobs)} job(s)")
            all_jobs.extend(jobs)
        except Exception as e:
            print(f"[WARN] Failed source {source.get('name')}: {e}")
    return all_jobs
