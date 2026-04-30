from __future__ import annotations

import re
import time
from typing import Any
from bs4 import BeautifulSoup

try:
    from jobfit.sources import _render_html_with_playwright
except Exception:
    _render_html_with_playwright = None


def _is_detail_target(job: dict[str, Any]) -> bool:
    url = str(job.get("url", "") or "").lower()
    source = str(job.get("source", "") or "").lower()
    return (
        "linkedin.com/jobs/view" in url
        or "jobsdb.com/job/" in url
        or "jobstreet.com/job/" in url
        or "linkedin" in source
    )


def _clean_text(text: str) -> str:
    text = re.sub(r"\s+", " ", text or "").strip()
    return text[:8000]


def _extract_text_from_html(html: str) -> str:
    soup = BeautifulSoup(html or "", "html.parser")

    for tag in soup(["script", "style", "noscript", "svg"]):
        tag.decompose()

    text = soup.get_text(" ", strip=True)
    return _clean_text(text)


def enrich_one_job_description(job: dict[str, Any], timeout_sleep: float = 0) -> dict[str, Any]:
    if job.get("description"):
        return job

    if not _is_detail_target(job):
        return job

    url = job.get("url")
    if not url:
        return job

    if _render_html_with_playwright is None:
        job["detail_enrich_status"] = "failed: playwright renderer not available"
        return job

    try:
        html = _render_html_with_playwright(url, timeout_ms=30000)
        text = _extract_text_from_html(html or "")

        if len(text) >= 200:
            job["description"] = text
            job["detail_enrich_status"] = "ok"
        else:
            job["detail_enrich_status"] = f"too short: {len(text)} chars"

    except Exception as e:
        job["detail_enrich_status"] = f"failed: {type(e).__name__}: {repr(e)[:200]}"

    if timeout_sleep > 0:
        time.sleep(timeout_sleep)

    return job


def enrich_job_descriptions(jobs: list[dict[str, Any]], config: dict[str, Any]) -> list[dict[str, Any]]:
    cfg = config.get("detail_enrichment", {}) or {}

    if not cfg.get("enabled", True):
        print("Detail enrichment disabled.")
        return jobs

    min_score = int(cfg.get("min_score", 75))
    limit = int(cfg.get("limit", 30))
    delay = float(cfg.get("delay_seconds", 1.5))

    candidates = [
        j for j in jobs
        if _is_detail_target(j)
        and not j.get("description")
        and int(j.get("score", 0) or 0) >= min_score
    ]

    candidates.sort(key=lambda x: int(x.get("score", 0) or 0), reverse=True)
    candidates = candidates[:limit]

    print(f"Detail enrichment candidates: {len(candidates)}")

    url_to_job = {id(j): j for j in jobs}

    for i, job in enumerate(candidates, start=1):
        print(f"  enriching {i}/{len(candidates)}: [{job.get('score')}] {job.get('company')} - {job.get('title')}")
        enrich_one_job_description(job, timeout_sleep=delay)

    return jobs
