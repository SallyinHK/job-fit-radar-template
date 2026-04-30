from __future__ import annotations

import re
from typing import Any


def _text(job: dict[str, Any]) -> str:
    return " ".join([
        str(job.get("title", "") or ""),
        str(job.get("company", "") or ""),
        str(job.get("source", "") or ""),
        str(job.get("description", "") or ""),
    ]).lower()


def _contains_any(text: str, terms: list[str]) -> bool:
    return any(str(t).lower() in text for t in terms if str(t).strip())


def _matches_company_blacklist(company: str, title: str, blacklist: list[str]) -> str:
    haystack = f"{company} {title}".lower()

    for raw in blacklist:
        term = str(raw or "").strip()
        if not term:
            continue

        t = term.lower()

        if len(t) <= 3:
            pattern = r"(^|[^a-z0-9])" + re.escape(t) + r"([^a-z0-9]|$)"
            if re.search(pattern, haystack):
                return term
        else:
            if t in haystack:
                return term

    return ""


def apply_company_quality_one(job: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    quality = config.get("company_quality", {}) or {}
    filters = config.get("filters", {}) or {}

    preferred_companies = quality.get("preferred_companies", []) or []
    strong_exclude_terms = quality.get("strong_exclude_terms", []) or []
    risk_terms = quality.get("risk_terms", []) or []

    blacklist_companies = []
    blacklist_companies.extend(filters.get("blacklist_companies", []) or [])
    blacklist_companies.extend(quality.get("blacklist_companies", []) or [])

    weak_company_penalty = int(quality.get("weak_company_penalty", 8))
    preferred_company_bonus = int(quality.get("preferred_company_bonus", 5))
    risk_penalty = int(quality.get("risk_penalty", 20))

    original_score = int(job.get("original_score", job.get("score", 0)) or 0)
    adjusted = original_score

    company = str(job.get("company", "") or "").strip()
    title = str(job.get("title", "") or "").strip()
    source = str(job.get("source", "") or "").strip()
    url = str(job.get("url", "") or "").strip()

    source_url_text = f"{source} {url}".lower()
    is_jobsdb_or_jobstreet = "jobsdb" in source_url_text or "jobstreet" in source_url_text

    text = _text(job)
    reasons = []
    decision = "keep"

    blacklist_hit = _matches_company_blacklist(company, title, blacklist_companies)
    if blacklist_hit:
        job["original_score"] = original_score
        job["score"] = 0
        job["company_quality_decision"] = "exclude"
        job["company_quality_reason"] = f"Company is blacklisted: {blacklist_hit}."
        return job

    if _contains_any(text, strong_exclude_terms):
        decision = "exclude"
        adjusted = min(adjusted, 30)
        reasons.append("Strong sales / insurance / wealth-management risk keyword matched.")

    elif _contains_any(text, risk_terms):
        decision = "review"
        adjusted -= risk_penalty
        reasons.append("Potential sales-heavy or low-analytical-content signal matched.")

    company_lower = company.lower()
    for c in preferred_companies:
        if str(c).lower() in company_lower:
            adjusted += preferred_company_bonus
            reasons.append(f"Preferred company matched: {c}.")
            break

    fake_company_patterns = [
        "jobsdb hk",
        "jobstreet sg",
        "linkedin hk",
        "linkedin sg",
        "recent",
    ]

    if not company or any(p in company_lower for p in fake_company_patterns):
        if is_jobsdb_or_jobstreet:
            reasons.append("Company name is missing or looks like a source label, but no penalty applied for JobsDB/JobStreet.")
        else:
            adjusted -= weak_company_penalty
            reasons.append("Company name is missing or looks like a source label.")

    vague_consultant_patterns = [
        "personal consultant",
        "financial services consultant",
        "wealth consultant",
        "investment consultant",
        "sales consultant",
        "business development consultant",
    ]

    if any(p in text for p in vague_consultant_patterns):
        decision = "exclude"
        adjusted = min(adjusted, 35)
        reasons.append("Consultant title is likely sales / advisory-sales oriented.")

    adjusted = max(0, min(100, adjusted))

    job["original_score"] = original_score
    job["score"] = adjusted
    job["company_quality_decision"] = decision
    job["company_quality_reason"] = "; ".join(reasons)

    return job


def apply_company_quality(jobs: list[dict[str, Any]], config: dict[str, Any]) -> list[dict[str, Any]]:
    return [apply_company_quality_one(j, config) for j in jobs]


def should_hide_by_company_quality(job: dict[str, Any]) -> bool:
    return str(job.get("company_quality_decision", "")).lower() == "exclude"
