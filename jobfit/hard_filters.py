from __future__ import annotations

import re
from typing import Any


def _job_text(job: dict[str, Any]) -> str:
    parts = [
        job.get("title", ""),
        job.get("company", ""),
        job.get("source", ""),
        job.get("description", ""),
        " ".join(job.get("reasons", []) if isinstance(job.get("reasons"), list) else []),
        job.get("company_quality_reason", ""),
    ]
    return " ".join(str(x or "") for x in parts).lower()


def required_years_exceeds(job: dict[str, Any], config: dict[str, Any]) -> bool:
    max_allowed = int(config.get("filters", {}).get("max_required_years", 2))
    text = _job_text(job)

    patterns = [
        # minimum 5 years / at least 4 years / more than 3 years
        r"\b(?:at\s+least|min(?:imum)?(?:\s+of)?|over|more\s+than|no\s+less\s+than)\s+(\d{1,2})\+?\s*(?:years?|yrs?)\+?\b",

        # 5+ years of experience / 5 years relevant experience / 5 yrs exp
        r"\b(\d{1,2})\+?\s*(?:years?|yrs?)\+?\s+(?:of\s+)?(?:relevant\s+)?(?:work\s+)?(?:professional\s+)?(?:experience|exp)\b",

        # 5-8 years of relevant experience
        r"\b(\d{1,2})\s*[-–—]\s*(\d{1,2})\s*(?:years?|yrs?)\s+(?:of\s+)?(?:relevant\s+)?(?:work\s+)?(?:professional\s+)?(?:experience|exp)\b",

        # 5-8 years
        r"\b(\d{1,2})\s*[-–—]\s*(\d{1,2})\s*(?:years?|yrs?)\b",

        # experience of 5 years
        r"\b(?:experience|exp)\s+(?:of\s+)?(\d{1,2})\+?\s*(?:years?|yrs?)\+?\b",

        # 5 years+
        r"\b(\d{1,2})\s*(?:years?|yrs?)\s*\+\b",
    ]

    for pattern in patterns:
        for m in re.finditer(pattern, text, flags=re.IGNORECASE):
            nums = [int(x) for x in m.groups() if x and str(x).isdigit()]
            if not nums:
                continue

            required = min(nums)
            snippet = text[max(0, m.start() - 100): m.end() + 140]

            has_requirement_context = any(x in snippet for x in [
                "experience",
                "exp",
                "requirement",
                "qualification",
                "minimum",
                "at least",
                "relevant",
                "work",
                "professional",
                "role requirements",
                "skills and qualifications",
            ])

            is_strong_year_pattern = (
                "+" in m.group(0)
                or "-" in m.group(0)
                or "–" in m.group(0)
                or "—" in m.group(0)
            )

            if required > max_allowed and (has_requirement_context or is_strong_year_pattern):
                return True

    return False


def company_blacklisted(job: dict[str, Any], config: dict[str, Any]) -> bool:
    text = _job_text(job)
    companies = config.get("filters", {}).get("blacklist_companies", []) or []

    for c in companies:
        term = str(c or "").strip().lower()
        if not term:
            continue

        if len(term) <= 3:
            pattern = r"(^|[^a-z0-9])" + re.escape(term) + r"([^a-z0-9]|$)"
            if re.search(pattern, text):
                return True
        elif term in text:
            return True

    return False


def tech_role_mismatch(job: dict[str, Any], config: dict[str, Any]) -> bool:
    filters = config.get("filters", {}) or {}

    title = str(job.get("title", "") or "").lower()
    text = _job_text(job)

    title_terms = filters.get("tech_mismatch_title_keywords", []) or []
    erp_terms = filters.get("erp_implementation_keywords", []) or []
    backend_stack_terms = filters.get("backend_stack_keywords", []) or []

    if any(str(term).lower() in title for term in title_terms):
        return True

    if any(str(term).lower() in text for term in erp_terms):
        if any(x in title for x in ["consultant", "engineer", "implementation", "oracle", "erp", "ebs", "fusion"]):
            return True
        if "oracle ebs" in text or "oracle fusion" in text or "ebs/fusion" in text:
            return True

    stack_hits = [term for term in backend_stack_terms if str(term).lower() in text]
    if len(stack_hits) >= 3 and any(x in title for x in ["programmer", "developer", "engineer", "software", "application"]):
        return True

    return False


def hard_exclude_reason(job: dict[str, Any], config: dict[str, Any]) -> str:
    if company_blacklisted(job, config):
        return "Company is on the blacklist."

    if required_years_exceeds(job, config):
        return "Role requires more years of experience than allowed."

    if tech_role_mismatch(job, config):
        return "Role is a technical developer or ERP implementation mismatch."

    return ""


def is_hard_excluded(job: dict[str, Any], config: dict[str, Any]) -> bool:
    return bool(hard_exclude_reason(job, config))
