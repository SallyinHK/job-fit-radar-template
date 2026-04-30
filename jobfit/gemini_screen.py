from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Any

try:
    from google import genai
except Exception:
    genai = None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def clean_company(company: str, source: str = "") -> str:
    """Remove fake company labels such as JobsDB HK - xxx recent."""
    c = str(company or "").strip()
    s = str(source or "").strip()

    bad_prefixes = [
        "JobsDB HK",
        "JobStreet SG",
        "LinkedIn HK",
        "LinkedIn SG",
        "LinkedIn Korea",
        "LinkedIn Japan",
    ]

    if not c:
        return ""

    if c == s:
        return ""

    if any(c.startswith(x) for x in bad_prefixes):
        return ""

    if " recent" in c and ("JobsDB" in c or "JobStreet" in c):
        return ""

    return c


def _safe_json(text: str) -> dict[str, Any]:
    text = (text or "").strip()
    text = re.sub(r"^```json\s*", "", text)
    text = re.sub(r"^```\s*", "", text)
    text = re.sub(r"\s*```$", "", text)

    try:
        return json.loads(text)
    except Exception:
        pass

    m = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not m:
        return {}

    try:
        return json.loads(m.group(0))
    except Exception:
        return {}


def _short(value: Any, n: int = 1800) -> str:
    text = str(value or "")
    return text[:n]


def _job_text(job: dict[str, Any]) -> str:
    parts = [
        f"Title: {job.get('title', '')}",
        f"Company: {job.get('company', '')}",
        f"Location: {job.get('location', '')}",
        f"Source: {job.get('source', '')}",
        f"URL: {job.get('url', '')}",
        f"Existing rule reasons: {job.get('reasons', '')}",
        f"Description: {_short(job.get('description', ''), 2200)}",
    ]
    return "\n".join(parts)


def _build_prompt(job: dict[str, Any], profile_text: str) -> str:
    return f"""
You are screening job postings for a graduate job seeker.

Main task:
Identify roles that are highly likely to be disguised insurance sales, wealth management sales, financial planning sales, agency recruitment, relationship-manager sales, lead-generation sales, or other sales-heavy roles with weak analytical content.

Important:
- Do NOT exclude a role only because it is in finance.
- Keep genuine analytical roles: finance analyst, business analyst, compliance analyst, risk analyst, KYC, operations analyst, consulting, transformation, data analyst.
- Exclude or mark as review if the role is likely sales target driven, commission driven, agency recruitment, insurance/IFA/wealth planning sales, or mainly lead generation.
- If the company field looks like a source label such as "JobsDB HK - xxx recent", do not treat it as a real company.
- Only return a company_name if it is clearly visible from the job text. Do not invent one.

User background summary:
{_short(profile_text, 3000)}

Job posting:
{_job_text(job)}

Return JSON only, no markdown:
{{
  "decision": "keep" | "exclude" | "review",
  "confidence": 0-100,
  "is_sales_trap": true | false,
  "company_name": "actual company name if clearly visible, otherwise empty string",
  "reason": "one short reason in English",
  "red_flags": ["short flag 1", "short flag 2"]
}}
""".strip()


def screen_one_job(job: dict[str, Any], profile_text: str, config: dict[str, Any]) -> dict[str, Any]:
    provider = os.getenv("GEMINI_SCREENING_PROVIDER") or config.get("ai", {}).get("provider") or "rules"
    if str(provider or "").lower() != "gemini":
        return job

    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        job["ai_decision"] = "not_checked"
        job["ai_reason"] = "GEMINI_API_KEY missing"
        return job

    if genai is None:
        job["ai_decision"] = "not_checked"
        job["ai_reason"] = "google-genai package not installed"
        return job

    if job.get("ai_checked_at") and job.get("ai_model"):
        return job

    model = os.getenv("GEMINI_MODEL") or config.get("ai_screening", {}).get("model") or config.get("ai", {}).get("model") or "gemini-2.5-flash"

    try:
        client = genai.Client(api_key=api_key)
        prompt = _build_prompt(job, profile_text)

        response = client.models.generate_content(
            model=model,
            contents=prompt,
        )

        data = _safe_json(getattr(response, "text", "") or "")

        decision = str(data.get("decision", "review")).lower()
        if decision not in {"keep", "exclude", "review"}:
            decision = "review"

        confidence = int(data.get("confidence", 0) or 0)
        is_sales_trap = bool(data.get("is_sales_trap", False))
        company_name = clean_company(str(data.get("company_name", "") or ""), job.get("source", ""))

        job["ai_decision"] = decision
        job["ai_confidence"] = confidence
        job["ai_sales_trap"] = is_sales_trap
        job["ai_reason"] = str(data.get("reason", "") or "")
        job["ai_red_flags"] = data.get("red_flags", []) if isinstance(data.get("red_flags", []), list) else []
        job["ai_checked_at"] = _now_iso()
        job["ai_model"] = model

        if company_name and not clean_company(job.get("company", ""), job.get("source", "")):
            job["company"] = company_name

    except Exception as e:
        job["ai_decision"] = "not_checked"
        job["ai_reason"] = f"Gemini error: {type(e).__name__}"

    return job


def should_hide_job(job: dict[str, Any], config: dict[str, Any]) -> bool:
    screening = config.get("ai_screening", {}) or {}
    exclude_confidence = int(screening.get("exclude_confidence", 70))

    decision = str(job.get("ai_decision", "") or "").lower()
    confidence = int(job.get("ai_confidence", 0) or 0)
    is_sales_trap = bool(job.get("ai_sales_trap", False))

    return decision == "exclude" and is_sales_trap and confidence >= exclude_confidence


def screen_jobs_with_gemini(jobs: list[dict[str, Any]], profile_text: str, config: dict[str, Any]) -> list[dict[str, Any]]:
    provider = os.getenv("GEMINI_SCREENING_PROVIDER") or config.get("ai", {}).get("provider") or "rules"
    if str(provider or "").lower() != "gemini":
        return jobs

    screening = config.get("ai_screening", {}) or {}
    candidate_limit = int(screening.get("candidate_limit", 60))
    min_score = int(screening.get("min_score", 75))
    request_delay_seconds = float(screening.get("request_delay_seconds", 0) or 0)

    checked = 0
    output = []

    for job in sorted(jobs, key=lambda x: int(x.get("score", 0)), reverse=True):
        if checked < candidate_limit and int(job.get("score", 0)) >= min_score:
            job = screen_one_job(job, profile_text, config)
            checked += 1
            if request_delay_seconds > 0 and checked < candidate_limit:
                time.sleep(request_delay_seconds)
        output.append(job)

    print(f"Gemini screening checked {checked} job(s).")
    return output
