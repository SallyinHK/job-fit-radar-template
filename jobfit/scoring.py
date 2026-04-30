from __future__ import annotations

import json
import os
import re
from typing import Dict, List

import requests

from .models import Job, ScoreResult
from .utils import clean_text, safe_int


PRIORITY_COMPANIES = [
    "deloitte", "ey", "kpmg", "pwc", "bnp paribas", "hsbc", "standard chartered",
    "citi", "jpmorgan", "goldman", "morgan stanley", "american express", "amex",
    "mufg", "buzzacott", "fdm group", "visa", "mastercard", "bloomberg",
]

SENIOR_PATTERNS = [
    r"\bsenior\b", r"\bmanager\b", r"\bvp\b", r"vice president", r"\bdirector\b",
    r"head of", r"lead or principal", r"principal consultant", r"team leader",
    r"5\+?\s*years", r"7\+?\s*years", r"10\+?\s*years", r"five years", r"seven years",
]

EARLY_CAREER_TERMS = [
    "graduate", "fresh graduate", "entry level", "early career", "campus", "trainee",
    "management trainee", "intern", "internship", "junior", "associate", "analyst",
]

ROLE_BONUS_GROUPS = {
    "finance/accounting/valuation": ["finance", "fp&a", "accounting", "valuation", "corporate finance", "financial analyst"],
    "consulting/strategy/advisory": ["consulting", "consultant", "strategy", "advisory", "transformation", "business analyst"],
    "risk/compliance/kyc": ["risk", "compliance", "kyc", "aml", "financial crime", "regulatory"],
    "payment/fintech/data": ["payment", "fintech", "business intelligence", "data analyst", "ai analyst", "digital"],
}


def load_profile(path: str = "profile.md") -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def _has_any(text: str, terms: List[str]) -> bool:
    return any(t.lower() in text for t in terms)


def _regex_any(text: str, patterns: List[str]) -> bool:
    return any(re.search(p, text, re.I) for p in patterns)


def _keyword_score(job: Job, config: Dict) -> ScoreResult:
    text = f"{job.company} {job.title} {job.location} {job.description}".lower()
    title = (job.title or "").lower()
    company = (job.company or "").lower()
    positives = config.get("filters", {}).get("positive_keywords", [])
    negatives = config.get("filters", {}).get("negative_keywords", [])

    score = 38
    matched_pos = [k for k in positives if k.lower() in text]
    matched_neg = [k for k in negatives if k.lower() in text]

    # Broad relevance.
    score += min(28, len(matched_pos) * 4)

    loc = (job.location or "").lower()
    if any(x in loc for x in ["hong kong", "hong kong sar", "singapore", "seoul", "korea", "tokyo", "japan", "remote"]):
        score += 8

    # Early-career friendliness is important for this user's search.
    if _has_any(text, EARLY_CAREER_TERMS):
        score += 12
    if any(x in title for x in ["graduate", "trainee", "intern", "junior"]):
        score += 8
    elif any(x in title for x in ["analyst", "associate", "consultant"]):
        score += 6

    # Role family fit with the user's CV.
    role_family_hits = []
    for family, terms in ROLE_BONUS_GROUPS.items():
        if _has_any(text, terms):
            role_family_hits.append(family)
            score += 7

    if _has_any(company, PRIORITY_COMPANIES):
        score += 5

    # Strong penalties.
    senior = _regex_any(text, SENIOR_PATTERNS)
    if senior:
        score -= 22
    if any(x in text for x in ["native korean", "business korean required", "fluent korean required", "japanese n1", "japanese n2", "native japanese"]):
        score -= 18
    elif any(x in text for x in ["korean", "japanese", "n1", "n2"]):
        score -= 10
    irrelevant_title_terms = [
        "production coordinator", "web programmer", "marketing officer", "marketing manager",
        "inventory control", "project manager", "accountant", "accounts assistant",
        "sales manager", "sales executive", "barista", "transport officer",
        "technical maintenance", "warehouse", "catering", "admissions",
    ]
    if any(x in title for x in ["sales consultant", "recruitment consultant", "personal consultant", "team secretary", "administrative assistant"]):
        score -= 15
    if any(x in title for x in irrelevant_title_terms):
        score -= 24
    # Avoid over-rewarding generic junior words when the actual title is not in a target family.
    target_title_terms = ["analyst", "consultant", "consulting", "strategy", "risk", "compliance", "kyc", "finance", "trainee", "graduate", "intern", "business analyst", "transformation"]
    if not any(x in title for x in target_title_terms) and any(x in title for x in ["junior", "assistant", "officer", "coordinator"]):
        score -= 18
    if matched_neg:
        score -= min(25, len(matched_neg) * 8)

    score = max(0, min(100, score))
    recommendation = "Apply" if score >= 80 else "Maybe" if score >= 65 else "Skip"
    priority = "High" if score >= 80 else "Medium" if score >= 65 else "Low"
    reasons = []
    if matched_pos:
        reasons.append("Matches target keywords: " + ", ".join(matched_pos[:6]))
    if _has_any(text, EARLY_CAREER_TERMS):
        reasons.append("Looks potentially suitable for graduate / early-career search.")
    if role_family_hits:
        reasons.append("Relevant role family: " + ", ".join(role_family_hits[:2]))
    if _has_any(company, PRIORITY_COMPANIES):
        reasons.append("Priority company or target employer group.")
    if job.location:
        reasons.append(f"Relevant location: {job.location}.")

    risks = []
    if senior:
        risks.append("May be too senior or require several years of experience.")
    if matched_neg:
        risks.append("Potential mismatch keywords: " + ", ".join(matched_neg[:5]))
    if any(x in text for x in ["korean", "japanese", "n1", "n2"]):
        risks.append("Language requirement may be a concern.")
    if not job.description or len(job.description) < 80:
        risks.append("Limited description available; open link to verify details.")

    return ScoreResult(
        score=score,
        recommendation=recommendation,
        priority=priority,
        reasons=reasons[:3] or ["Rule-based score generated without AI API."],
        resume_keywords=(matched_pos[:8] + role_family_hits[:2])[:10],
        risks=risks[:3],
        summary="Rule-based score. Add OpenAI/Gemini API key for stronger judgement.",
    )


def _extract_json(text: str) -> Dict:
    text = text.strip()
    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, flags=re.S)
    if fenced:
        text = fenced.group(1)
    else:
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            text = text[start:end + 1]
    return json.loads(text)


def build_prompt(profile: str, job: Job) -> str:
    jd = clean_text(job.description, 9000)
    return f"""
You are a job application screening assistant. Compare the job with the candidate profile and return JSON only.

Candidate profile:
{profile}

Job:
Company: {job.company}
Title: {job.title}
Location: {job.location}
Source: {job.source}
Apply URL: {job.url}
Description:
{jd}

Scoring rules:
- 0-100 match score.
- Be strict about visa and language feasibility.
- Hong Kong is highest priority; Singapore is possible but EP sponsorship risk matters; Korea is possible under D-10 but limited Korean is a concern; Japan is low priority unless Japanese is not required.
- Fresh graduate / analyst / trainee roles should score higher if role relevance is good.
- Senior roles, hard local language requirements, and 5+ years experience should score lower.

Return exactly this JSON schema:
{{
  "score": 0,
  "recommendation": "Apply | Maybe | Skip",
  "priority": "High | Medium | Low",
  "reasons": ["reason 1", "reason 2", "reason 3"],
  "resume_keywords": ["keyword 1", "keyword 2"],
  "risks": ["risk 1", "risk 2"],
  "summary": "one sentence recommendation"
}}
""".strip()


def _score_openai(prompt: str) -> Dict:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    model = os.getenv("OPENAI_MODEL", "gpt-5.4-mini")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is missing")
    r = requests.post(
        "https://api.openai.com/v1/responses",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": model,
            "input": "Return valid JSON only.\n\n" + prompt,
        },
        timeout=60,
    )
    r.raise_for_status()
    data = r.json()
    content = data.get("output_text") or ""
    if not content:
        parts = []
        for item in data.get("output", []) or []:
            for c in item.get("content", []) or []:
                if c.get("type") in {"output_text", "text"}:
                    parts.append(c.get("text", ""))
        content = "".join(parts)
    return _extract_json(content)


def _score_gemini(prompt: str) -> Dict:
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    model = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is missing")
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
    r = requests.post(
        url,
        headers={"Content-Type": "application/json"},
        json={
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.2, "response_mime_type": "application/json"},
        },
        timeout=60,
    )
    r.raise_for_status()
    content = r.json()["candidates"][0]["content"]["parts"][0]["text"]
    return _extract_json(content)


def score_job(job: Job, profile: str, config: Dict) -> ScoreResult:
    provider = os.getenv("AI_PROVIDER", "rules").strip().lower()
    pre_score = _keyword_score(job, config)
    if provider in {"", "rules", "rule"}:
        return pre_score

    # Cost guard: only call the LLM for roles that pass a rough rule-based pre-filter.
    ai_min = int(config.get("ai", {}).get("min_rule_score", 70))
    if pre_score.score < ai_min:
        return pre_score

    prompt = build_prompt(profile, job)
    try:
        if provider == "openai":
            data = _score_openai(prompt)
        elif provider == "gemini":
            data = _score_gemini(prompt)
        else:
            raise RuntimeError(f"Unsupported AI_PROVIDER: {provider}")
        score = max(0, min(100, safe_int(data.get("score"), 0)))
        recommendation = data.get("recommendation") or ("Apply" if score >= 80 else "Maybe" if score >= 65 else "Skip")
        priority = data.get("priority") or ("High" if score >= 80 else "Medium" if score >= 65 else "Low")
        return ScoreResult(
            score=score,
            recommendation=recommendation,
            priority=priority,
            reasons=list(data.get("reasons") or [])[:3],
            resume_keywords=list(data.get("resume_keywords") or [])[:8],
            risks=list(data.get("risks") or [])[:4],
            summary=str(data.get("summary") or ""),
        )
    except Exception as e:
        fallback = _keyword_score(job, config)
        fallback.summary = f"AI scoring failed, used rule fallback. Error: {e}"
        return fallback

def _job_value(job, field, default=""):
    """Read field from either a Job dataclass object or a dict."""
    if isinstance(job, dict):
        return job.get(field, default)
    return getattr(job, field, default)


def is_excluded_job(job, config):
    """
    Hard exclude obviously irrelevant roles before scoring.
    Uses title/company/source mainly, so we don't accidentally exclude a good role
    just because the description mentions a negative word once.
    """
    filters = config.get("filters", {}) or {}
    exclude_keywords = filters.get("exclude_keywords", []) or []
    if not exclude_keywords:
        return False

    title = str(_job_value(job, "title", "") or "").lower()
    company = str(_job_value(job, "company", "") or "").lower()
    source = str(_job_value(job, "source", "") or "").lower()

    text = f"{title} {company} {source}"

    for kw in exclude_keywords:
        kw = str(kw or "").strip().lower()
        if kw and kw in text:
            return True

    return False
