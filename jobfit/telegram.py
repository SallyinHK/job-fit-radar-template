from __future__ import annotations

import json
import os
from collections import Counter
from typing import List

import requests

from jobfit.classify import source_label, short_job_type


def _escape_html(text: str) -> str:
    return (text or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _source_label(row) -> str:
    url = (row["url"] or "").lower()
    company = (row["company"] or "").lower()

    if "jobsdb.com" in url or "jobstreet.com" in url:
        return "JobsDB/JobStreet"
    if "linkedin.com" in url:
        return "LinkedIn"
    if any(x in url or x in company for x in ["deloitte", "ey.com", "kpmg", "pwc", "bnpparibas", "hotjob"]):
        return "Official"
    return "Other"


def send_telegram_message(text: str) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        raise RuntimeError("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is missing")
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    r = requests.post(url, json={
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }, timeout=30)
    r.raise_for_status()


def get_chat_id_updates() -> dict:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is missing")
    url = f"https://api.telegram.org/bot{token}/getUpdates"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


def _loads(value: str):
    try:
        return json.loads(value or "[]")
    except Exception:
        return []


def format_jobs_message(
    rows: List,
    title: str = "Job Fit Radar",
    max_roles: int = 10,
    dashboard_path: str | None = None,
    total_high: int | None = None
) -> str:
    """Compact mobile notification: source + title + score + link only."""
    if not rows:
        return f"<b>{_escape_html(title)}</b>\nNo high-match new roles found this round."

    count_text = total_high if total_high is not None else len(rows)
    shown = min(max_roles, len(rows))

    source_counts = Counter(_source_label(row) for row in rows)
    source_summary = " · ".join(f"{k} {v}" for k, v in source_counts.items())

    lines = [
        f"<b>{_escape_html(title)}</b>",
        f"{count_text} high-match role(s). Showing top {shown}.",
        f"Sources: {source_summary}",
        f"Dashboard: {os.getenv('DASHBOARD_URL') or dashboard_path or 'python main.py dashboard'}",
    ]

    for idx, row in enumerate(rows[:max_roles], start=1):
        label = _source_label(row)
        title_text = _escape_html(row["title"])
        score = row["score"]
        recommendation = _escape_html(row["recommendation"] or "Review")
        url = row["url"] or ""

        lines.append("")
        lines.append(f"{idx}. [{label} | {short_job_type(row)}] <b>{title_text}</b>")
        lines.append(f"Score: <b>{score}/100</b> | {recommendation}")
        if url:
            lines.append(f"Apply: {url}")

    return "\n".join(lines)
