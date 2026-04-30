from __future__ import annotations

import html
import os
import re
from textwrap import wrap

import requests

from jobfit.telegram import send_telegram_message


def html_to_text(text: str) -> str:
    """Convert the existing Telegram HTML message to readable plain text."""
    text = re.sub(r"<br\s*/?>", "\n", text or "", flags=re.IGNORECASE)
    text = re.sub(r"</p\s*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    return html.unescape(text).strip()


def send_ntfy_message(text: str) -> None:
    topic = os.getenv("NTFY_TOPIC", "").strip()
    server = os.getenv("NTFY_SERVER", "https://ntfy.sh").strip().rstrip("/")
    if not topic:
        raise RuntimeError("NTFY_TOPIC is missing. Put a random topic name into .env, e.g. NTFY_TOPIC=your-random-topic-name")
    url = f"{server}/{topic}"
    plain = html_to_text(text)
    headers = {
        "Title": os.getenv("NTFY_TITLE", "Job Fit Radar"),
        "Tags": os.getenv("NTFY_TAGS", "briefcase"),
        "Priority": os.getenv("NTFY_PRIORITY", "default"),
    }
    r = requests.post(url, data=plain.encode("utf-8"), headers=headers, timeout=30)
    r.raise_for_status()


def send_discord_message(text: str) -> None:
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not webhook_url:
        raise RuntimeError("DISCORD_WEBHOOK_URL is missing")
    plain = html_to_text(text)
    # Discord messages have a 2000 character limit. Send chunks if needed.
    chunks = []
    while plain:
        chunks.append(plain[:1900])
        plain = plain[1900:]
    for chunk in chunks or ["Job Fit Radar: no message content"]:
        r = requests.post(webhook_url, json={"content": chunk}, timeout=30)
        r.raise_for_status()


def send_notification(text: str) -> None:
    notifier = os.getenv("NOTIFIER", "telegram").strip().lower()
    if notifier == "telegram":
        send_telegram_message(text)
    elif notifier == "ntfy":
        send_ntfy_message(text)
    elif notifier == "discord":
        send_discord_message(text)
    else:
        raise RuntimeError("Unsupported NOTIFIER. Use telegram, ntfy, or discord.")
