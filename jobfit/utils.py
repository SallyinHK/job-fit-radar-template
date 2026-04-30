import hashlib
import html
import re
from bs4 import BeautifulSoup


def clean_text(value: str, max_len: int | None = None) -> str:
    if not value:
        return ""
    value = html.unescape(str(value))
    # Avoid BeautifulSoup warnings when the input is just a URL or short plain text.
    if "<" not in value and ">" not in value:
        text = value
    else:
        soup = BeautifulSoup(value, "lxml")
        text = soup.get_text(" ")
    text = re.sub(r"\s+", " ", text).strip()
    if max_len and len(text) > max_len:
        return text[:max_len].rsplit(" ", 1)[0] + "..."
    return text


def normalize(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (value or "").lower()).strip()


def fingerprint(company: str, title: str, location: str, url: str) -> str:
    # Prefer the actual apply URL when available, so the same JobsDB/LinkedIn role
    # found through several keyword searches is not saved repeatedly.
    u = (url or "").split("?")[0].split("#")[0].rstrip("/").lower()
    if u:
        # Normalize common job-board URLs to the stable job id.
        m = re.search(r"/job/(\d+)", u)
        if m and "jobsdb.com" in u:
            base = f"jobsdb:{m.group(1)}"
        else:
            m = re.search(r"/jobs/view/.*?(\d+)$", u)
            base = f"linkedin:{m.group(1)}" if m and "linkedin.com" in u else u
    else:
        base = f"{normalize(company)}|{normalize(title)}|{normalize(location)}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default
