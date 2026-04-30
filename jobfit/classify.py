from __future__ import annotations


def row_value(row, key: str, default: str = ""):
    """Read value from sqlite3.Row, dict, or dataclass-like object."""
    try:
        if isinstance(row, dict):
            return row.get(key, default)
        if hasattr(row, "keys") and key in row.keys():
            return row[key]
    except Exception:
        pass
    return getattr(row, key, default)


def source_label(row) -> str:
    url = str(row_value(row, "url", "") or "").lower()
    location = str(row_value(row, "location", "") or "").lower()
    company = str(row_value(row, "company", "") or "").lower()

    if "hk.jobsdb.com" in url:
        return "JobsDB HK"
    if "sg.jobstreet.com" in url or "jobstreet.com" in url:
        return "JobStreet SG"
    if "linkedin.com" in url:
        if "singapore" in location:
            return "LinkedIn SG"
        if "korea" in location or "seoul" in location:
            return "LinkedIn Korea"
        if "japan" in location or "tokyo" in location:
            return "LinkedIn Japan"
        if "hong" in location:
            return "LinkedIn HK"
        return "LinkedIn"
    if any(x in url or x in company for x in ["deloitte", "ey.com", "kpmg", "pwc", "bnpparibas", "hotjob"]):
        return "Official"
    return "Other"


def classify_job_type(row) -> str:
    title = str(row_value(row, "title", "") or "").lower()
    company = str(row_value(row, "company", "") or "").lower()
    url = str(row_value(row, "url", "") or "").lower()
    source = source_label(row).lower()

    text = f"{title} {company} {url} {source}"

    official_words = [
        "campus recruitment",
        "early career opportunities",
        "graduate programme",
        "graduate program",
        "students",
        "campus hires",
        "apply now",
        "register your interest",
    ]

    intern_words = [
        "intern",
        "internship",
        "summer internship",
        "winter internship",
        "trainee internship",
        "placement",
        "temporary",
        "part-time",
        "part time",
        "contract",
        "1-year contract",
        "12 month",
        "12-month",
        "project based",
    ]

    full_time_words = [
        "graduate analyst",
        "graduate program",
        "graduate programme",
        "management trainee",
        "analyst",
        "associate",
        "consultant",
        "business analyst",
        "finance analyst",
        "risk analyst",
        "compliance analyst",
        "strategy analyst",
        "research analyst",
    ]

    if source_label(row) == "Official" and any(w in text for w in official_words):
        return "Official Portal"

    if any(w in text for w in intern_words):
        return "Internship / Temporary"

    if any(w in text for w in full_time_words):
        return "Full-time / Graduate"

    return "Other / Review"


def short_job_type(row) -> str:
    t = classify_job_type(row)
    if t == "Full-time / Graduate":
        return "FT"
    if t == "Internship / Temporary":
        return "Intern"
    if t == "Official Portal":
        return "Official"
    return "Review"
