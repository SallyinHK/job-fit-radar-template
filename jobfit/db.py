from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple

from .models import Job, ScoreResult
from .utils import fingerprint


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect(path: str) -> sqlite3.Connection:
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    return con


def init_db(con: sqlite3.Connection) -> None:
    con.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fp TEXT UNIQUE NOT NULL,
        company TEXT,
        title TEXT,
        location TEXT,
        url TEXT,
        description TEXT,
        source TEXT,
        posted_at TEXT,
        first_seen_at TEXT,
        last_seen_at TEXT,
        score INTEGER,
        recommendation TEXT,
        priority TEXT,
        reasons_json TEXT,
        resume_keywords_json TEXT,
        risks_json TEXT,
        summary TEXT,
        sent_at TEXT,
        raw_json TEXT
    )
    """)
    con.commit()


def is_new_job(con: sqlite3.Connection, job: Job) -> Tuple[bool, str]:
    fp = fingerprint(job.company, job.title, job.location, job.url)
    row = con.execute("SELECT id FROM jobs WHERE fp = ?", (fp,)).fetchone()
    return row is None, fp


def upsert_job(con: sqlite3.Connection, job: Job, score: ScoreResult) -> bool:
    fp = fingerprint(job.company, job.title, job.location, job.url)
    created = now_iso()
    row = con.execute("SELECT id, sent_at FROM jobs WHERE fp = ?", (fp,)).fetchone()
    payload = (
        fp, job.company, job.title, job.location, job.url, job.description,
        job.source, job.posted_at, created, created, score.score,
        score.recommendation, score.priority, json.dumps(score.reasons, ensure_ascii=False),
        json.dumps(score.resume_keywords, ensure_ascii=False), json.dumps(score.risks, ensure_ascii=False),
        score.summary, json.dumps(job.raw, ensure_ascii=False),
    )
    if row is None:
        con.execute("""
        INSERT INTO jobs (
            fp, company, title, location, url, description, source, posted_at,
            first_seen_at, last_seen_at, score, recommendation, priority,
            reasons_json, resume_keywords_json, risks_json, summary, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, payload)
        con.commit()
        return True
    else:
        con.execute("""
        UPDATE jobs SET
            company=?, title=?, location=?, url=?, description=?, source=?, posted_at=?,
            last_seen_at=?, score=?, recommendation=?, priority=?, reasons_json=?,
            resume_keywords_json=?, risks_json=?, summary=?, raw_json=?
        WHERE fp=?
        """, (
            job.company, job.title, job.location, job.url, job.description, job.source,
            job.posted_at, created, score.score, score.recommendation, score.priority,
            json.dumps(score.reasons, ensure_ascii=False),
            json.dumps(score.resume_keywords, ensure_ascii=False),
            json.dumps(score.risks, ensure_ascii=False),
            score.summary, json.dumps(job.raw, ensure_ascii=False), fp,
        ))
        con.commit()
        return False


def get_unsent_high_score(con: sqlite3.Connection, threshold: int, limit: int = 20) -> List[sqlite3.Row]:
    return con.execute(
        """
        SELECT * FROM jobs
        WHERE sent_at IS NULL AND score >= ?
        ORDER BY score DESC, first_seen_at DESC
        LIMIT ?
        """,
        (threshold, limit),
    ).fetchall()


def mark_sent(con: sqlite3.Connection, ids: Iterable[int]) -> None:
    ts = now_iso()
    con.executemany("UPDATE jobs SET sent_at = ? WHERE id = ?", [(ts, int(i)) for i in ids])
    con.commit()


def high_score_jobs(con: sqlite3.Connection, threshold: int, limit: int = 200) -> List[sqlite3.Row]:
    return con.execute(
        """
        SELECT * FROM jobs
        WHERE score >= ?
        ORDER BY score DESC, first_seen_at DESC
        LIMIT ?
        """,
        (threshold, limit),
    ).fetchall()


def recent_jobs(con: sqlite3.Connection, limit: int = 20) -> List[sqlite3.Row]:
    return con.execute("SELECT * FROM jobs ORDER BY first_seen_at DESC LIMIT ?", (limit,)).fetchall()


def dashboard_jobs_with_region_picks(con: sqlite3.Connection, threshold: int, limit: int = 200, min_region_score: int = 50) -> List[sqlite3.Row]:
    """Return normal high-score jobs plus at least one top job per region when available.

    This keeps the dashboard useful for cross-border search:
    - main shortlist: score >= threshold
    - regional coverage: top 1 from HK / Singapore / Korea / Japan if not already shown
    """
    rows = con.execute(
        """
        SELECT * FROM jobs
        WHERE score >= ?
        ORDER BY score DESC, first_seen_at DESC
        LIMIT ?
        """,
        (threshold, limit),
    ).fetchall()

    seen_ids = {int(row["id"]) for row in rows}

    region_filters = {
        "Hong Kong": """
            location LIKE '%Hong Kong%'
            OR location LIKE '%Hong%'
            OR url LIKE '%hk.jobsdb.com%'
            OR url LIKE '%location=Hong%20Kong%'
        """,
        "Singapore": """
            location LIKE '%Singapore%'
            OR url LIKE '%sg.jobstreet.com%'
            OR url LIKE '%location=Singapore%'
        """,
        "Korea": """
            location LIKE '%Korea%'
            OR location LIKE '%Seoul%'
            OR url LIKE '%South%20Korea%'
            OR url LIKE '%Seoul%'
        """,
        "Japan": """
            location LIKE '%Japan%'
            OR location LIKE '%Tokyo%'
            OR url LIKE '%Tokyo%2C%20Japan%'
            OR url LIKE '%location=Tokyo%'
        """,
    }

    for region, where_clause in region_filters.items():
        # If this region is already represented in the high-score list, skip.
        already_has_region = False
        for row in rows:
            loc = (row["location"] or "").lower()
            url = (row["url"] or "").lower()
            if region == "Hong Kong" and ("hong" in loc or "hk.jobsdb.com" in url):
                already_has_region = True
            elif region == "Singapore" and ("singapore" in loc or "sg.jobstreet.com" in url):
                already_has_region = True
            elif region == "Korea" and ("korea" in loc or "seoul" in loc or "south%20korea" in url):
                already_has_region = True
            elif region == "Japan" and ("japan" in loc or "tokyo" in loc or "tokyo%2c%20japan" in url):
                already_has_region = True

        if already_has_region:
            continue

        row = con.execute(
            f"""
            SELECT * FROM jobs
            WHERE ({where_clause})
              AND score >= ?
            ORDER BY score DESC, first_seen_at DESC
            LIMIT 1
            """,
            (min_region_score,),
        ).fetchone()

        if row is not None and int(row["id"]) not in seen_ids:
            rows.append(row)
            seen_ids.add(int(row["id"]))

    rows = sorted(rows, key=lambda r: (int(r["score"] or 0), r["first_seen_at"] or ""), reverse=True)
    return rows[:limit]
