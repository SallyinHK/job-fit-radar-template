from __future__ import annotations

import html
import json
import os
import shutil
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import yaml
from dotenv import load_dotenv

from jobfit.classify import classify_job_type, short_job_type, source_label
from jobfit.gemini_screen import clean_company, screen_jobs_with_gemini, should_hide_job
from jobfit.company_quality import apply_company_quality, should_hide_by_company_quality
from jobfit.hard_filters import is_hard_excluded
from jobfit.detail_enrich import enrich_job_descriptions
from main import scan_once

load_dotenv()


STATE_PATH = Path("cloud_state.json")
PUBLIC_JOBS_PATH = Path("cloud_jobs.json")
DOCS_PATH = Path("docs/index.html")

FAST_INTERVAL_SECONDS = 12 * 60 * 60
SLOW_INTERVAL_SECONDS = 36 * 60 * 60
SLOW_INITIAL_DELAY_SECONDS = 6 * 60 * 60

RETENTION_DAYS = 7


def now_ts() -> float:
    return time.time()


def now_dt() -> datetime:
    return datetime.now(timezone.utc)


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M")


def iso_now() -> str:
    return now_dt().isoformat()


def parse_iso(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return now_dt()


def esc(x) -> str:
    return html.escape(str(x or ""))


def load_json(path: Path, default):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return default


def save_json(path: Path, data) -> None:
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def load_state() -> dict:
    current = now_ts()
    default = {
        "created_at": current,
        "last_fast_scan": 0,
        "last_slow_scan": current - SLOW_INTERVAL_SECONDS + SLOW_INITIAL_DELAY_SECONDS,
        "sent_urls": [],
    }
    return load_json(STATE_PATH, default)


def save_state(state: dict) -> None:
    save_json(STATE_PATH, state)


def load_config() -> dict:
    with open("config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def threshold() -> int:
    """Broad minimum threshold used for initial database query.

    If source-specific thresholds exist, use the lowest value so we do not
    accidentally discard jobs before source-level filtering.
    """
    config = load_config()
    thresholds = config.get("run", {}).get("score_thresholds", {})

    if isinstance(thresholds, dict) and thresholds:
        return int(min(thresholds.values()))

    return int(config.get("run", {}).get("score_threshold", config.get("score_threshold", 80)))


def threshold_for_item(item) -> int:
    """Return source-specific threshold for a sqlite row or public job dict."""
    config = load_config()
    thresholds = config.get("run", {}).get("score_thresholds", {}) or {}

    default = int(config.get("run", {}).get("score_threshold", config.get("score_threshold", 80)))

    url = ""
    source = ""

    try:
        if isinstance(item, dict):
            url = str(item.get("url", "") or "").lower()
            source = str(item.get("source", "") or "").lower()
        elif hasattr(item, "keys"):
            url = str(item["url"] or "").lower()
            source = str(source_label(item) or "").lower()
    except Exception:
        pass

    if "jobsdb.com" in url or "jobstreet.com" in url or "jobsdb" in source or "jobstreet" in source:
        return int(thresholds.get("jobsdb", default))

    if "linkedin.com" in url or "linkedin" in source:
        return int(thresholds.get("linkedin", default))

    if any(x in url for x in ["deloitte", "ey.com", "kpmg", "pwc", "bnpparibas", "hotjob"]):
        return int(thresholds.get("official", default))

    return default


def safe_loads(value):
    try:
        if not value:
            return []
        if isinstance(value, list):
            return value
        return json.loads(value)
    except Exception:
        return []


def get_rows(min_score: int = 0, limit: int = 300):
    db_path = os.getenv("DATABASE_PATH", "jobs.db")
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    rows = con.execute(
        """
        SELECT *
        FROM jobs
        WHERE score >= ?
        ORDER BY score DESC, first_seen_at DESC
        LIMIT ?
        """,
        (min_score, limit),
    ).fetchall()
    con.close()
    return rows


def row_to_public_job(row) -> dict:
    reasons = safe_loads(row["reasons_json"] if "reasons_json" in row.keys() else "")
    reasons = [str(x) for x in reasons if str(x).strip()][:3]

    src = source_label(row)
    company = clean_company(row["company"], src)

    description = ""
    try:
        if "description" in row.keys():
            description = row["description"] or ""
    except Exception:
        description = ""

    return {
        "title": row["title"],
        "company": company,
        "location": row["location"],
        "url": row["url"],
        "score": int(row["score"] or 0),
        "recommendation": row["recommendation"] if "recommendation" in row.keys() else "Review",
        "priority": row["priority"] if "priority" in row.keys() else "",
        "source": src,
        "job_type": classify_job_type(row),
        "reasons": reasons,
        "description": description[:3000],
        "first_seen_at": iso_now(),
        "last_seen_at": iso_now(),
    }


def merge_recent_jobs(current_rows) -> list[dict]:
    """Merge current scan results into public-safe 7-day job history."""
    existing = load_json(PUBLIC_JOBS_PATH, [])
    by_url = {}

    for item in existing:
        url = item.get("url")
        if url:
            by_url[url] = item

    for row in current_rows:
        item = row_to_public_job(row)
        url = item.get("url")
        if not url:
            continue

        if url in by_url:
            old = by_url[url]
            old.update({
                "title": item["title"],
                "company": item["company"],
                "location": item["location"],
                "score": max(int(old.get("score", 0)), item["score"]),
                "recommendation": item["recommendation"],
                "priority": item["priority"],
                "source": item["source"],
                "job_type": item["job_type"],
                "reasons": item["reasons"] or old.get("reasons", []),
                "last_seen_at": iso_now(),
            })
        else:
            by_url[url] = item

    cutoff = now_dt() - timedelta(days=RETENTION_DAYS)
    kept = []
    for item in by_url.values():
        last_seen = parse_iso(item.get("last_seen_at", ""))
        if last_seen >= cutoff:
            kept.append(item)

    kept.sort(key=lambda x: (int(x.get("score", 0)), x.get("last_seen_at", "")), reverse=True)
    save_json(PUBLIC_JOBS_PATH, kept)
    return kept


def add_region_representatives(rows: list[dict]) -> list[dict]:
    """Ensure dashboard has at least one representative job per region if available."""
    regions = {
        "Hong Kong": lambda x: "hong" in str(x.get("location", "")).lower() or "hk.jobsdb.com" in str(x.get("url", "")).lower(),
        "Singapore": lambda x: "singapore" in str(x.get("location", "")).lower() or "sg.jobstreet.com" in str(x.get("url", "")).lower(),
        "Korea": lambda x: "korea" in str(x.get("location", "")).lower() or "seoul" in str(x.get("location", "")).lower(),
        "Japan": lambda x: "japan" in str(x.get("location", "")).lower() or "tokyo" in str(x.get("location", "")).lower(),
    }

    selected = list(rows)
    selected_urls = {x.get("url") for x in selected}

    all_jobs = load_json(PUBLIC_JOBS_PATH, [])
    for region, predicate in regions.items():
        if any(predicate(x) for x in selected):
            continue

        candidates = [x for x in all_jobs if predicate(x)]
        candidates.sort(key=lambda x: int(x.get("score", 0)), reverse=True)

        if candidates and candidates[0].get("url") not in selected_urls:
            selected.append(candidates[0])
            selected_urls.add(candidates[0].get("url"))

    selected.sort(key=lambda x: int(x.get("score", 0)), reverse=True)
    return selected



def is_jobsdb_or_jobstreet_item(item) -> bool:
    text = f"{item.get('source', '')} {item.get('url', '')}".lower()
    return "jobsdb" in text or "jobstreet" in text


def ensure_jobsdb_source_picks(rows: list[dict], all_jobs: list[dict], config: dict, limit: int = 40, min_score: int = 60) -> list[dict]:
    """
    Keep a small JobsDB/JobStreet block in the dashboard even if those roles are
    below the normal source threshold. This prevents cloud runs from wiping out
    local JobsDB history.
    """
    existing_urls = {r.get("url") for r in rows if r.get("url")}
    extra = []

    for job in all_jobs:
        if not is_jobsdb_or_jobstreet_item(job):
            continue

        url = job.get("url")
        if not url or url in existing_urls:
            continue

        score = int(job.get("score", 0) or 0)
        original_score = int(job.get("original_score", score) or 0)
        best_score = max(score, original_score)

        if best_score < min_score:
            continue

        if should_hide_job(job, config) or should_hide_by_company_quality(job) or is_hard_excluded(job, config):
            continue

        extra.append(job)

    extra.sort(key=lambda x: int(x.get("score", x.get("original_score", 0)) or 0), reverse=True)

    for job in extra[:limit]:
        rows.append(job)
        existing_urls.add(job.get("url"))

    rows.sort(key=lambda x: int(x.get("score", x.get("original_score", 0)) or 0), reverse=True)
    return rows


def region_bucket(row) -> str:
    text = " ".join([
        str(row.get("location", "") or ""),
        str(row.get("source", "") or ""),
        str(row.get("url", "") or ""),
    ]).lower()

    if "hong kong" in text or "hong kong sar" in text or "hk.jobsdb" in text:
        return "hk"

    if "singapore" in text or "sg.jobstreet" in text:
        return "sg"

    if "korea" in text or "seoul" in text or "japan" in text or "tokyo" in text:
        return "krjp"

    return "other"


def region_label(bucket: str) -> str:
    return {
        "hk": "Hong Kong",
        "sg": "Singapore",
        "krjp": "Korea / Japan",
        "other": "Other",
    }.get(bucket, "Other")



def platform_bucket(row) -> str:
    text = " ".join([
        str(row.get("source", "") or ""),
        str(row.get("url", "") or ""),
        str(row.get("company", "") or ""),
    ]).lower()

    if "linkedin" in text or "linkedin.com" in text:
        return "linkedin"
    if "jobsdb" in text or "jobsdb.com" in text:
        return "jobsdb"
    if "jobstreet" in text or "jobstreet.com" in text:
        return "jobstreet"
    return "official"


def platform_label(bucket: str) -> str:
    return {
        "linkedin": "LinkedIn",
        "jobsdb": "JobsDB",
        "jobstreet": "JobStreet",
        "official": "Official",
    }.get(bucket, "Official")


def dashboard_job_type(row) -> str:
    """Official Portal is a source/platform, not a job type."""
    typ = str(row.get("job_type", "") or "").strip()
    title = str(row.get("title", "") or "").lower()

    if typ.lower() == "official portal":
        if any(x in title for x in ["intern", "internship", "summer", "temporary", "part-time", "contract"]):
            return "Internship / Temporary"
        return "Full-time / Graduate"

    return typ or "Other / Review"


def write_public_dashboard(rows: list[dict]):
    rows = [dict(r, job_type=dashboard_job_type(r)) for r in rows]
    DOCS_PATH.parent.mkdir(parents=True, exist_ok=True)

    sections = {
        "Full-time / Graduate": [],
        "Internship / Temporary": [],
        "Official Portal": [],
        "Other / Review": [],
    }

    for r in rows:
        sections.setdefault(r.get("job_type", "Other / Review"), []).append(r)

    nav = []
    body = []
    idx = 1

    for section, items in sections.items():
        if not items:
            continue

        anchor = section.lower().replace(" / ", "-").replace(" ", "-")
        nav.append(f'<a href="#{anchor}">{esc(section)} ({len(items)})</a>')
        body.append(f'<h2 id="{anchor}">{esc(section)} <span>{len(items)} role(s)</span></h2>')

        for r in items:
            title = esc(r.get("title"))
            company = esc(r.get("company") or "Company not captured")
            location = esc(r.get("location"))
            score = esc(r.get("score"))
            url = esc(r.get("url"))
            src = esc(r.get("source"))
            typ = esc(r.get("job_type"))
            last_seen = esc(str(r.get("last_seen_at", ""))[:16].replace("T", " "))
            region = region_bucket(r)
            region_text = esc(region_label(region))
            platform = platform_bucket(r)
            platform_text = esc(platform_label(platform))

            reasons = r.get("reasons") or []
            if reasons:
                reasons_html = "".join(f"<li>{esc(x)}</li>" for x in reasons[:3])
            else:
                reasons_html = "<li>Open the application link to review role details.</li>"

            body.append(f"""
            <article class="card" data-region="{region}" data-platform="{platform}">
              <div class="meta">
                <span>#{idx}</span>
                <span class="pill score">{score}/100</span>
                <span class="pill">{src}</span>
                <span class="pill">{typ}</span>\n                <span class="pill region-pill">{region_text}</span>
                <span class="pill platform-pill">{platform_text}</span>
              </div>

              <h3>{title}</h3>
              <p class="sub">{company} · {location}</p>

              <div class="why">
                <h4>Why it may fit</h4>
                <ul>{reasons_html}</ul>
              </div>

              <p class="seen">Last seen: {last_seen}</p>
              <a class="button" href="{url}" target="_blank" rel="noopener">Open application link</a>
            </article>
            """)
            idx += 1

    generated = now_text()
    total = len(rows)

    html_doc = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Job Fit Radar Shortlist</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body {{
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: #f6f7fb;
      color: #152033;
    }}
    .wrap {{
      max-width: 1050px;
      margin: 0 auto;
      padding: 36px 20px;
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: 34px;
      letter-spacing: -0.03em;
    }}
    .sub, .seen {{
      color: #667085;
    }}
    .seen {{
      font-size: 14px;
      margin-top: 12px;
    }}
    .nav {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin: 20px 0 30px;
    }}
    .nav a {{
      text-decoration: none;
      color: #172033;
      background: white;
      border: 1px solid #e7eaf0;
      border-radius: 999px;
      padding: 9px 13px;
      font-weight: 700;
    }}
    h2 {{
      margin-top: 34px;
      font-size: 24px;
    }}
    h2 span {{
      color: #7a8497;
      font-size: 16px;
      font-weight: 500;
    }}
    .card {{
      background: white;
      border: 1px solid #e4e8f0;
      border-radius: 18px;
      padding: 22px;
      margin: 14px 0;
      box-shadow: 0 10px 28px rgba(16, 24, 40, 0.07);
    }}
    .meta {{
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      align-items: center;
      color: #667085;
      font-weight: 700;
      font-size: 14px;
    }}
    .pill {{
      background: #eef2ff;
      color: #3446a1;
      border-radius: 999px;
      padding: 5px 10px;
    }}
    .score {{
      background: #dcfce7;
      color: #166534;
    }}
    h3 {{
      font-size: 21px;
      margin: 14px 0 6px;
    }}
    h4 {{
      margin: 18px 0 6px;
      color: #344054;
    }}
    ul {{
      margin-top: 6px;
    }}
    li {{
      margin: 4px 0;
    }}
    .button {{
      display: inline-block;
      margin-top: 10px;
      background: #111827;
      color: white;
      text-decoration: none;
      padding: 11px 14px;
      border-radius: 11px;
      font-weight: 800;
    }}
    .region-filter {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin: 4px 0 8px;
    }}
    .region-filter button {{
      border: 1px solid #d8deea;
      background: white;
      color: #172033;
      border-radius: 999px;
      padding: 9px 13px;
      font-weight: 800;
      cursor: pointer;
    }}
    .region-filter button.active {{
      background: #111827;
      color: white;
      border-color: #111827;
    }}
    .filter-count {{
      margin-top: 4px;
      font-size: 14px;
    }}
    .region-pill {{
      background: #f1f5f9;
      color: #334155;
    }}
    .card.viewed-card {{
      opacity: 0.62;
      filter: grayscale(0.15);
    }}
    .card.viewed-card h3 {{
      color: #64748b;
    }}
    .viewed-badge {{
      background: #e2e8f0;
      color: #475569;
    }}
    a.viewed-link {{
      background: #64748b !important;
      color: white !important;
    }}
    .viewed-note {{
      margin-top: -2px;
      font-size: 13px;
    }}
    .reset-viewed-btn {{
      border: none;
      background: transparent;
      color: #2563eb;
      font-weight: 800;
      cursor: pointer;
      padding: 0;
      margin-left: 6px;
    }}
    .platform-filter {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin: 6px 0 8px;
    }}
    .platform-filter button {{
      border: 1px solid #d8deea;
      background: white;
      color: #172033;
      border-radius: 999px;
      padding: 9px 13px;
      font-weight: 800;
      cursor: pointer;
    }}
    .platform-filter button.active {{
      background: #0f172a;
      color: white;
      border-color: #0f172a;
    }}
    .platform-pill {{
      background: #fff7ed;
      color: #9a3412;
    }}
  </style>
</head>
<body>
  <main class="wrap">
    <h1>Job Fit Radar Shortlist</h1>
    <p class="sub">Generated at {generated}. Showing {total} role(s) from the last {RETENTION_DAYS} days. Public-safe version: no CV or profile details included.</p>
    <nav class="nav">{''.join(nav)}</nav>
    <div class="region-filter" aria-label="Region filter">
      <button class="active" data-region="all" onclick="setRegionFilter('all')">All</button>
      <button data-region="hk" onclick="setRegionFilter('hk')">Hong Kong</button>
      <button data-region="sg" onclick="setRegionFilter('sg')">Singapore</button>
      <button data-region="krjp" onclick="setRegionFilter('krjp')">Korea / Japan</button>
      <button data-region="other" onclick="setRegionFilter('other')">Other locations</button>
    </div>
    <div class="platform-filter" aria-label="Platform filter">
      <button class="active" data-platform="all" onclick="setPlatformFilter('all')">All platforms</button>
      <button data-platform="linkedin" onclick="setPlatformFilter('linkedin')">LinkedIn</button>
      <button data-platform="jobsdb" onclick="setPlatformFilter('jobsdb')">JobsDB</button>
      <button data-platform="jobstreet" onclick="setPlatformFilter('jobstreet')">JobStreet</button>
      <button data-platform="official" onclick="setPlatformFilter('official')">Official</button>
    </div>
    <p class="sub filter-count">Visible after filter: <span id="visible-count">{total}</span> role(s).</p>\n    <p class="sub viewed-note">Viewed status is saved in this browser only. <button class="reset-viewed-btn" onclick="resetViewedJobs()">Reset viewed</button></p>\n    <p class="sub filter-empty" id="filter-empty" style="display:none;">No roles in this region filter.</p>
    {''.join(body)}
  </main>
  <script>
    let currentRegionFilter = 'all';
    let currentPlatformFilter = 'all';

    function setRegionFilter(region) {{
      currentRegionFilter = region;
      applyDashboardFilters();
    }}

    function setPlatformFilter(platform) {{
      currentPlatformFilter = platform;
      applyDashboardFilters();
    }}

    function applyDashboardFilters() {{
      let visible = 0;

      document.querySelectorAll('.card').forEach(card => {{
        const regionOk = currentRegionFilter === 'all' || card.dataset.region === currentRegionFilter;
        const platformOk = currentPlatformFilter === 'all' || card.dataset.platform === currentPlatformFilter;
        const show = regionOk && platformOk;

        card.style.display = show ? '' : 'none';
        if (show) visible += 1;
      }});

      document.querySelectorAll('.region-filter button').forEach(btn => {{
        btn.classList.toggle('active', btn.dataset.region === currentRegionFilter);
      }});

      document.querySelectorAll('.platform-filter button').forEach(btn => {{
        btn.classList.toggle('active', btn.dataset.platform === currentPlatformFilter);
      }});

      const count = document.getElementById('visible-count');
      if (count) count.textContent = visible;

      const empty = document.getElementById('filter-empty');
      if (empty) empty.style.display = visible === 0 ? '' : 'none';

      updateSectionCounts();
    }}

    function filterRegion(region) {{
      setRegionFilter(region);
    }}

    function updateSectionCounts() {{
      const headings = Array.from(document.querySelectorAll('main h2'));

      headings.forEach(h2 => {{
        let node = h2.nextElementSibling;
        const cards = [];

        while (node && node.tagName !== 'H2') {{
          if (node.classList && node.classList.contains('card')) {{
            cards.push(node);
          }}
          if (node.querySelectorAll) {{
            cards.push(...Array.from(node.querySelectorAll('.card')));
          }}
          node = node.nextElementSibling;
        }}

        if (cards.length === 0) return;

        const visibleCards = cards.filter(card => card.style.display !== 'none').length;
        h2.style.display = visibleCards > 0 ? '' : 'none';

        const countSpan = h2.querySelector('span');
        if (countSpan) {{
          countSpan.textContent = `${{visibleCards}} role(s)`;
        }}
      }});
    }}

    document.addEventListener('DOMContentLoaded', () => {{
      applyDashboardFilters();
    }});
</script>
  <script id="viewed-job-store">
    (function() {{
      const KEY = "jobFitRadarViewedUrls";

      function loadViewed() {{
        try {{
          return new Set(JSON.parse(localStorage.getItem(KEY) || "[]"));
        }} catch (e) {{
          return new Set();
        }}
      }}

      function saveViewed(viewed) {{
        localStorage.setItem(KEY, JSON.stringify(Array.from(viewed)));
      }}

      function markCard(link) {{
        const card = link.closest(".card");
        if (!card) return;

        card.classList.add("viewed-card");
        link.classList.add("viewed-link");
        link.textContent = "Viewed · Open link";

        if (!card.querySelector(".viewed-badge")) {{
          const badge = document.createElement("span");
          badge.className = "pill viewed-badge";
          badge.textContent = "Viewed";

          const firstPill = card.querySelector(".pill");
          if (firstPill && firstPill.parentElement) {{
            firstPill.parentElement.appendChild(badge);
          }} else {{
            card.prepend(badge);
          }}
        }}
      }}

      function applyViewedState() {{
        const viewed = loadViewed();

        document.querySelectorAll("a").forEach(link => {{
          const label = (link.textContent || "").trim().toLowerCase();
          if (!label.includes("open application link") && !label.includes("viewed · open link")) return;

          const url = link.href;
          if (!url) return;

          if (viewed.has(url)) {{
            markCard(link);
          }}

          link.addEventListener("click", () => {{
            const current = loadViewed();
            current.add(url);
            saveViewed(current);
            markCard(link);
          }}, {{ capture: true }});
        }});
      }}

      window.resetViewedJobs = function() {{
        localStorage.removeItem(KEY);
        document.querySelectorAll(".viewed-card").forEach(card => card.classList.remove("viewed-card"));
        document.querySelectorAll(".viewed-link").forEach(link => {{
          link.classList.remove("viewed-link");
          link.textContent = "Open application link";
        }});
        document.querySelectorAll(".viewed-badge").forEach(badge => badge.remove());
      }};

      document.addEventListener("DOMContentLoaded", applyViewedState);
    }})();
  </script>
</body>
</html>
"""
    DOCS_PATH.write_text(html_doc, encoding="utf-8")


def send_ntfy(rows, scan_label: str, total_high: int):
    if not rows:
        return

    server = os.getenv("NTFY_SERVER", "https://ntfy.sh").rstrip("/")
    topic = os.getenv("NTFY_TOPIC", "").strip()
    dashboard_url = os.getenv("DASHBOARD_URL", "").strip()

    if not topic:
        print("[WARN] NTFY_TOPIC missing; notification skipped.")
        return

    shown = rows[:10]
    lines = [
        f"Job Fit Radar — {scan_label}",
        f"{total_high} new high-match role(s). Showing top {len(shown)}.",
    ]

    if dashboard_url:
        lines.append(f"Dashboard: {dashboard_url}")

    for i, r in enumerate(shown, start=1):
        lines.append("")
        lines.append(f"{i}. [{source_label(r)} | {short_job_type(r)}] {r['title']}")
        lines.append(f"Score: {r['score']}/100")
        lines.append(f"Apply: {r['url']}")

    resp = requests.post(
        f"{server}/{topic}",
        data="\n".join(lines).encode("utf-8"),
        headers={
            "Title": "Job Fit Radar",
            "Priority": "default",
            "Tags": "briefcase",
        },
        timeout=30,
    )
    resp.raise_for_status()
    print(f"Notification sent: {len(shown)} role(s).")


def run_one_scan(label: str, source_file: str, state: dict):
    print(f"[{now_text()}] Running {label} scan with {source_file}")

    if not Path(source_file).exists():
        print(f"[WARN] Missing {source_file}; skipped.")
        return False

    shutil.copyfile(source_file, "sources.yaml")

    db_path = Path(os.getenv("DATABASE_PATH", "jobs.db"))
    if db_path.exists():
        db_path.unlink()

    scan_once(send=False)

    min_score = threshold()
    raw_rows = get_rows(min_score=min_score, limit=300)
    current_rows = [r for r in raw_rows if int(r['score'] or 0) >= threshold_for_item(r)]

    merged_jobs = merge_recent_jobs(current_rows)

    profile_text = Path("profile.md").read_text(encoding="utf-8") if Path("profile.md").exists() else ""
    config = load_config()

    # Company quality rules run before Gemini.
    merged_jobs = apply_company_quality(merged_jobs, config)

    # Enrich high-score candidates by opening detail pages.
    # This is needed because LinkedIn search cards often do not include requirements like "5-8 years".
    merged_jobs = enrich_job_descriptions(merged_jobs, config)

    # Re-apply company quality after description enrichment.
    merged_jobs = apply_company_quality(merged_jobs, config)

    # Gemini is only used as a small secondary screening layer.
    merged_jobs = screen_jobs_with_gemini(merged_jobs, profile_text, config)

    save_json(PUBLIC_JOBS_PATH, merged_jobs)
    dashboard_rows = [x for x in merged_jobs if int(x.get("score", 0)) >= threshold_for_item(x) and not should_hide_job(x, load_config()) and not should_hide_by_company_quality(x) and not is_hard_excluded(x, load_config())]
    dashboard_rows = add_region_representatives(dashboard_rows)
    dashboard_rows = ensure_jobsdb_source_picks(dashboard_rows, merged_jobs, config, limit=40, min_score=60)

    write_public_dashboard(dashboard_rows)

    sent_urls = set(state.get("sent_urls", []))
    new_high = [r for r in current_rows if r["url"] and r["url"] not in sent_urls]

    screened_current_urls = {x.get("url") for x in merged_jobs if not should_hide_job(x, load_config()) and not should_hide_by_company_quality(x) and not is_hard_excluded(x, load_config())}
    new_high = [r for r in new_high if r["url"] in screened_current_urls]

    if new_high:
        send_ntfy(new_high, scan_label=label, total_high=len(new_high))
        for r in new_high:
            sent_urls.add(r["url"])

    state["sent_urls"] = sorted(sent_urls)
    state[f"last_{label.lower()}_scan"] = now_ts()

    print(f"[{now_text()}] {label} scan done. Current high-match roles: {len(current_rows)}. New high-match roles: {len(new_high)}.")
    return True


def main():
    state = load_state()
    current = now_ts()

    is_manual_run = os.getenv("GITHUB_EVENT_NAME") == "workflow_dispatch"

    due_fast = current - float(state.get("last_fast_scan", 0)) >= FAST_INTERVAL_SECONDS
    due_slow = current - float(state.get("last_slow_scan", 0)) >= SLOW_INTERVAL_SECONDS

    # When manually clicking "Run workflow", force a FAST scan so the dashboard
    # is populated immediately. Scheduled runs still use the 12h / 36h logic.
    if is_manual_run:
        print("Manual workflow run detected. Forcing FAST scan.")
        due_fast = True

    ran_any = False

    if due_slow:
        ran_any = run_one_scan("SLOW", "sources_slow.yaml", state) or ran_any

    if due_fast:
        ran_any = run_one_scan("FAST", "sources_fast.yaml", state) or ran_any

    if not ran_any:
        print("No scan due.")

        if not PUBLIC_JOBS_PATH.exists():
            print("cloud_jobs.json does not exist yet. Keeping existing dashboard unchanged.")
            save_state(state)
            return

        print("Refreshing dashboard from existing cloud_jobs.json.")
        jobs = load_json(PUBLIC_JOBS_PATH, [])
        min_score = threshold()
        rows = [x for x in jobs if int(x.get("score", 0)) >= threshold_for_item(x) and not should_hide_job(x, load_config()) and not should_hide_by_company_quality(x) and not is_hard_excluded(x, load_config())]
        rows = add_region_representatives(rows)

        # Do not overwrite a non-empty dashboard with 0 roles unless this is truly intentional.
        if not rows and DOCS_PATH.exists():
            print("No rows available from cloud_jobs.json. Keeping existing dashboard unchanged.")
            save_state(state)
            return

        write_public_dashboard(rows)

    save_state(state)


if __name__ == "__main__":
    main()
