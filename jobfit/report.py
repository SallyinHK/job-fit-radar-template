from __future__ import annotations

import html
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from jobfit.classify import classify_job_type, source_label


def _get(row, key, default=""):
    try:
        if hasattr(row, "keys") and key in row.keys():
            return row[key]
        if isinstance(row, dict):
            return row.get(key, default)
    except Exception:
        pass
    return getattr(row, key, default)


def _loads(value):
    try:
        if not value:
            return []
        if isinstance(value, list):
            return value
        return json.loads(value)
    except Exception:
        return []


def _esc(value):
    return html.escape(str(value or ""))


def _card(row, idx):
    title = _esc(_get(row, "title"))
    company = _esc(_get(row, "company"))
    location = _esc(_get(row, "location"))
    url = _esc(_get(row, "url"))
    score = _esc(_get(row, "score"))
    recommendation = _esc(_get(row, "recommendation", "Review"))
    priority = _esc(_get(row, "priority", ""))
    summary = _esc(_get(row, "summary", ""))

    source = _esc(source_label(row))
    job_type = _esc(classify_job_type(row))

    reasons = _loads(_get(row, "reasons_json"))
    risks = _loads(_get(row, "risks_json"))
    keywords = _loads(_get(row, "resume_keywords_json"))

    reasons_html = "".join(f"<li>{_esc(x)}</li>" for x in reasons[:3])
    risks_html = "".join(f"<li>{_esc(x)}</li>" for x in risks[:3])
    keywords_text = ", ".join(str(x) for x in keywords[:8])

    return f"""
    <article class="card">
      <div class="meta">
        <span>#{idx}</span>
        <span class="pill score">{score}/100</span>
        <span class="pill">{recommendation}</span>
        <span class="pill">{priority}</span>
        <span class="pill source">{source}</span>
        <span class="pill type">{job_type}</span>
      </div>

      <h2>{title}</h2>
      <p class="sub">{company} · {location}</p>
      <p>{summary}</p>

      <div class="grid">
        <div>
          <h3>Why it may fit</h3>
          <ul>{reasons_html or "<li>Open link to review details.</li>"}</ul>
        </div>
        <div>
          <h3>Risks to check</h3>
          <ul>{risks_html or "<li>Check visa, language, and seniority requirements.</li>"}</ul>
        </div>
      </div>

      <p><strong>Resume angle:</strong> {_esc(keywords_text)}</p>
      <a class="button" href="{url}" target="_blank" rel="noopener">Open application link</a>
    </article>
    """


def write_html_report(rows, path="outputs/latest_shortlist.html"):
    Path(path).parent.mkdir(parents=True, exist_ok=True)

    rows = list(rows or [])
    rows = sorted(rows, key=lambda r: int(_get(r, "score", 0) or 0), reverse=True)

    sections = defaultdict(list)
    for row in rows:
        sections[classify_job_type(row)].append(row)

    section_order = [
        "Full-time / Graduate",
        "Internship / Temporary",
        "Official Portal",
        "Other / Review",
    ]

    total = len(rows)
    generated = datetime.now().strftime("%Y-%m-%d %H:%M")

    nav_parts = []
    for name in section_order:
        count = len(sections.get(name, []))
        if count:
            anchor = name.lower().replace(" / ", "-").replace(" ", "-")
            nav_parts.append(f'<a href="#{anchor}">{_esc(name)} ({count})</a>')

    body_parts = []
    running_idx = 1

    for name in section_order:
        group = sections.get(name, [])
        if not group:
            continue

        anchor = name.lower().replace(" / ", "-").replace(" ", "-")
        body_parts.append(f"""
        <section id="{anchor}">
          <h1 class="section-title">{_esc(name)} <span>{len(group)} role(s)</span></h1>
        """)

        for row in group:
            body_parts.append(_card(row, running_idx))
            running_idx += 1

        body_parts.append("</section>")

    html_doc = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Job Fit Radar Shortlist</title>
  <style>
    body {{
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: #f5f7fb;
      color: #172033;
    }}
    .wrap {{
      max-width: 1120px;
      margin: 0 auto;
      padding: 42px 28px;
    }}
    .top h1 {{
      margin: 0;
      font-size: 36px;
      letter-spacing: -0.03em;
    }}
    .top p {{
      color: #65728a;
      font-size: 17px;
    }}
    .nav {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin: 20px 0 28px;
    }}
    .nav a {{
      text-decoration: none;
      background: white;
      color: #243253;
      padding: 10px 14px;
      border-radius: 999px;
      box-shadow: 0 6px 20px rgba(20, 31, 55, 0.08);
      font-weight: 700;
    }}
    .section-title {{
      margin: 32px 0 16px;
      font-size: 26px;
    }}
    .section-title span {{
      color: #7a8498;
      font-size: 17px;
      font-weight: 500;
    }}
    .card {{
      background: white;
      border: 1px solid #e4e9f2;
      border-radius: 20px;
      padding: 26px;
      margin: 16px 0 22px;
      box-shadow: 0 12px 32px rgba(20, 31, 55, 0.08);
    }}
    .meta {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
      color: #6b768d;
      font-weight: 700;
    }}
    .pill {{
      background: #eef2ff;
      color: #3346a3;
      border-radius: 999px;
      padding: 6px 12px;
      font-size: 14px;
    }}
    .score {{
      background: #ddf8ea;
      color: #0e7a43;
    }}
    .source {{
      background: #fff3d8;
      color: #91610e;
    }}
    .type {{
      background: #eaf7ff;
      color: #1b6c92;
    }}
    h2 {{
      margin: 18px 0 6px;
      font-size: 24px;
    }}
    .sub {{
      color: #66728a;
      font-size: 17px;
      margin-top: 0;
    }}
    .grid {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 24px;
    }}
    h3 {{
      margin-bottom: 6px;
      color: #4d5a73;
      font-size: 16px;
    }}
    ul {{
      margin-top: 6px;
    }}
    .button {{
      display: inline-block;
      margin-top: 12px;
      background: #101827;
      color: white;
      padding: 12px 16px;
      border-radius: 12px;
      text-decoration: none;
      font-weight: 800;
    }}
    @media (max-width: 800px) {{
      .grid {{
        grid-template-columns: 1fr;
      }}
    }}
  </style>
</head>
<body>
  <main class="wrap">
    <div class="top">
      <h1>Job Fit Radar Shortlist</h1>
      <p>Generated at {generated}. Showing {total} role(s), sorted by score and grouped by role type.</p>
    </div>
    <nav class="nav">
      {''.join(nav_parts)}
    </nav>
    {''.join(body_parts)}
  </main>
</body>
</html>
"""

    Path(path).write_text(html_doc, encoding="utf-8")
    return str(Path(path).resolve())
