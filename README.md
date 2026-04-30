# Job Fit Radar

A customizable job-search dashboard workflow.

It collects job postings from selected sources, scores them against a user profile, filters out poor-fit roles, generates a GitHub Pages dashboard, and optionally sends mobile notifications.

## Features

- Multi-source job collection
  - LinkedIn search pages
  - JobsDB / JobStreet local sync
  - Selected official company career pages

- Rule-based scoring
  - Positive keywords
  - Negative keywords
  - Source-specific thresholds
  - Region preferences

- Hard filters
  - Senior experience requirements, such as 3+ years / 5-8 years
  - Insurance / wealth-management sales roles
  - Traditional backend developer roles
  - ERP / Oracle / SAP implementation mismatch roles
  - Company blacklist

- Dashboard
  - GitHub Pages static HTML
  - Region filter
  - Platform filter
  - Job type grouping
  - Viewed status saved in browser localStorage

- Automation
  - GitHub Actions scheduled scan
  - Fast / slow scan split
  - ntfy mobile push notifications

- Optional AI screening
  - Gemini can be used as a small secondary screening layer
  - Main scoring remains rule-based by default to avoid API quota issues

## Setup

1. Copy `.env.example` to `.env`.

```bash
cp .env.example .env
```

2. Edit `.env` and set your own notification topic, API keys, and provider settings.

3. Copy `profile.example.md` to `profile.md`.

```bash
cp profile.example.md profile.md
```

4. Edit `profile.md` with your own background, target roles, locations, visa constraints, and screening preferences.

5. Install dependencies.

```bash
pip install -r requirements.txt
python -m playwright install chromium
```

6. Run a local test.

```bash
GEMINI_SCREENING_PROVIDER=rules GITHUB_EVENT_NAME=workflow_dispatch python -u cloud_runner.py
```

7. Open the dashboard.

```bash
open docs/index.html
```

## GitHub Pages

Enable GitHub Pages from the `docs/` folder.

Recommended settings:

- Source: Deploy from a branch
- Branch: `main`
- Folder: `/docs`

## GitHub Actions

The workflow in `.github/workflows/job-radar.yml` can run scheduled scans.

Add repository secrets / variables as needed.

Secrets:

- `NTFY_TOPIC`
- `GEMINI_API_KEY` optional

Variables:

- `AI_PROVIDER=rules`
- `GEMINI_SCREENING_PROVIDER=rules`
- `GEMINI_MODEL=gemini-2.5-flash`

## JobsDB / JobStreet note

JobsDB / JobStreet may return 403 on GitHub Actions cloud runners. If this happens, run the local sync instead:

```bash
GEMINI_SCREENING_PROVIDER=rules python -u local_jobsdb_sync.py
```

Then commit and push the refreshed dashboard:

```bash
git add cloud_jobs.json docs/index.html cloud_state.json
git commit -m "Refresh local JobsDB JobStreet results"
git push
```

## Customization

Most personalization happens in:

- `profile.md`
- `config.yaml`
- `sources.yaml`
- `sources_fast.yaml`
- `sources_slow.yaml`
- `sources_jobsdb_local.yaml`

You should customize:

- target locations
- positive keywords
- negative keywords
- company blacklist
- role mismatch filters
- source URLs
- score thresholds
- notification settings

## Important

Do not commit:

- `.env`
- API keys
- personal resume details
- real job-history data
- notification topics
- private profile information

This template is intended as a starting point for a personalized job-search workflow, not a plug-and-play product.
