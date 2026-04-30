from __future__ import annotations

import argparse
import os
import time
from pathlib import Path

import yaml
from dotenv import load_dotenv

from jobfit.db import connect, get_unsent_high_score, high_score_jobs, dashboard_jobs_with_region_picks, init_db, mark_sent, recent_jobs, upsert_job
from jobfit.scoring import load_profile, score_job, is_excluded_job
from jobfit.sources import fetch_all_jobs, load_sources
from jobfit.telegram import format_jobs_message, get_chat_id_updates
from jobfit.notify import send_notification
from jobfit.report import write_html_report


def load_config(path: str = "config.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def scan_once(send: bool = True) -> None:
    load_dotenv()
    config = load_config()
    db_path = os.getenv("DATABASE_PATH", "jobs.db")
    profile = load_profile()
    sources = load_sources()

    con = connect(db_path)
    init_db(con)

    if not sources:
        print("No enabled sources. Edit sources.yaml and set enabled: true for at least one source.")
        return

    max_jobs = int(config.get("run", {}).get("max_jobs_per_source", 80))
    threshold = int(config.get("run", {}).get("score_threshold", 80))
    print(f"Fetching jobs from {len(sources)} source(s)...")
    jobs = fetch_all_jobs(sources, max_jobs_per_source=max_jobs)
    print(f"Fetched {len(jobs)} job(s). Scoring and saving...")

    new_count = 0
    for job in jobs:
        if not job.title and not job.url:
            continue
        if is_excluded_job(job, config):
            continue
        result = score_job(job, profile, config)
        created = upsert_job(con, job, result)
        if created:
            new_count += 1
            print(f"NEW [{result.score}] {job.company} - {job.title} ({job.location})")

    print(f"Scan complete. New jobs: {new_count}")

    report_limit = int(config.get("run", {}).get("report_limit", 200))
    dashboard_cfg = config.get("dashboard", {})
    min_region_score = int(dashboard_cfg.get("min_region_score", 50))
    all_high = dashboard_jobs_with_region_picks(con, threshold=threshold, limit=report_limit, min_region_score=min_region_score)
    report_path = write_html_report(all_high, path="outputs/latest_shortlist.html")
    print(f"Dashboard updated: {report_path}")

    notify_limit = int(config.get("run", {}).get("notification_candidate_limit", 30))
    rows = get_unsent_high_score(con, threshold=threshold, limit=notify_limit)
    if send:
        send_empty = bool(config.get("run", {}).get("send_empty_updates", False))
        if rows or send_empty:
            telegram_cfg = config.get("telegram", {})
            text = format_jobs_message(
                rows,
                title=telegram_cfg.get("message_title", "Job Fit Radar"),
                max_roles=int(telegram_cfg.get("max_roles_per_message", 3)),
                dashboard_path=str(report_path),
                total_high=len(rows),
            )
            send_notification(text)
            if rows:
                mark_sent(con, [row["id"] for row in rows])
            print(f"Notification sent. Roles included: {len(rows)}")
        else:
            print("No high-score unsent jobs. Notification skipped.")


def run_forever() -> None:
    load_dotenv()
    config = load_config()
    interval_hours = float(config.get("run", {}).get("interval_hours", 2))
    interval_seconds = int(interval_hours * 3600)
    print(f"Job Fit Radar running. Interval: {interval_hours} hour(s). Press Ctrl+C to stop.")
    while True:
        try:
            scan_once(send=True)
        except Exception as e:
            print(f"[ERROR] {e}")
        time.sleep(interval_seconds)


def send_test() -> None:
    load_dotenv()
    send_notification("<b>Job Fit Radar</b>\nTest message successful ✅")
    print("Test notification sent.")


def show_chat_id() -> None:
    load_dotenv()
    updates = get_chat_id_updates()
    print("Raw Telegram getUpdates response:")
    print(updates)
    print("\nLook for result -> message -> chat -> id. Put that value into TELEGRAM_CHAT_ID in .env.")


def show_recent(limit: int = 10) -> None:
    load_dotenv()
    con = connect(os.getenv("DATABASE_PATH", "jobs.db"))
    init_db(con)
    rows = recent_jobs(con, limit=limit)
    for row in rows:
        print(f"[{row['score']}] {row['title']} | {row['url']}")


def build_dashboard(open_file: bool = False) -> None:
    load_dotenv()
    config = load_config()
    threshold = int(config.get("run", {}).get("score_threshold", 85))
    report_limit = int(config.get("run", {}).get("report_limit", 200))
    con = connect(os.getenv("DATABASE_PATH", "jobs.db"))
    init_db(con)
    dashboard_cfg = config.get("dashboard", {})
    min_region_score = int(dashboard_cfg.get("min_region_score", 50))
    rows = dashboard_jobs_with_region_picks(con, threshold=threshold, limit=report_limit, min_region_score=min_region_score)
    path = write_html_report(rows, path="outputs/latest_shortlist.html")
    print(f"Dashboard written: {path}")
    if open_file:
        import webbrowser
        webbrowser.open(Path(path).resolve().as_uri())


def main() -> None:
    parser = argparse.ArgumentParser(description="Job Fit Radar: monitor jobs, score CV fit, send alerts.")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("init-db")
    sub.add_parser("scan-once")
    sub.add_parser("run-forever")
    sub.add_parser("send-test")
    sub.add_parser("get-chat-id")
    sub.add_parser("dashboard")
    recent_parser = sub.add_parser("recent")
    recent_parser.add_argument("--limit", type=int, default=10)
    args = parser.parse_args()

    if args.command == "init-db":
        load_dotenv()
        con = connect(os.getenv("DATABASE_PATH", "jobs.db"))
        init_db(con)
        print("Database initialized.")
    elif args.command == "scan-once":
        scan_once(send=True)
    elif args.command == "run-forever":
        run_forever()
    elif args.command == "send-test":
        send_test()
    elif args.command == "get-chat-id":
        show_chat_id()
    elif args.command == "dashboard":
        build_dashboard(open_file=True)
    elif args.command == "recent":
        show_recent(limit=args.limit)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
