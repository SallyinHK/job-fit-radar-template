from __future__ import annotations

import asyncio
import re
from pathlib import Path
from urllib.parse import urljoin

import yaml
from playwright.async_api import async_playwright


OUT_DIR = Path("debug_pages")
OUT_DIR.mkdir(exist_ok=True)


USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def slugify(text: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9]+", "-", text.lower()).strip("-")
    return text[:80] or "source"


def load_jobsdb_sources():
    with open("sources.yaml", "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    sources = []
    for s in data.get("sources", []):
        name = s.get("name", "")
        url = s.get("url", "")
        if "JobsDB HK" in name or "JobStreet SG" in name or "jobsdb.com" in url or "jobstreet.com" in url:
            sources.append(s)

    return sources


async def diagnose_source(browser, source):
    name = source.get("name", "unknown")
    url = source.get("url", "")

    print(f"\n[DEBUG] Checking {name}")
    print(f"[DEBUG] URL: {url}")

    context = await browser.new_context(
        user_agent=USER_AGENT,
        locale="en-HK",
        timezone_id="Asia/Hong_Kong",
        viewport={"width": 1440, "height": 1200},
        extra_http_headers={
            "Accept-Language": "en-HK,en;q=0.9,zh-HK;q=0.8,zh;q=0.7",
        },
    )

    page = await context.new_page()

    status = "NO_RESPONSE"
    final_url = url
    title = ""
    body_text = ""
    job_links = []
    possible_block = False
    error = ""

    try:
        response = await page.goto(url, wait_until="domcontentloaded", timeout=45000)
        if response:
            status = str(response.status)

        await page.wait_for_timeout(8000)

        # Scroll a little to trigger lazy-loaded job cards.
        for _ in range(3):
            await page.mouse.wheel(0, 1800)
            await page.wait_for_timeout(2000)

        final_url = page.url
        title = await page.title()

        body_text = await page.locator("body").inner_text(timeout=10000)
        lower_body = body_text.lower()

        block_words = [
            "access denied",
            "captcha",
            "robot",
            "unusual traffic",
            "verify you are human",
            "forbidden",
            "blocked",
            "enable cookies",
            "security check",
        ]
        possible_block = any(w in lower_body for w in block_words)

        anchors = await page.locator("a").evaluate_all(
            """els => els.map(a => ({
                href: a.href || "",
                text: (a.innerText || "").trim().slice(0, 180)
            }))"""
        )

        for a in anchors:
            href = a.get("href", "")
            text = a.get("text", "")
            if "/job/" in href or "/jobs/" in href:
                job_links.append({
                    "href": urljoin(final_url, href),
                    "text": text,
                })

    except Exception as e:
        error = repr(e)

    safe_name = slugify(name)
    out_path = OUT_DIR / f"{safe_name}.txt"

    lines = [
        f"Source: {name}",
        f"Original URL: {url}",
        f"Final URL: {final_url}",
        f"HTTP status: {status}",
        f"Title: {title}",
        f"Possible block page: {possible_block}",
        f"Error: {error}",
        "",
        f"Job-like links found: {len(job_links)}",
        "",
        "First 30 job-like links:",
    ]

    for i, link in enumerate(job_links[:30], start=1):
        lines.append(f"{i}. {link['text']}")
        lines.append(f"   {link['href']}")

    lines.extend([
        "",
        "Body text sample, first 5000 chars:",
        "-" * 80,
        body_text[:5000],
    ])

    out_path.write_text("\n".join(lines), encoding="utf-8")

    print(f"[DEBUG] status={status}, links={len(job_links)}, possible_block={possible_block}, saved={out_path}")

    await context.close()


async def main():
    sources = load_jobsdb_sources()

    if not sources:
        print("[DEBUG] No JobsDB / JobStreet sources found in sources.yaml")
        return

    print(f"[DEBUG] Found {len(sources)} JobsDB / JobStreet sources")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        for source in sources:
            await diagnose_source(browser, source)

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
