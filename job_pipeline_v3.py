"""
=============================================================
ASHVIN'S JOB PIPELINE v5 — FIXED FILTERS + TELEGRAM
=============================================================
pip install apify-client requests apscheduler
=============================================================
"""

import os
import re
import hashlib
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apify_client import ApifyClient
import requests

APIFY_API_TOKEN    = os.environ.get("APIFY_API_TOKEN", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")

ET                       = ZoneInfo("America/New_York")
FRESHNESS_WINDOW_MINUTES = 120
SEEN_JOB_HASHES: set     = set()

LINKEDIN_ACTOR = "curious_coder/linkedin-jobs-scraper"
INDEED_ACTOR   = "misceres/indeed-scraper"

# ─────────────────────────────────────────────
# SEARCH URLS
# ─────────────────────────────────────────────

def build_linkedin_urls():
    roles = [
        '%22Data+Scientist%22',
        '%22AI+Engineer%22',
        '%22Machine+Learning+Engineer%22',
        '%22Senior+Data+Analyst%22',
        '%22Analytics+Engineer%22',
        '%22Applied+Scientist%22',
        '%22Quantitative+Analyst%22',
    ]
    urls = []
    for role in roles:
        # f_TPR=r86400=last 24h, f_E=1,2=entry+associate, f_WT=2,3=remote+hybrid
        urls.append(
            f"https://www.linkedin.com/jobs/search/?keywords={role}"
            f"&location=United+States&f_TPR=r86400&f_E=1%2C2&f_WT=2%2C3"
        )
        urls.append(
            f"https://www.linkedin.com/jobs/search/?keywords={role}"
            f"&location=United+States&f_TPR=r86400&f_E=1%2C2"
        )
    return urls


def build_indeed_urls():
    searches = [
        '%22data+scientist%22',
        '%22AI+engineer%22',
        '%22machine+learning+engineer%22',
        '%22senior+data+analyst%22',
        '%22analytics+engineer%22',
        '%22applied+scientist%22',
        '%22quantitative+analyst%22',
        '%22trade+operations+analyst%22',
        '%22data+scientist%22+entry+level',
        '%22junior+data+scientist%22',
    ]
    urls = []
    for q in searches:
        urls.append(
            f"https://www.indeed.com/jobs?q={q}"
            f"&l=United+States&fromage=1&sort=date"
        )
    return urls


# ─────────────────────────────────────────────
# FIELD NORMALIZER
# Maps different actor field names → consistent dict
# ─────────────────────────────────────────────

def normalize(job):
    return {
        "title":       (job.get("title")
                        or job.get("positionName")
                        or job.get("jobTitle")
                        or job.get("name")
                        or "Unknown Role"),
        "company":     (job.get("companyName")
                        or job.get("company")
                        or job.get("employer")
                        or "Unknown Company"),
        "location":    (job.get("location")
                        or job.get("jobLocation")
                        or "Unknown Location"),
        "salary":      (job.get("salary")
                        or job.get("salaryMin")
                        or job.get("compensation")
                        or job.get("salaryRange")
                        or "Not listed"),
        "url":         (job.get("jobUrl")
                        or job.get("url")
                        or job.get("externalUrl")
                        or job.get("applyUrl")
                        or "#"),
        "description": (job.get("description")
                        or job.get("jobDescription")
                        or job.get("summary")
                        or ""),
        "posted_at":   (job.get("postedAt")
                        or job.get("datePosted")
                        or job.get("publishedAt")
                        or job.get("date")
                        or ""),
        "_source":     job.get("_source", ""),
    }


# ─────────────────────────────────────────────
# SCRAPERS
# ─────────────────────────────────────────────

def scrape_linkedin():
    print("  [LinkedIn] Scraping...")
    try:
        client = ApifyClient(APIFY_API_TOKEN)
        run    = client.actor(LINKEDIN_ACTOR).call(run_input={
            "urls":  build_linkedin_urls()[:8],
            "count": 10,
        })
        items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        for j in items: j["_source"] = "LinkedIn"
        print(f"  [LinkedIn] {len(items)} raw")
        return items
    except Exception as e:
        print(f"  [LinkedIn ERROR] {e}")
        return []


def scrape_indeed():
    print("  [Indeed] Scraping...")
    try:
        client = ApifyClient(APIFY_API_TOKEN)
        run    = client.actor(INDEED_ACTOR).call(run_input={
            "startUrls": [{"url": u} for u in build_indeed_urls()],
            "maxItems":  100,
        })
        items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        for j in items: j["_source"] = "Indeed"
        print(f"  [Indeed] {len(items)} raw")
        return items
    except Exception as e:
        print(f"  [Indeed ERROR] {e}")
        return []


# ─────────────────────────────────────────────
# FILTERS
# ─────────────────────────────────────────────

DISQUALIFY_PHRASES = [
    "sponsorship not available", "we do not offer sponsorship",
    "no visa sponsorship", "unable to sponsor", "will not sponsor",
    "cannot sponsor", "not able to sponsor",
    "must be a u.s. citizen", "must be a us citizen",
    "u.s. citizenship required", "us citizenship required",
    "active security clearance", "security clearance required",
    "secret clearance", "top secret",
    "green card required", "gc required",
    "work authorization will not", "no h-1b", "no h1b",
]

# Titles that are clearly irrelevant
BAD_TITLE_WORDS = [
    "cashier", "sales representative", "busser", "waiter", "driver",
    "warehouse", "nurse", "teacher", "mechanic", "electrician",
    "customer service", "call center", "receptionist",
    "firmware engineer", "hardware engineer", "production manager",
    "marketing coordinator", "hr coordinator", "construction",
    "t-mobile", "now hiring",
]

SALARY_KEYWORDS = [
    "$120k","$125k","$130k","$140k","$150k","$160k","$170k","$180k","$190k","$200k",
    "120,000","125,000","130,000","140,000","150,000","160,000","170,000","180,000",
]


def job_hash(job):
    key = job["title"].lower().strip() + job["company"].lower().strip()
    return hashlib.md5(key.encode()).hexdigest()


def is_fresh(job):
    raw = job.get("posted_at", "")
    if not raw:
        return True
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=FRESHNESS_WINDOW_MINUTES)
        posted = (
            datetime.fromtimestamp(float(raw), tz=timezone.utc)
            if isinstance(raw, (int, float))
            else datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        )
        return posted >= cutoff
    except Exception:
        return True


def is_bad_title(job):
    title = job["title"].lower()
    return any(kw in title for kw in BAD_TITLE_WORDS)


def is_disqualified(job):
    desc = job["description"].lower()
    return any(p in desc for p in DISQUALIFY_PHRASES)


def has_target_salary(job):
    txt = job["salary"].lower()
    if not txt or txt == "not listed":
        return True
    if any(kw.lower() in txt for kw in SALARY_KEYWORDS):
        return True
    for n in re.findall(r'\$?([\d,]+)', txt):
        try:
            v = int(n.replace(",", ""))
            if v >= 120000 or 120 <= v <= 999:
                return True
        except ValueError:
            pass
    return False


def score_job(job):
    score = 0
    text  = (job["title"] + " " + job["description"]).lower()

    for skill, pts in {
        "python": 15, "sql": 10, "machine learning": 12,
        "power bi": 8, "data science": 10, "azure": 8,
        "llm": 12, "rag": 10, "databricks": 8, "tensorflow": 8,
        "tableau": 6, "pandas": 5, "statistical": 8,
        "etl": 6, "forecasting": 7, "nlp": 8, "deep learning": 8,
        "scikit": 7, "spark": 7, "snowflake": 6,
    }.items():
        if skill in text: score += pts

    # Strong title match bonus
    for t in ["data scientist", "ai engineer", "ml engineer",
              "machine learning", "analytics engineer",
              "applied scientist", "quantitative"]:
        if t in job["title"].lower(): score += 15

    for p in ["1-3 years","2+ years","entry level","junior",
              "0-2 years","new grad","associate","early career"]:
        if p in text: score += 10

    for p in ["3-5 years","3+ years","senior"]:
        if p in text: score += 3

    for p in ["10+ years","8+ years","7+ years","director",
              "principal","vp of","head of","staff engineer"]:
        if p in text: score -= 20

    for p in ["quant","trading","risk","portfolio",
              "hedge fund","bloomberg","sharpe","derivatives"]:
        if p in text: score += 8

    return min(100, max(0, score))


def filter_jobs(raw_jobs):
    normalized = [normalize(j) for j in raw_jobs]
    s_seen = s_title = s_fresh = s_disq = s_salary = 0
    passed = []

    for job in normalized:
        h = job_hash(job)
        if h in SEEN_JOB_HASHES:       s_seen  += 1; continue
        if is_bad_title(job):           s_title += 1; continue
        if not is_fresh(job):           s_fresh += 1; continue
        if is_disqualified(job):        s_disq  += 1; continue
        if not has_target_salary(job):  s_salary+= 1; continue
        job["score"] = score_job(job)
        SEEN_JOB_HASHES.add(h)
        passed.append(job)

    print(
        f"  Filters: {len(normalized)} in | "
        f"dupe:{s_seen} bad_title:{s_title} stale:{s_fresh} "
        f"disq:{s_disq} salary:{s_salary} | {len(passed)} passed"
    )

    # Sort by score, then dedupe by title+company
    passed.sort(key=lambda j: j["score"], reverse=True)
    seen_keys, deduped = set(), []
    for job in passed:
        key = job["title"].lower()[:30] + job["company"].lower()[:20]
        if key not in seen_keys:
            seen_keys.add(key)
            deduped.append(job)

    print(f"  After dedup: {len(deduped)} unique jobs")
    return deduped


# ─────────────────────────────────────────────
# TELEGRAM — ALL JOBS SHOWN, NO "CHECK APIFY"
# ─────────────────────────────────────────────

def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("  [Telegram] Tokens missing"); return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text,
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=15,
        )
        if not r.ok:
            print(f"  [Telegram ERROR] {r.status_code}: {r.text[:200]}")
            return False
        return True
    except Exception as e:
        print(f"  [Telegram ERROR] {e}"); return False


def score_label(score):
    if score >= 70: return "🟢 HIGH"
    if score >= 45: return "🟡 MED"
    return "🔵 LOW"


def send_all_jobs(jobs, run_label):
    if not jobs:
        send_telegram(
            f"✅ <b>Pipeline ran — {run_label}</b>\n"
            f"🕐 {datetime.now(ET).strftime('%a %b %d, %I:%M %p ET')}\n"
            f"No new matching jobs this run."
        )
        return

    # Header
    send_telegram(
        f"🚨 <b>{len(jobs)} New Job(s) — {run_label}</b>\n"
        f"🕐 {datetime.now(ET).strftime('%a %b %d, %I:%M %p ET')}\n"
        f"All {len(jobs)} match(es) below 👇"
    )

    # Send in batches of 5 (mobile-friendly)
    for i in range(0, len(jobs), 5):
        batch = jobs[i:i+5]
        lines = []
        for j, job in enumerate(batch, start=i+1):
            sal = job["salary"] if job["salary"] != "Not listed" else "Salary not listed"
            lines.append(
                f"{j}. {score_label(job['score'])} — <b>{job['title']}</b>\n"
                f"   🏢 {job['company']}  📍 {job['location']}\n"
                f"   💰 {sal}  |  {job['_source']}\n"
                f"   📊 Match: {job['score']}/100\n"
                f"   🔗 <a href='{job['url']}'>Apply Now →</a>\n"
            )
        send_telegram("\n".join(lines))

    # Top 3 summary at the end
    top3  = jobs[:3]
    lines = ["⭐ <b>Top picks this run:</b>"]
    for job in top3:
        lines.append(f"• <a href='{job['url']}'>{job['title']} @ {job['company']}</a>")
    lines.append("\n🎯 Apply within the hour for best results!")
    send_telegram("\n".join(lines))


# ─────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────

def run_pipeline():
    now_et  = datetime.now(ET)
    weekday = now_et.weekday()
    hour    = now_et.hour

    if weekday > 3 or hour < 7 or hour > 21:
        print(f"[{now_et.strftime('%a %I:%M %p ET')}] Outside window."); return

    day_names = ["Monday","Tuesday","Wednesday","Thursday"]
    run_label = f"{day_names[weekday]} {now_et.strftime('%I:%M %p ET')}"
    print(f"\n{'='*52}\n  Run: {run_label}\n{'='*52}")

    all_jobs = scrape_linkedin() + scrape_indeed()
    print(f"  Total raw: {len(all_jobs)}")

    filtered = filter_jobs(all_jobs)
    send_all_jobs(filtered, run_label)
    print(f"  Done — sent {len(filtered)} jobs\n")


# ─────────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────────

def run_startup_checks():
    print("\n--- Startup checks ---")
    missing = [k for k, v in {
        "APIFY_API_TOKEN":    APIFY_API_TOKEN,
        "TELEGRAM_BOT_TOKEN": TELEGRAM_BOT_TOKEN,
        "TELEGRAM_CHAT_ID":   TELEGRAM_CHAT_ID,
    }.items() if not v]

    if missing:
        print(f"  Missing: {', '.join(missing)}"); return False
    print("  All env vars set")

    ok = send_telegram(
        "✅ <b>Job Pipeline v5 is live!</b>\n\n"
        "Watching: Data Scientist · AI Engineer · ML Engineer\n"
        "Senior Analyst · Analytics Engineer · Quant · Trade Ops\n\n"
        "Mon/Wed/Thu: every 3h | Tuesday: every 2h\n"
        "Filters: $120K+ · Sponsorship-friendly · Fresh jobs only\n"
        "All matching jobs shown here — no more check Apify messages!"
    )
    if not ok:
        print("  Telegram failed — check tokens"); return False
    print("  Telegram working!")
    print("--- Checks passed ---\n")
    return True


if __name__ == "__main__":
    print("="*52)
    print("  Ashvin Job Pipeline v5")
    print(f"  {datetime.now(ET).strftime('%A %B %d, %I:%M %p ET')}")
    print("="*52)

    if not run_startup_checks():
        exit(1)

    scheduler = BlockingScheduler(timezone=ET)
    scheduler.add_job(run_pipeline, CronTrigger(
        day_of_week="mon,wed,thu", hour="7,10,13,16,19,21",
        minute=0, timezone=ET), id="mwt")
    scheduler.add_job(run_pipeline, CronTrigger(
        day_of_week="tue", hour="7,9,11,13,15,17,19,21",
        minute=0, timezone=ET), id="tue")

    print("  Mon/Wed/Thu → 7,10am,1,4,7,9pm ET")
    print("  Tuesday     → 7,9,11am,1,3,5,7,9pm ET")
    print("  Fri-Sun     → OFF\n")

    now = datetime.now(ET)
    if now.weekday() <= 3 and 7 <= now.hour <= 21:
        print("  Running initial scan...\n")
        run_pipeline()

    try:
        scheduler.start()
    except KeyboardInterrupt:
        print("  Stopped.")
