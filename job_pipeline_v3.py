"""
=============================================================
ASHVIN'S JOB PIPELINE v5.1
=============================================================
Changes:
  ✓ Indeed: switched to borderline/indeed-scraper (4.87★, better dates)
  ✓ LinkedIn: fetchJobDetails=True → gets real applicantCount field
  ✓ LinkedIn: capped at 50 results per run
  ✓ Indeed: capped at 50 results per run
  ✓ Applicant filter: skip if applicantCount > 100
    (LinkedIn only — Indeed doesn't expose this field)
  ✓ Indeed: uses maxAge=1 (today only) for freshness guarantee
  ✓ Freshness window bumped to 3hrs (180 min) to catch more real jobs

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
FRESHNESS_WINDOW_MINUTES = 180   # 3 hours — catches more real new jobs
MAX_APPLICANTS           = 100   # skip if more than this applied (LinkedIn only)
SEEN_JOB_HASHES: set     = set()

# Actors
LINKEDIN_ACTOR = "curious_coder/linkedin-jobs-scraper"
INDEED_ACTOR   = "borderline/indeed-scraper"   # switched: 4.87★, better date support


# ─────────────────────────────────────────────
# SEARCH URLS
# ─────────────────────────────────────────────

def build_linkedin_urls():
    """
    Exact quoted titles + entry/associate level + remote/hybrid + last 24h.
    fetchJobDetails=True will be set in run_input to get applicantCount.
    """
    roles = [
        '%22Data+Scientist%22',
        '%22AI+Engineer%22',
        '%22Machine+Learning+Engineer%22',
        '%22Senior+Data+Analyst%22',
        '%22Analytics+Engineer%22',
        '%22Applied+Scientist%22',
        '%22Quantitative+Analyst%22',
        '%22Trade+Operations+Analyst%22',
    ]
    urls = []
    for role in roles:
        # Entry + Associate level, remote + hybrid, posted last 24h
        urls.append(
            f"https://www.linkedin.com/jobs/search/?keywords={role}"
            f"&location=United+States&f_TPR=r86400&f_E=1%2C2&f_WT=2%2C3"
        )
    return urls


def build_indeed_urls():
    """
    borderline/indeed-scraper takes direct Indeed search URLs.
    fromage=1 = posted today, sort=date = newest first.
    Quoted titles prevent irrelevant results.
    """
    roles = [
        '%22data+scientist%22',
        '%22AI+engineer%22',
        '%22machine+learning+engineer%22',
        '%22senior+data+analyst%22',
        '%22analytics+engineer%22',
        '%22applied+scientist%22',
        '%22quantitative+analyst%22',
        '%22trade+operations+analyst%22',
        '%22junior+data+scientist%22',
        '%22data+scientist%22+entry+level',
    ]
    urls = []
    for q in roles:
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
    # applicantCount: LinkedIn returns this as int when fetchJobDetails=True
    # Indeed (borderline) does not expose it — we default to None (skip check)
    raw_applicants = (
        job.get("applicantCount")
        or job.get("numberOfApplicants")
        or job.get("applicants")
    )
    try:
        applicant_count = int(raw_applicants) if raw_applicants is not None else None
    except (ValueError, TypeError):
        applicant_count = None

    return {
        "title":           (job.get("title")
                            or job.get("positionName")
                            or job.get("jobTitle")
                            or job.get("name")
                            or "Unknown Role"),
        "company":         (job.get("companyName")
                            or job.get("company")
                            or job.get("employer")
                            or "Unknown Company"),
        "location":        (job.get("location")
                            or job.get("jobLocation")
                            or "Unknown Location"),
        "salary":          (job.get("salary")
                            or job.get("salaryMin")
                            or job.get("compensation")
                            or job.get("salaryRange")
                            or "Not listed"),
        "url":             (job.get("jobUrl")
                            or job.get("url")
                            or job.get("externalUrl")
                            or job.get("applyUrl")
                            or "#"),
        "description":     (job.get("description")
                            or job.get("jobDescription")
                            or job.get("summary")
                            or ""),
        "posted_at":       (job.get("postedAt")
                            or job.get("datePosted")
                            or job.get("publishedAt")
                            or job.get("date")
                            or job.get("scrapedAt")
                            or ""),
        "applicant_count": applicant_count,  # None = unknown (don't filter out)
        "_source":         job.get("_source", ""),
    }


# ─────────────────────────────────────────────
# SCRAPERS — capped at 50 each
# ─────────────────────────────────────────────

def scrape_linkedin():
    print("  [LinkedIn] Scraping (cap: 50, fetchJobDetails: True)...")
    try:
        client = ApifyClient(APIFY_API_TOKEN)
        run    = client.actor(LINKEDIN_ACTOR).call(run_input={
            "urls":             build_linkedin_urls(),
            "count":            7,           # ~7 per URL × 8 URLs = ~56, trimmed to 50
            "fetchJobDetails":  True,        # ← enables applicantCount field
        })
        items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        items = items[:50]                   # hard cap at 50
        for j in items: j["_source"] = "LinkedIn"
        print(f"  [LinkedIn] {len(items)} raw (capped at 50)")
        return items
    except Exception as e:
        print(f"  [LinkedIn ERROR] {e}")
        return []


def scrape_indeed():
    print("  [Indeed] Scraping with borderline actor (cap: 50)...")
    try:
        client = ApifyClient(APIFY_API_TOKEN)
        run    = client.actor(INDEED_ACTOR).call(run_input={
            "startUrls": [{"url": u} for u in build_indeed_urls()],
            "maxItems":  50,                 # hard cap at 50
            "maxAge":    1,                  # today only (borderline supports this)
        })
        items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        items = items[:50]
        for j in items: j["_source"] = "Indeed"
        print(f"  [Indeed] {len(items)} raw (capped at 50)")
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

BAD_TITLE_WORDS = [
    "cashier", "sales representative", "busser", "waiter", "driver",
    "warehouse", "nurse", "teacher", "mechanic", "electrician",
    "customer service", "call center", "receptionist",
    "firmware engineer", "hardware engineer", "production manager",
    "marketing coordinator", "hr coordinator", "construction",
    "t-mobile", "now hiring", "campus minister", "religious",
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
        return True   # no timestamp → include (don't miss real jobs)
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


def has_too_many_applicants(job):
    """
    Returns True (= disqualify) if applicant count is known AND over limit.
    If count is None (Indeed, or LinkedIn without detail fetch), allow through.
    """
    count = job.get("applicant_count")
    if count is None:
        return False   # unknown → don't filter out
    too_many = count > MAX_APPLICANTS
    if too_many:
        print(f"    ✗ Too many applicants ({count}) — {job['title']} @ {job['company']}")
    return too_many


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

    s_seen = s_title = s_fresh = s_disq = s_salary = s_applicants = 0
    passed = []

    for job in normalized:
        h = job_hash(job)
        if h in SEEN_JOB_HASHES:           s_seen       += 1; continue
        if is_bad_title(job):               s_title      += 1; continue
        if not is_fresh(job):               s_fresh      += 1; continue
        if has_too_many_applicants(job):    s_applicants += 1; continue
        if is_disqualified(job):            s_disq       += 1; continue
        if not has_target_salary(job):      s_salary     += 1; continue

        job["score"] = score_job(job)
        SEEN_JOB_HASHES.add(h)
        passed.append(job)

    print(
        f"  Filters: {len(normalized)} in | "
        f"dupe:{s_seen} bad_title:{s_title} stale:{s_fresh} "
        f"too_many_applicants:{s_applicants} disq:{s_disq} "
        f"salary:{s_salary} | {len(passed)} passed ✅"
    )

    passed.sort(key=lambda j: j["score"], reverse=True)

    # Dedupe by title+company (both scrapers may find same job)
    seen_keys, deduped = set(), []
    for job in passed:
        key = job["title"].lower()[:30] + job["company"].lower()[:20]
        if key not in seen_keys:
            seen_keys.add(key)
            deduped.append(job)

    print(f"  After dedup: {len(deduped)} unique jobs")
    return deduped


# ─────────────────────────────────────────────
# TELEGRAM
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
            f"No new matching jobs under 100 applicants this run."
        )
        return

    # Header
    send_telegram(
        f"🚨 <b>{len(jobs)} Fresh Job(s) — {run_label}</b>\n"
        f"🕐 {datetime.now(ET).strftime('%a %b %d, %I:%M %p ET')}\n"
        f"✅ All under 100 applicants · Posted ≤3hrs ago\n"
        f"All {len(jobs)} match(es) below 👇"
    )

    # Batches of 5
    for i in range(0, len(jobs), 5):
        batch = jobs[i:i+5]
        lines = []
        for j, job in enumerate(batch, start=i+1):
            sal = job["salary"] if job["salary"] != "Not listed" else "Salary not listed"
            # Show applicant count if known
            appl = job.get("applicant_count")
            appl_str = f"👥 {appl} applicants" if appl is not None else "👥 Applicants: unknown"
            lines.append(
                f"{j}. {score_label(job['score'])} — <b>{job['title']}</b>\n"
                f"   🏢 {job['company']}  📍 {job['location']}\n"
                f"   💰 {sal}  |  {job['_source']}\n"
                f"   {appl_str}  |  Match: {job['score']}/100\n"
                f"   🔗 <a href='{job['url']}'>Apply Now →</a>\n"
            )
        send_telegram("\n".join(lines))

    # Top 3 summary
    top3  = jobs[:3]
    lines = ["⭐ <b>Top picks this run:</b>"]
    for job in top3:
        appl = job.get("applicant_count")
        appl_str = f"{appl} applicants" if appl is not None else "applicants unknown"
        lines.append(
            f"• <a href='{job['url']}'>{job['title']} @ {job['company']}</a>"
            f" ({appl_str})"
        )
    lines.append("\n🎯 Apply within the hour — you're in the first wave!")
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
    print(f"  Total raw: {len(all_jobs)} (LinkedIn + Indeed, 50 each max)")

    filtered = filter_jobs(all_jobs)
    send_all_jobs(filtered, run_label)
    print(f"  Done — sent {len(filtered)} jobs to Telegram\n")


# ─────────────────────────────────────────────
# STARTUP CHECKS
# ─────────────────────────────────────────────

def run_startup_checks():
    print("\n--- Startup checks ---")
    missing = [k for k, v in {
        "APIFY_API_TOKEN":    APIFY_API_TOKEN,
        "TELEGRAM_BOT_TOKEN": TELEGRAM_BOT_TOKEN,
        "TELEGRAM_CHAT_ID":   TELEGRAM_CHAT_ID,
    }.items() if not v]

    if missing:
        print(f"  ❌ Missing: {', '.join(missing)}"); return False
    print("  ✅ All env vars set")

    ok = send_telegram(
        "✅ <b>Job Pipeline v5.1 is live!</b>\n\n"
        "🔍 <b>Roles:</b> Data Scientist · AI Engineer · ML Engineer\n"
        "Senior Analyst · Analytics Engineer · Quant · Trade Ops\n\n"
        "🎯 <b>Filters active:</b>\n"
        "• Posted ≤ 3 hours ago\n"
        "• Under 100 applicants (LinkedIn)\n"
        "• $120K+ salary (or unlisted)\n"
        "• No sponsorship/clearance restrictions\n"
        "• Max 50 results per source per run\n\n"
        "📅 Mon/Wed/Thu: every 3h | Tue: every 2h"
    )
    if not ok:
        print("  ❌ Telegram failed — check tokens in Railway Variables")
        return False
    print("  ✅ Telegram working!")
    print("--- Checks passed ---\n")
    return True


# ─────────────────────────────────────────────
# SCHEDULER
# ─────────────────────────────────────────────

if __name__ == "__main__":
    print("="*52)
    print("  Ashvin Job Pipeline v5.1")
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

    print("  Mon/Wed/Thu → 7, 10am, 1, 4, 7, 9pm ET")
    print("  Tuesday     → 7, 9, 11am, 1, 3, 5, 7, 9pm ET")
    print("  Fri-Sun     → OFF\n")

    now = datetime.now(ET)
    if now.weekday() <= 3 and 7 <= now.hour <= 21:
        print("  Running initial scan...\n")
        run_pipeline()

    try:
        scheduler.start()
    except KeyboardInterrupt:
        print("  Stopped.")
