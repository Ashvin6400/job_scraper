"""
=============================================================
ASHVIN'S JOB PIPELINE v4 — FIXED SCRAPER INPUTS
=============================================================
What was broken in v3:
  ✗ Wrong input format for LinkedIn actor (used searchTerms — doesn't exist)
  ✗ Wrong input format for Indeed actor (used position/country — wrong fields)
  ✗ Used raw requests instead of apify-client (less reliable)

What's fixed now:
  ✓ LinkedIn: passes direct search URLs (correct format)
  ✓ Indeed: passes direct search URLs (correct format)
  ✓ Uses official apify-client library
  ✓ Sends a Telegram test message on startup so you know it works
  ✓ Better logging so you can see exactly what's happening

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

# ─────────────────────────────────────────────
# CONFIGURATION — from Railway environment vars
# ─────────────────────────────────────────────

APIFY_API_TOKEN    = os.environ.get("APIFY_API_TOKEN", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")

ET                       = ZoneInfo("America/New_York")
FRESHNESS_WINDOW_MINUTES = 120
SEEN_JOB_HASHES: set     = set()

# ─────────────────────────────────────────────
# ACTOR IDs (correct ones)
# ─────────────────────────────────────────────

# LinkedIn — takes URLs like linkedin.com/jobs/search/?keywords=...
LINKEDIN_ACTOR = "curious_coder/linkedin-jobs-scraper"

# Indeed — takes URLs like indeed.com/jobs?q=...&l=...
INDEED_ACTOR   = "misceres/indeed-scraper"

# ─────────────────────────────────────────────
# SEARCH URL BUILDERS
# ─────────────────────────────────────────────

def build_linkedin_urls() -> list:
    """Build LinkedIn job search URLs for each role."""
    queries = [
        "Data+Scientist",
        "AI+Engineer",
        "Machine+Learning+Engineer",
        "Applied+Scientist",
        "Senior+Data+Analyst",
        "Analytics+Engineer",
        "Quantitative+Analyst",
        "Trade+Operations+Analyst",
    ]
    urls = []
    for q in queries:
        # f_TPR=r86400 = posted in last 24 hours
        # f_WT=2 = remote
        urls.append(
            f"https://www.linkedin.com/jobs/search/?keywords={q}"
            f"&location=United+States&f_TPR=r86400&f_WT=2&position=1&pageNum=0"
        )
        # Also non-remote (hybrid/onsite)
        urls.append(
            f"https://www.linkedin.com/jobs/search/?keywords={q}"
            f"&location=United+States&f_TPR=r86400&position=1&pageNum=0"
        )
    return urls


def build_indeed_urls() -> list:
    """Build Indeed job search URLs for each role."""
    queries = [
        ("data+scientist", "United+States"),
        ("AI+engineer", "United+States"),
        ("machine+learning+engineer", "United+States"),
        ("senior+data+analyst", "United+States"),
        ("quantitative+analyst", "United+States"),
        ("trade+operations+analyst", "United+States"),
        ("analytics+engineer", "United+States"),
        ("applied+scientist", "United+States"),
    ]
    urls = []
    for q, loc in queries:
        # fromage=1 = posted today, sort=date = newest first
        urls.append(
            f"https://www.indeed.com/jobs?q={q}&l={loc}&fromage=1&sort=date"
        )
    return urls


# ─────────────────────────────────────────────
# SCRAPERS (using apify-client — official library)
# ─────────────────────────────────────────────

def scrape_linkedin() -> list:
    print("  [LinkedIn] Starting scrape...")
    try:
        client   = ApifyClient(APIFY_API_TOKEN)
        run_input = {
            "urls": build_linkedin_urls()[:6],  # limit to 6 URLs per run to control cost
            "count": 10,                         # jobs per URL
        }
        run    = client.actor(LINKEDIN_ACTOR).call(run_input=run_input)
        items  = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        print(f"  [LinkedIn] Got {len(items)} raw jobs")
        for j in items:
            j["_source"] = "LinkedIn"
        return items
    except Exception as e:
        print(f"  [LinkedIn ERROR] {e}")
        return []


def scrape_indeed() -> list:
    print("  [Indeed] Starting scrape...")
    try:
        client    = ApifyClient(APIFY_API_TOKEN)
        run_input = {
            "startUrls": [{"url": u} for u in build_indeed_urls()[:6]],
            "maxItems":  50,
        }
        run   = client.actor(INDEED_ACTOR).call(run_input=run_input)
        items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        print(f"  [Indeed] Got {len(items)} raw jobs")
        for j in items:
            j["_source"] = "Indeed"
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
    "cannot sponsor", "sponsorship is not", "not able to sponsor",
    "must be a u.s. citizen", "must be a us citizen",
    "u.s. citizenship required", "us citizenship required",
    "active security clearance", "security clearance required",
    "secret clearance", "top secret", "must hold a clearance",
    "green card required", "gc required", "permanent resident required",
    "work authorization will not", "no h-1b", "no h1b",
]

SALARY_KEYWORDS = [
    "$120k","$125k","$130k","$140k","$150k","$160k","$170k","$180k","$190k","$200k",
    "120,000","125,000","130,000","140,000","150,000","160,000","170,000","180,000",
]


def job_hash(job: dict) -> str:
    key = (
        str(job.get("title","") or "").lower().strip() +
        str(job.get("companyName", job.get("company","")) or "").lower().strip()
    )
    return hashlib.md5(key.encode()).hexdigest()


def is_fresh(job: dict) -> bool:
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=FRESHNESS_WINDOW_MINUTES)
    for field in ["postedAt","datePosted","publishedAt","date","listingDate","jobPostedAt","scrapedAt"]:
        raw = job.get(field)
        if not raw:
            continue
        try:
            posted = (
                datetime.fromtimestamp(float(raw), tz=timezone.utc)
                if isinstance(raw, (int, float))
                else datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            )
            return posted >= cutoff
        except Exception:
            continue
    return True  # no timestamp → include (don't miss real jobs)


def is_disqualified(job: dict) -> bool:
    desc = (job.get("description","") or job.get("jobDescription","") or "").lower()
    return any(p in desc for p in DISQUALIFY_PHRASES)


def has_target_salary(job: dict) -> bool:
    txt = " ".join([
        str(job.get("salary","") or ""),
        str(job.get("salaryMin","") or ""),
        str(job.get("compensation","") or ""),
    ]).lower()
    if not txt.strip():
        return True  # no salary listed → include, filter manually
    if any(kw.lower() in txt for kw in SALARY_KEYWORDS):
        return True
    for n in re.findall(r'\$?([\d,]+)', txt):
        try:
            v = int(n.replace(",",""))
            if v >= 120000 or 120 <= v <= 999:
                return True
        except ValueError:
            pass
    return False


def score_job(job: dict) -> int:
    score = 0
    text  = " ".join([
        str(job.get("title","") or ""),
        str(job.get("description","") or ""),
        str(job.get("jobDescription","") or ""),
    ]).lower()

    for skill, pts in {
        "python":15, "sql":10, "machine learning":12, "power bi":8,
        "data science":10, "azure":8, "llm":12, "rag":10,
        "databricks":8, "tensorflow":8, "tableau":6, "pandas":5,
        "statistical":8, "etl":6, "forecasting":7, "nlp":8,
    }.items():
        if skill in text:
            score += pts

    for p in ["1-3 years","2+ years","entry level","junior","0-2 years","new grad","associate"]:
        if p in text: score += 10
    for p in ["3-5 years","senior","3+ years"]:
        if p in text: score += 3
    for p in ["10+ years","8+ years","director","principal staff","vp of"]:
        if p in text: score -= 15
    for p in ["quant","trading","risk","portfolio","hedge fund","bloomberg","sharpe"]:
        if p in text: score += 8

    return min(100, max(0, score))


def filter_jobs(raw: list) -> list:
    passed = []
    skipped_seen = skipped_stale = skipped_disq = skipped_salary = 0

    for job in raw:
        h = job_hash(job)
        if h in SEEN_JOB_HASHES:
            skipped_seen += 1
            continue
        if not is_fresh(job):
            skipped_stale += 1
            continue
        if is_disqualified(job):
            skipped_disq += 1
            continue
        if not has_target_salary(job):
            skipped_salary += 1
            continue
        job["relevance_score"] = score_job(job)
        SEEN_JOB_HASHES.add(h)
        passed.append(job)

    print(f"  Filter breakdown → seen:{skipped_seen} stale:{skipped_stale} "
          f"disqualified:{skipped_disq} salary:{skipped_salary} passed:{len(passed)}")

    return sorted(passed, key=lambda j: j.get("relevance_score", 0), reverse=True)


# ─────────────────────────────────────────────
# TELEGRAM
# ─────────────────────────────────────────────

def send_telegram(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("  [Telegram] Tokens not set — skipping")
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=15,
        )
        if not r.ok:
            print(f"  [Telegram ERROR] {r.status_code}: {r.text}")
            return False
        return True
    except Exception as e:
        print(f"  [Telegram ERROR] {e}")
        return False


def send_alerts(jobs: list, run_label: str):
    if not jobs:
        print("  No new matching jobs this run.")
        return

    send_telegram(
        f"🚨 <b>{len(jobs)} New Job(s) — {run_label}</b>\n"
        f"🕐 {datetime.now(ET).strftime('%a %b %d, %I:%M %p ET')}\n"
        f"{'─' * 28}"
    )

    for job in jobs[:8]:
        score   = job.get("relevance_score", 0)
        icon    = "🟢" if score >= 70 else "🟡" if score >= 45 else "🔵"
        title   = job.get("title","?")
        company = job.get("companyName", job.get("company","?"))
        loc     = job.get("location","?")
        salary  = job.get("salary", job.get("compensation","Not listed"))
        url     = job.get("jobUrl", job.get("url", job.get("externalUrl","#")))
        source  = job.get("_source","")

        send_telegram(
            f"{icon} <b>{title}</b>\n"
            f"🏢 {company}\n"
            f"📍 {loc}\n"
            f"💰 {salary}\n"
            f"📊 Match: {score}/100  |  {source}\n"
            f"🔗 <a href='{url}'>Apply Now →</a>"
        )

    if len(jobs) > 8:
        send_telegram(f"⚠️ +{len(jobs) - 8} more — check Apify console.")


# ─────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────

def run_pipeline():
    now_et  = datetime.now(ET)
    weekday = now_et.weekday()  # 0=Mon … 6=Sun
    hour    = now_et.hour

    if weekday > 3 or hour < 7 or hour > 21:
        print(f"[{now_et.strftime('%a %I:%M %p ET')}] Outside active window — skipping.")
        return

    day_names = ["Monday","Tuesday","Wednesday","Thursday"]
    run_label = f"{day_names[weekday]} {now_et.strftime('%I:%M %p ET')}"
    print(f"\n{'='*50}")
    print(f"  Pipeline run: {run_label}")
    print(f"{'='*50}")

    all_jobs = []
    all_jobs.extend(scrape_linkedin())
    all_jobs.extend(scrape_indeed())

    print(f"\n  Total raw jobs: {len(all_jobs)}")

    filtered = filter_jobs(all_jobs)
    send_alerts(filtered, run_label)
    print(f"  Run complete.\n")


# ─────────────────────────────────────────────
# STARTUP CHECKS
# ─────────────────────────────────────────────

def run_startup_checks():
    print("\n--- Startup checks ---")

    # Check env vars
    missing = []
    if not APIFY_API_TOKEN:    missing.append("APIFY_API_TOKEN")
    if not TELEGRAM_BOT_TOKEN: missing.append("TELEGRAM_BOT_TOKEN")
    if not TELEGRAM_CHAT_ID:   missing.append("TELEGRAM_CHAT_ID")

    if missing:
        print(f"  ❌ Missing environment variables: {', '.join(missing)}")
        print("  → Go to Railway → Variables tab and add them")
        return False

    print("  ✅ All environment variables set")

    # Test Telegram
    print("  Testing Telegram...")
    ok = send_telegram(
        "✅ <b>Ashvin Job Pipeline v4 is live!</b>\n"
        "🕐 Running Mon/Wed/Thu every 3h, Tuesday every 2h\n"
        "🔍 Searching: Data Scientist, AI Engineer, ML Engineer, "
        "Senior Analyst, Quant, Trade Ops\n"
        "📱 You'll get alerts like this when matching jobs are found."
    )
    if ok:
        print("  ✅ Telegram working — check your phone!")
    else:
        print("  ❌ Telegram failed — check BOT_TOKEN and CHAT_ID")
        return False

    print("--- Checks passed ---\n")
    return True


# ─────────────────────────────────────────────
# SCHEDULER
# ─────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 50)
    print("  Ashvin Job Pipeline v4")
    print("=" * 50)
    print(f"  Time: {datetime.now(ET).strftime('%A %I:%M %p ET')}")

    ok = run_startup_checks()
    if not ok:
        print("Fix the above errors then redeploy. Exiting.")
        exit(1)

    scheduler = BlockingScheduler(timezone=ET)

    # Mon / Wed / Thu — every 3 hours
    scheduler.add_job(run_pipeline, CronTrigger(
        day_of_week="mon,wed,thu",
        hour="7,10,13,16,19,21",
        minute=0,
        timezone=ET,
    ), id="mwt", name="Mon/Wed/Thu 3hr")

    # Tuesday — every 2 hours
    scheduler.add_job(run_pipeline, CronTrigger(
        day_of_week="tue",
        hour="7,9,11,13,15,17,19,21",
        minute=0,
        timezone=ET,
    ), id="tue", name="Tuesday 2hr")

    print("  Schedule active:")
    print("  ├─ Mon/Wed/Thu → 7am, 10am, 1pm, 4pm, 7pm, 9pm ET")
    print("  ├─ Tuesday     → 7am, 9am, 11am, 1pm, 3pm, 5pm, 7pm, 9pm ET")
    print("  └─ Fri–Sun     → OFF")
    print("\n  Waiting for next scheduled run...\n")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        print("  Stopped.")
