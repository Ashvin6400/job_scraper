"""
=============================================================
ASHVIN'S JOB PIPELINE v7 — GOOGLE JOBS VIA SERPER.DEV
=============================================================
No more Apify — replaced with Serper.dev free tier.

Serper.dev free tier: 2,500 searches/month
Our usage: ~9 queries × 6 runs/day Mon-Tue = ~700/month ✅

Coverage: Google Jobs aggregates LinkedIn, ZipRecruiter,
Glassdoor, company career pages — broader than LinkedIn alone.
(Note: Indeed is NOT included — Google and Indeed are competitors)

Signup: serper.dev → free account → copy API key

pip install requests apscheduler
=============================================================
"""

import os
import re
import hashlib
import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import requests

SERPER_API_KEY     = os.environ.get("SERPER_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")

ET                   = ZoneInfo("America/New_York")
MAX_APPLICANTS       = 100
SEEN_JOB_HASHES: set = set()

# ─────────────────────────────────────────────
# TARGET ROLES
# ─────────────────────────────────────────────

SEARCH_QUERIES = [
    "Data Scientist",
    "AI Engineer",
    "Machine Learning Engineer",
    "Senior Data Analyst",
    "Analytics Engineer",
    "Applied Scientist",
    "Quantitative Analyst",
    "Trade Operations Analyst",
    "Junior Data Scientist",
]

# ─────────────────────────────────────────────
# GOOGLE JOBS SCRAPER via Serper.dev
# Returns structured JSON — no HTML parsing needed
# ─────────────────────────────────────────────

def scrape_google_jobs() -> list:
    """
    Serper.dev /google/jobs endpoint returns structured job listings
    directly from Google Jobs — covers LinkedIn, ZipRecruiter,
    Glassdoor, company career pages in one call.
    """
    print(f"  [Google Jobs] Scraping {len(SEARCH_QUERIES)} queries via Serper...")
    all_jobs = []

    for query in SEARCH_QUERIES:
        try:
            response = requests.post(
                "https://google.serper.dev/jobs",
                headers={
                    "X-API-KEY":    SERPER_API_KEY,
                    "Content-Type": "application/json",
                },
                json={
                    "q":      query,
                    "gl":     "us",          # country: US
                    "hl":     "en",          # language: English
                    "num":    10,            # results per query (max 10 on free)
                    "tbs":    "qdr:d",       # posted in last 24 hours
                },
                timeout=15,
            )

            if not response.ok:
                print(f"    '{query}' ERROR: {response.status_code} {response.text[:100]}")
                continue

            data = response.json()
            jobs = data.get("jobs", [])

            for j in jobs:
                j["_source"]  = "Google Jobs"
                j["_query"]   = query

            all_jobs.extend(jobs)
            print(f"    '{query}': {len(jobs)} results")

        except Exception as e:
            print(f"    '{query}' ERROR: {e}")

    # Cap at 50 total
    all_jobs = all_jobs[:50]
    print(f"  [Google Jobs] {len(all_jobs)} total raw (capped at 50)")
    return all_jobs


# ─────────────────────────────────────────────
# NORMALIZER — Serper Google Jobs field names
# Confirmed from Serper docs:
#   jobTitle, companyName, location, salary,
#   applyLink, description, date, via,
#   extensions (list like ["2 hours ago", "Full-time", "Remote"])
# ─────────────────────────────────────────────

def normalize(job: dict) -> dict:
    # Extensions array contains: posting age, job type, remote/hybrid, salary
    extensions = job.get("extensions", []) or []
    ext_text   = " | ".join(str(e) for e in extensions).lower()

    # Extract age from extensions e.g. "2 hours ago", "1 day ago"
    age_str = ""
    for ext in extensions:
        e = str(ext).lower()
        if "hour" in e or "minute" in e or "day" in e or "just" in e:
            age_str = str(ext)
            break

    # Salary from extensions or dedicated field
    salary = job.get("salary", "") or ""
    if not salary:
        for ext in extensions:
            e = str(ext)
            if "$" in e or "year" in e.lower() or "hour" in e.lower():
                salary = e
                break
    if not salary:
        salary = "Not listed"

    # Source platform (e.g. "via LinkedIn", "via ZipRecruiter")
    via = job.get("via", "")
    source_display = f"Google Jobs ({via})" if via else "Google Jobs"

    return {
        "title":           job.get("title",       job.get("jobTitle", "Unknown Role")),
        "company":         job.get("companyName", job.get("company",  "Unknown Company")),
        "location":        job.get("location",    "Unknown Location"),
        "salary":          salary,
        "url":             job.get("applyLink",   job.get("link",     "#")),
        "description":     job.get("description", ""),
        "posted_at":       job.get("date",        ""),
        "age_str":         age_str,
        "posted_today":    ("hour" in ext_text or "minute" in ext_text or
                            "just" in ext_text or "today" in ext_text),
        "via":             via,
        "extensions":      extensions,
        "applicant_count": None,   # Google Jobs doesn't expose this
        "_source":         source_display,
        "_query":          job.get("_query", ""),
    }


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
    "marketing coordinator", "construction", "now hiring",
]

SALARY_KEYWORDS = [
    "$120k","$125k","$130k","$140k","$150k","$160k","$170k","$180k","$190k","$200k",
    "120,000","125,000","130,000","140,000","150,000","160,000","170,000","180,000",
]


def job_hash(job: dict) -> str:
    key = job["title"].lower().strip() + job["company"].lower().strip()
    return hashlib.md5(key.encode()).hexdigest()


def is_fresh(job: dict) -> bool:
    """
    Google Jobs via Serper returns:
    - extensions like ["2 hours ago", "Full-time"]
    - date field like "2 days ago" or "2026-03-17"
    We use posted_today (from extensions) as primary signal.
    """
    if job.get("posted_today"):
        return True

    age = job.get("age_str", "").lower()
    if age:
        nums = re.findall(r'\d+', age)
        n    = int(nums[0]) if nums else 99
        if "minute" in age:              return True
        if "hour"   in age and n <= 3:   return True
        if "just"   in age:              return True
        if "day"    in age:              return False

    # Fallback: parse date field
    date_str = job.get("posted_at", "")
    if date_str:
        try:
            # "2 days ago" style
            if "day" in date_str.lower():
                nums = re.findall(r'\d+', date_str)
                if nums and int(nums[0]) <= 1:
                    return True
                return False
            # ISO date "2026-03-17"
            posted = datetime.fromisoformat(date_str).date()
            today  = datetime.now(ET).date()
            return posted >= today - timedelta(days=1)
        except Exception:
            pass

    return True  # unknown → include


def is_bad_title(job: dict) -> bool:
    return any(kw in job["title"].lower() for kw in BAD_TITLE_WORDS)


def is_disqualified(job: dict) -> bool:
    return any(p in job["description"].lower() for p in DISQUALIFY_PHRASES)


def has_target_salary(job: dict) -> bool:
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


def score_job(job: dict) -> int:
    score = 0
    text  = (job["title"] + " " + job["description"]).lower()

    for skill, pts in {
        "python":15,"sql":10,"machine learning":12,"power bi":8,
        "data science":10,"azure":8,"llm":12,"rag":10,
        "databricks":8,"tensorflow":8,"tableau":6,"pandas":5,
        "statistical":8,"etl":6,"forecasting":7,"nlp":8,
        "deep learning":8,"scikit":7,"spark":7,"snowflake":6,
    }.items():
        if skill in text: score += pts

    for t in ["data scientist","ai engineer","ml engineer","machine learning",
              "analytics engineer","applied scientist","quantitative"]:
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


def filter_jobs(raw_jobs: list) -> list:
    normalized = [normalize(j) for j in raw_jobs]
    s_seen = s_title = s_fresh = s_disq = s_sal = 0
    passed = []

    for job in normalized:
        h = job_hash(job)
        if h in SEEN_JOB_HASHES:          s_seen  += 1; continue
        if is_bad_title(job):              s_title += 1; continue
        if not is_fresh(job):              s_fresh += 1; continue
        if is_disqualified(job):           s_disq  += 1; continue
        if not has_target_salary(job):     s_sal   += 1; continue
        job["score"] = score_job(job)
        SEEN_JOB_HASHES.add(h)
        passed.append(job)

    print(
        f"  Filters: {len(normalized)} in | "
        f"dupe:{s_seen} bad_title:{s_title} stale:{s_fresh} "
        f"disq:{s_disq} salary:{s_sal} | {len(passed)} passed ✅"
    )

    passed.sort(key=lambda j: j["score"], reverse=True)

    # Dedupe by title+company
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

def send_telegram(text: str) -> bool:
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


def score_label(score: int) -> str:
    if score >= 70: return "🟢 HIGH"
    if score >= 45: return "🟡 MED"
    return "🔵 LOW"


def send_all_jobs(jobs: list, run_label: str):
    if not jobs:
        send_telegram(
            f"✅ <b>Pipeline ran — {run_label}</b>\n"
            f"🕐 {datetime.now(ET).strftime('%a %b %d, %I:%M %p ET')}\n"
            f"No new matching jobs this run."
        )
        return

    send_telegram(
        f"🚨 <b>{len(jobs)} Fresh Job(s) — {run_label}</b>\n"
        f"🕐 {datetime.now(ET).strftime('%a %b %d, %I:%M %p ET')}\n"
        f"📡 Source: Google Jobs (LinkedIn · ZipRecruiter · Glassdoor · Company pages)\n"
        f"All {len(jobs)} match(es) below 👇"
    )

    for i in range(0, len(jobs), 5):
        batch = jobs[i:i+5]
        lines = []
        for j, job in enumerate(batch, start=i+1):
            sal  = job["salary"] if job["salary"] != "Not listed" else "Salary not listed"
            age  = f" · {job['age_str']}" if job.get("age_str") else ""
            via  = f" · via {job['via']}" if job.get("via") else ""
            lines.append(
                f"{j}. {score_label(job['score'])} — <b>{job['title']}</b>\n"
                f"   🏢 {job['company']}  📍 {job['location']}\n"
                f"   💰 {sal}{age}{via}\n"
                f"   📊 Match: {job['score']}/100\n"
                f"   🔗 <a href='{job['url']}'>Apply Now →</a>\n"
            )
        send_telegram("\n".join(lines))

    top3  = jobs[:3]
    lines = ["⭐ <b>Top picks this run:</b>"]
    for job in top3:
        lines.append(f"• <a href='{job['url']}'>{job['title']} @ {job['company']}</a>")
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

    raw      = scrape_google_jobs()
    filtered = filter_jobs(raw)
    send_all_jobs(filtered, run_label)
    print(f"  Done — {len(filtered)} jobs sent to Telegram\n")


# ─────────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────────

def run_startup_checks() -> bool:
    print("\n--- Startup checks ---")
    missing = [k for k, v in {
        "SERPER_API_KEY":     SERPER_API_KEY,
        "TELEGRAM_BOT_TOKEN": TELEGRAM_BOT_TOKEN,
        "TELEGRAM_CHAT_ID":   TELEGRAM_CHAT_ID,
    }.items() if not v]
    if missing:
        print(f"  ❌ Missing env vars: {', '.join(missing)}"); return False
    print("  ✅ All env vars set")

    ok = send_telegram(
        "✅ <b>Job Pipeline v7 is live!</b>\n\n"
        "📡 <b>Source: Google Jobs (FREE)</b>\n"
        "Covers: LinkedIn · ZipRecruiter · Glassdoor · Company pages\n\n"
        "🔍 <b>Roles:</b> Data Scientist · AI Engineer · ML Engineer\n"
        "Senior Analyst · Analytics Engineer · Quant · Trade Ops\n\n"
        "🎯 <b>Filters:</b>\n"
        "• Posted today / last few hours only\n"
        "• $120K+ or salary not listed\n"
        "• No sponsorship/clearance requirements\n\n"
        "📅 Mon/Wed/Thu: every 3h | Tuesday: every 2h\n"
        "No Apify needed — runs on free Serper.dev tier!"
    )
    if not ok:
        print("  ❌ Telegram failed"); return False
    print("  ✅ Telegram working!")
    print("--- Checks passed ---\n")
    return True


# ─────────────────────────────────────────────
# SCHEDULER
# ─────────────────────────────────────────────

if __name__ == "__main__":
    print("="*52)
    print("  Ashvin Job Pipeline v7")
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
