"""
=============================================================
ASHVIN'S JOB PIPELINE v7 — JSEARCH API (FREE, GOOGLE JOBS)
=============================================================
Uses JSearch by OpenWeb Ninja via RapidAPI (free tier).
Pulls from Google for Jobs → covers LinkedIn, Indeed,
Glassdoor, ZipRecruiter, company pages in one call.

Setup (2 min):
  1. Go to rapidapi.com/letscrape-6bRBa3QguO5/api/jsearch
  2. Sign up → subscribe to FREE plan (no credit card)
  3. Copy your X-RapidAPI-Key from the API console
  4. Add to Railway as: RAPIDAPI_KEY=your_key_here

Free tier: 200 requests/month
Our usage: 9 queries × ~6 runs/week = ~216/month
Tip: Run Tuesday only at full 9 queries, other days 5 queries
     to stay comfortably within 200/month free tier.

pip install requests apscheduler
=============================================================
"""

import os
import re
import hashlib
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import requests

RAPIDAPI_KEY       = os.environ.get("RAPIDAPI_KEY", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")

ET                   = ZoneInfo("America/New_York")
MAX_APPLICANTS       = 100
SEEN_JOB_HASHES: set = set()

JSEARCH_URL = "https://jsearch.p.rapidapi.com/search"
JSEARCH_HEADERS = {
    "X-RapidAPI-Key":  RAPIDAPI_KEY,
    "X-RapidAPI-Host": "jsearch.p.rapidapi.com",
}

# Tuesday: all 9 queries. Other days: top 5 to save API calls.
SEARCH_QUERIES_FULL = [
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
SEARCH_QUERIES_SHORT = SEARCH_QUERIES_FULL[:5]


# ─────────────────────────────────────────────
# SCRAPER — JSearch API
# ─────────────────────────────────────────────

def scrape_jsearch(is_tuesday: bool) -> list:
    queries   = SEARCH_QUERIES_FULL if is_tuesday else SEARCH_QUERIES_SHORT
    all_jobs  = []

    print(f"  [JSearch] {len(queries)} queries, date_posted=today...")

    for query in queries:
        try:
            resp = requests.get(
                JSEARCH_URL,
                headers=JSEARCH_HEADERS,
                params={
                    "query":       f"{query} in United States",
                    "page":        "1",
                    "num_pages":   "1",
                    "date_posted": "today",      # only today's jobs
                    "remote_jobs_only": "false", # include all — filter later
                    "employment_types": "FULLTIME",
                },
                timeout=20,
            )

            if not resp.ok:
                print(f"    '{query}' ERROR: {resp.status_code} {resp.text[:120]}")
                continue

            data = resp.json()
            jobs = data.get("data", [])

            for j in jobs:
                j["_source"] = "Google Jobs"
                j["_query"]  = query

            all_jobs.extend(jobs)
            print(f"    '{query}': {len(jobs)} results")

        except Exception as e:
            print(f"    '{query}' ERROR: {e}")

    all_jobs = all_jobs[:60]
    print(f"  [JSearch] {len(all_jobs)} total raw")
    return all_jobs


# ─────────────────────────────────────────────
# NORMALIZER — JSearch confirmed field names
# job_title, employer_name, job_city, job_state,
# job_min_salary, job_max_salary, job_salary_period,
# job_apply_link, job_description,
# job_posted_at_datetime_utc, job_posted_at_timestamp,
# job_is_remote, job_required_experience
# ─────────────────────────────────────────────

def normalize(job: dict) -> dict:
    # Location
    city  = job.get("job_city", "") or ""
    state = job.get("job_state", "") or ""
    if city and state:
        location = f"{city}, {state}"
    elif city or state:
        location = city or state
    else:
        location = "United States"
    if job.get("job_is_remote"):
        location = f"Remote ({location})" if location != "United States" else "Remote"

    # Salary
    s_min    = job.get("job_min_salary")
    s_max    = job.get("job_max_salary")
    s_period = (job.get("job_salary_period") or "year").lower()
    if s_min and s_max:
        salary = f"${int(s_min):,} – ${int(s_max):,} / {s_period}"
    elif s_min:
        salary = f"From ${int(s_min):,} / {s_period}"
    else:
        salary = "Not listed"

    # Timestamp — JSearch provides both UTC string and Unix timestamp
    posted_utc = job.get("job_posted_at_datetime_utc", "")
    posted_ts  = job.get("job_posted_at_timestamp")

    # Age string
    age_str = ""
    if posted_ts:
        try:
            posted_dt = datetime.fromtimestamp(int(posted_ts), tz=timezone.utc)
            delta     = datetime.now(timezone.utc) - posted_dt
            hours     = delta.total_seconds() / 3600
            if hours < 1:
                age_str = f"{int(delta.total_seconds()/60)} min ago"
            elif hours < 24:
                age_str = f"{int(hours)}h ago"
            else:
                age_str = f"{int(hours/24)}d ago"
        except Exception:
            pass

    # Via (source platform)
    via = job.get("job_publisher", "") or ""

    return {
        "title":           job.get("job_title", "Unknown Role"),
        "company":         job.get("employer_name", "Unknown Company"),
        "location":        location,
        "salary":          salary,
        "url":             job.get("job_apply_link", job.get("job_google_link", "#")),
        "description":     job.get("job_description", ""),
        "posted_utc":      posted_utc,
        "posted_ts":       posted_ts,
        "age_str":         age_str,
        "via":             via,
        "applicant_count": None,  # JSearch doesn't expose this
        "_source":         f"Google Jobs",
        "_via":            via,
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
    "cashier","sales representative","busser","waiter","driver",
    "warehouse","nurse","teacher","mechanic","electrician",
    "customer service","call center","receptionist",
    "firmware engineer","hardware engineer","production manager",
    "marketing coordinator","construction","now hiring",
]

SALARY_KEYWORDS = [
    "$120k","$125k","$130k","$140k","$150k","$160k","$170k","$180k","$190k","$200k",
    "120,000","125,000","130,000","140,000","150,000","160,000","170,000","180,000",
]


def job_hash(job: dict) -> str:
    key = job["title"].lower().strip() + job["company"].lower().strip()
    return hashlib.md5(key.encode()).hexdigest()


def is_fresh(job: dict) -> bool:
    ts = job.get("posted_ts")
    if ts:
        try:
            posted = datetime.fromtimestamp(int(ts), tz=timezone.utc)
            delta  = datetime.now(timezone.utc) - posted
            return delta.total_seconds() <= (24 * 3600)  # within last 24h
        except Exception:
            pass

    utc = job.get("posted_utc", "")
    if utc:
        try:
            posted = datetime.fromisoformat(utc.replace("Z", "+00:00"))
            delta  = datetime.now(timezone.utc) - posted
            return delta.total_seconds() <= (24 * 3600)
        except Exception:
            pass

    return True  # no timestamp → include


def is_bad_title(job: dict) -> bool:
    return any(kw in (job["title"] or "").lower() for kw in BAD_TITLE_WORDS)


def is_disqualified(job: dict) -> bool:
    return any(p in (job["description"] or "").lower() for p in DISQUALIFY_PHRASES)


def has_target_salary(job: dict) -> bool:
    txt = (job["salary"] or "").lower()
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
    text  = ((job["title"] or "") + " " + (job["description"] or "")).lower()

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
        if h in SEEN_JOB_HASHES:      s_seen  += 1; continue
        if is_bad_title(job):          s_title += 1; continue
        if not is_fresh(job):          s_fresh += 1; continue
        if is_disqualified(job):       s_disq  += 1; continue
        if not has_target_salary(job): s_sal   += 1; continue
        job["score"] = score_job(job)
        SEEN_JOB_HASHES.add(h)
        passed.append(job)

    print(
        f"  Filters: {len(normalized)} in | "
        f"dupe:{s_seen} bad_title:{s_title} stale:{s_fresh} "
        f"disq:{s_disq} salary:{s_sal} | {len(passed)} passed ✅"
    )

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
        f"📡 Via Google Jobs (LinkedIn · Indeed · Glassdoor · ZipRecruiter)\n"
        f"All {len(jobs)} match(es) below 👇"
    )

    for i in range(0, len(jobs), 5):
        batch = jobs[i:i+5]
        lines = []
        for j, job in enumerate(batch, start=i+1):
            sal = job["salary"] if job["salary"] != "Not listed" else "Salary not listed"
            age = f" · {job['age_str']}" if job.get("age_str") else ""
            via = f" · {job['_via']}" if job.get("_via") else ""
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

    day_names  = ["Monday","Tuesday","Wednesday","Thursday"]
    run_label  = f"{day_names[weekday]} {now_et.strftime('%I:%M %p ET')}"
    is_tuesday = (weekday == 1)

    print(f"\n{'='*52}\n  Run: {run_label}\n{'='*52}")

    raw      = scrape_jsearch(is_tuesday)
    filtered = filter_jobs(raw)
    send_all_jobs(filtered, run_label)
    print(f"  Done — {len(filtered)} jobs sent to Telegram\n")


# ─────────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────────

def run_startup_checks() -> bool:
    print("\n--- Startup checks ---")
    missing = [k for k, v in {
        "RAPIDAPI_KEY":       RAPIDAPI_KEY,
        "TELEGRAM_BOT_TOKEN": TELEGRAM_BOT_TOKEN,
        "TELEGRAM_CHAT_ID":   TELEGRAM_CHAT_ID,
    }.items() if not v]
    if missing:
        print(f"  ❌ Missing: {', '.join(missing)}"); return False
    print("  ✅ All env vars set")

    ok = send_telegram(
        "✅ <b>Job Pipeline v7 is live!</b>\n\n"
        "📡 <b>Source: Google Jobs via JSearch (FREE)</b>\n"
        "Covers: LinkedIn · Indeed · Glassdoor · ZipRecruiter · Company pages\n\n"
        "🔍 <b>Roles:</b> Data Scientist · AI/ML Engineer\n"
        "Senior Analyst · Analytics Engineer · Quant · Trade Ops\n\n"
        "🎯 <b>Filters:</b>\n"
        "• Posted today only\n"
        "• $120K+ or salary not listed\n"
        "• No sponsorship/clearance\n\n"
        "📅 Mon/Wed/Thu: every 3h | Tuesday: every 2h (all queries)"
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
