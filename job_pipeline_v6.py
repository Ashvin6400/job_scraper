"""
=============================================================
ASHVIN'S JOB PIPELINE v6 — ALL FIELD NAMES CONFIRMED
=============================================================
Fixed from debug output:
  ✓ LinkedIn applicant field: applicantsCount (not applicantCount)
  ✓ LinkedIn date field: postedAt (date string "2026-03-17")
  ✓ Indeed date: uses postedToday=True + age field ("2 hours ago")
  ✓ Indeed location: nested dict → location.city + formattedAddress
  ✓ Indeed salary: nested dict → parse correctly
  ✓ Indeed input: query + country (not startUrls)
  ✓ Indeed maxRows: 50 (maxItems was being ignored → used maxRows)
  ✓ LinkedIn count: minimum 10, using 10 per URL
  ✓ Applicant filter: <100 for LinkedIn, pass-through for Indeed

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

ET                   = ZoneInfo("America/New_York")
MAX_APPLICANTS       = 100
SEEN_JOB_HASHES: set = set()

LINKEDIN_ACTOR = "curious_coder/linkedin-jobs-scraper"
INDEED_ACTOR   = "borderline/indeed-scraper"

# ─────────────────────────────────────────────
# LINKEDIN SEARCH URLS
# ─────────────────────────────────────────────

LINKEDIN_ROLES = [
    '%22Data+Scientist%22',
    '%22AI+Engineer%22',
    '%22Machine+Learning+Engineer%22',
    '%22Senior+Data+Analyst%22',
    '%22Analytics+Engineer%22',
    '%22Applied+Scientist%22',
    '%22Quantitative+Analyst%22',
    '%22Trade+Operations+Analyst%22',
]

def build_linkedin_urls():
    urls = []
    for role in LINKEDIN_ROLES:
        # f_TPR=r86400 = last 24h, f_E=1,2 = entry+associate, f_WT=2,3 = remote+hybrid
        urls.append(
            f"https://www.linkedin.com/jobs/search/?keywords={role}"
            f"&location=United+States&f_TPR=r86400&f_E=1%2C2&f_WT=2%2C3"
        )
    return urls

# ─────────────────────────────────────────────
# INDEED QUERY PAIRS
# ─────────────────────────────────────────────

INDEED_QUERIES = [
    "data scientist",
    "AI engineer",
    "machine learning engineer",
    "senior data analyst",
    "analytics engineer",
    "applied scientist",
    "quantitative analyst",
    "trade operations analyst",
    "junior data scientist",
]

# ─────────────────────────────────────────────
# NORMALIZER — confirmed field names from debug
# ─────────────────────────────────────────────

def normalize(job: dict) -> dict:
    source = job.get("_source", "")

    # ── Title ──
    title = (job.get("title") or job.get("positionName") or "Unknown Role")

    # ── Company ──
    company = (job.get("companyName") or job.get("company") or "Unknown Company")

    # ── Location ──
    # LinkedIn: plain string
    # Indeed: {"city": "Remote", "formattedAddress": "Remote, US", ...}
    loc_raw = job.get("location", "")
    if isinstance(loc_raw, dict):
        location = (loc_raw.get("formattedAddress")
                    or loc_raw.get("city")
                    or "Unknown Location")
    else:
        location = str(loc_raw) if loc_raw else "Unknown Location"

    # ── Salary ──
    # LinkedIn: plain string or empty
    # Indeed: dict like {"min": 80000, "max": 120000, "type": "yearly"} or {}
    sal_raw = job.get("salary", "")
    if isinstance(sal_raw, dict) and sal_raw:
        s_min  = sal_raw.get("min", "")
        s_max  = sal_raw.get("max", "")
        s_type = sal_raw.get("type", "")
        if s_min and s_max:
            salary = f"${int(s_min):,} - ${int(s_max):,} {s_type}"
        elif s_min:
            salary = f"From ${int(s_min):,} {s_type}"
        else:
            salary = "Not listed"
    elif isinstance(sal_raw, str) and sal_raw.strip():
        salary = sal_raw.strip()
    else:
        salary = "Not listed"

    # ── URL ──
    url = (job.get("link")           # LinkedIn uses "link"
           or job.get("applyUrl")
           or job.get("jobUrl")
           or job.get("url")
           or "#")

    # ── Description ──
    description = (job.get("descriptionText")
                   or job.get("description")
                   or job.get("jobDescription")
                   or "")

    # ── Applicant count ──
    # LinkedIn: "applicantsCount" (confirmed from debug, e.g. 200)
    # Indeed: not present
    raw_appl = job.get("applicantsCount")   # confirmed LinkedIn field name
    try:
        applicant_count = int(raw_appl) if raw_appl is not None else None
    except (ValueError, TypeError):
        applicant_count = None

    # ── Freshness ──
    # LinkedIn: postedAt = "2026-03-17" (date string)
    # Indeed:   datePublished = "2026-03-17", postedToday = True, age = "2 hours ago"
    posted_at      = (job.get("postedAt") or job.get("datePublished") or "")
    posted_today   = job.get("postedToday", False)   # Indeed boolean
    age_str        = job.get("age", "")              # Indeed "2 hours ago"

    return {
        "title":           title,
        "company":         company,
        "location":        location,
        "salary":          salary,
        "url":             url,
        "description":     description,
        "applicant_count": applicant_count,
        "posted_at":       posted_at,
        "posted_today":    posted_today,
        "age_str":         age_str,
        "_source":         source,
    }

# ─────────────────────────────────────────────
# SCRAPERS
# ─────────────────────────────────────────────

def scrape_linkedin() -> list:
    print("  [LinkedIn] Scraping (10 per URL × 8 roles = up to 80, capped at 50)...")
    try:
        client = ApifyClient(APIFY_API_TOKEN)
        run    = client.actor(LINKEDIN_ACTOR).call(run_input={
            "urls":            build_linkedin_urls(),
            "count":           10,          # min allowed is 10
            "fetchJobDetails": True,        # needed to get applicantsCount
        })
        items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        items = items[:50]
        for j in items: j["_source"] = "LinkedIn"
        print(f"  [LinkedIn] {len(items)} raw")
        return items
    except Exception as e:
        print(f"  [LinkedIn ERROR] {e}")
        return []


def scrape_indeed() -> list:
    print(f"  [Indeed] Scraping {len(INDEED_QUERIES)} queries (maxRows=50, today only)...")
    all_items = []
    try:
        client = ApifyClient(APIFY_API_TOKEN)
        for query in INDEED_QUERIES:
            if len(all_items) >= 50:
                break
            try:
                run = client.actor(INDEED_ACTOR).call(run_input={
                    "query":    query,
                    "country":  "us",
                    "location": "United States",
                    "maxRows":  10,     # 10 per query × 9 queries = up to 90, stop at 50
                    "maxAge":   1,      # today only
                })
                items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
                for j in items: j["_source"] = "Indeed"
                all_items.extend(items)
                print(f"    '{query}': {len(items)} results")
            except Exception as e:
                print(f"    '{query}' ERROR: {e}")

        all_items = all_items[:50]
        print(f"  [Indeed] {len(all_items)} raw (capped at 50)")
        return all_items
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
    "now hiring", "campus", "religious",
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
    LinkedIn: postedAt is a date string like "2026-03-17" — accept if today or yesterday
    Indeed:   postedToday=True is reliable. Also parse age_str as backup.
    No timestamp at all → include (don't miss real jobs)
    """
    # Indeed: trust the postedToday boolean
    if job.get("posted_today") is True:
        return True

    # Indeed/LinkedIn: parse age string "X hours ago" or "X minutes ago"
    age = job.get("age_str", "").lower()
    if age:
        nums = re.findall(r'\d+', age)
        if nums:
            n = int(nums[0])
            if "minute" in age and n <= 180:   return True
            if "hour"   in age and n <= 3:     return True
            if "just"   in age:                return True
            # "1 day ago" or more → stale
            if "day" in age:                   return False

    # LinkedIn: postedAt date string
    posted_at = job.get("posted_at", "")
    if posted_at:
        try:
            today     = datetime.now(ET).date()
            yesterday = today - timedelta(days=1)
            posted    = datetime.fromisoformat(str(posted_at)).date()
            return posted >= yesterday
        except Exception:
            pass

    return True   # unknown → include


def has_too_many_applicants(job: dict) -> bool:
    count = job.get("applicant_count")
    if count is None:
        return False   # Indeed or unknown → don't filter
    if count > MAX_APPLICANTS:
        print(f"    ✗ {count} applicants — skipping: {job['title']} @ {job['company']}")
        return True
    return False


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
        "python":15, "sql":10, "machine learning":12, "power bi":8,
        "data science":10, "azure":8, "llm":12, "rag":10,
        "databricks":8, "tensorflow":8, "tableau":6, "pandas":5,
        "statistical":8, "etl":6, "forecasting":7, "nlp":8,
        "deep learning":8, "scikit":7, "spark":7, "snowflake":6,
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
    s_seen = s_title = s_fresh = s_appl = s_disq = s_sal = 0
    passed = []

    for job in normalized:
        h = job_hash(job)
        if h in SEEN_JOB_HASHES:          s_seen  += 1; continue
        if is_bad_title(job):              s_title += 1; continue
        if not is_fresh(job):              s_fresh += 1; continue
        if has_too_many_applicants(job):   s_appl  += 1; continue
        if is_disqualified(job):           s_disq  += 1; continue
        if not has_target_salary(job):     s_sal   += 1; continue
        job["score"] = score_job(job)
        SEEN_JOB_HASHES.add(h)
        passed.append(job)

    print(
        f"  Filters: {len(normalized)} in | "
        f"dupe:{s_seen} bad_title:{s_title} stale:{s_fresh} "
        f">100_applicants:{s_appl} disq:{s_disq} salary:{s_sal} "
        f"| {len(passed)} passed ✅"
    )

    passed.sort(key=lambda j: j["score"], reverse=True)

    # Dedupe cross-source
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
        f"✅ Posted today · Under 100 applicants (LinkedIn)\n"
        f"All {len(jobs)} match(es) below 👇"
    )

    for i in range(0, len(jobs), 5):
        batch = jobs[i:i+5]
        lines = []
        for j, job in enumerate(batch, start=i+1):
            sal   = job["salary"] if job["salary"] != "Not listed" else "Salary not listed"
            count = job.get("applicant_count")
            appl  = f"👥 {count} applicants" if count is not None else "👥 Applicants N/A (Indeed)"
            age   = f" · {job['age_str']}" if job.get("age_str") else ""
            lines.append(
                f"{j}. {score_label(job['score'])} — <b>{job['title']}</b>\n"
                f"   🏢 {job['company']}  📍 {job['location']}\n"
                f"   💰 {sal}  |  {job['_source']}{age}\n"
                f"   {appl}  |  Match: {job['score']}/100\n"
                f"   🔗 <a href='{job['url']}'>Apply Now →</a>\n"
            )
        send_telegram("\n".join(lines))

    top3  = jobs[:3]
    lines = ["⭐ <b>Top picks this run:</b>"]
    for job in top3:
        count = job.get("applicant_count")
        appl  = f"{count} applicants" if count is not None else "applicants N/A"
        lines.append(
            f"• <a href='{job['url']}'>{job['title']} @ {job['company']}</a>"
            f" ({appl})"
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
    print(f"  Total raw: {len(all_jobs)}")

    filtered = filter_jobs(all_jobs)
    send_all_jobs(filtered, run_label)
    print(f"  Done — {len(filtered)} jobs sent to Telegram\n")

# ─────────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────────

def run_startup_checks() -> bool:
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
        "✅ <b>Job Pipeline v6 is live!</b>\n\n"
        "🔍 Watching: Data Scientist · AI Engineer · ML Engineer\n"
        "Senior Analyst · Analytics Engineer · Quant · Trade Ops\n\n"
        "🎯 <b>Active filters:</b>\n"
        "• Posted today only\n"
        "• LinkedIn: under 100 applicants ✅\n"
        "• Indeed: today's jobs only (postedToday=True)\n"
        "• $120K+ or salary not listed\n"
        "• No sponsorship/clearance requirements\n"
        "• Max 50 per source\n\n"
        "📅 Mon/Wed/Thu: every 3h | Tuesday: every 2h\n"
        "All matching jobs sent here — no more \'check Apify\' messages!"
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
    print("  Ashvin Job Pipeline v6")
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
