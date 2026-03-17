"""
=============================================================
ASHVIN'S JOB PIPELINE v8 — DIRECT LINKEDIN + JSEARCH
=============================================================
Scraping strategy:
  1. LinkedIn public API (no key, no login, free, unlimited)
     → linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search
     → Returns 25 per page, paginate for more
     → f_TPR=r3600 = posted in last 1 HOUR (not 24hrs)
     → Real timestamps on every job

  2. JSearch (RapidAPI free tier, 200/month)
     → Used as supplement only (5 queries on Tue, 3 other days)
     → Covers Indeed/Glassdoor/ZipRecruiter that LinkedIn misses

This means every run gets genuinely fresh jobs posted in the
last hour — not the last 24 hours.

pip install requests apscheduler beautifulsoup4
=============================================================
"""

import os
import re
import time
import hashlib
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from bs4 import BeautifulSoup
import requests

RAPIDAPI_KEY       = os.environ.get("RAPIDAPI_KEY", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")

ET                   = ZoneInfo("America/New_York")
SEEN_JOB_HASHES: set = set()

LINKEDIN_BASE = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
JSEARCH_URL   = "https://jsearch.p.rapidapi.com/search"
JSEARCH_HEADERS = {
    "X-RapidAPI-Key":  RAPIDAPI_KEY,
    "X-RapidAPI-Host": "jsearch.p.rapidapi.com",
}

# LinkedIn headers to avoid bot detection
LI_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# ─────────────────────────────────────────────
# ROLES
# ─────────────────────────────────────────────

ROLES = [
    "Data Scientist",
    "AI Engineer",
    "Machine Learning Engineer",
    "Senior Data Analyst",
    "Analytics Engineer",
    "Applied Scientist",
    "Quantitative Analyst",
    "Trade Operations Analyst",
]

# JSearch supplement — fewer queries to save free tier
JSEARCH_ROLES_TUESDAY = ["Data Scientist", "AI Engineer", "Machine Learning Engineer",
                          "Analytics Engineer", "Quantitative Analyst"]
JSEARCH_ROLES_OTHER   = ["Data Scientist", "AI Engineer", "Machine Learning Engineer"]

# ─────────────────────────────────────────────
# LINKEDIN SCRAPER — public API, no auth needed
# ─────────────────────────────────────────────

def scrape_linkedin_role(keyword: str, pages: int = 2) -> list:
    """
    Scrape LinkedIn public job search API.
    pages=2 → 50 results per role (25 per page).
    f_TPR=r3600 → posted in last 1 hour only.
    f_E=1,2 → Entry + Associate level.
    """
    jobs = []
    for page in range(pages):
        try:
            params = {
                "keywords":  keyword,
                "location":  "United States",
                "f_TPR":     "r11800",   # last 1 HOUR — key change from r86400
                "f_E":       "1,2",    # entry + associate
                "f_JT":      "F",      # full time
                "start":     page * 25,
            }
            resp = requests.get(
                LINKEDIN_BASE,
                headers=LI_HEADERS,
                params=params,
                timeout=15,
            )
            if resp.status_code == 429:
                print(f"    LinkedIn rate limited — waiting 10s")
                time.sleep(10)
                continue
            if not resp.ok:
                print(f"    LinkedIn {keyword} p{page}: {resp.status_code}")
                break

            soup  = BeautifulSoup(resp.text, "html.parser")
            cards = soup.find_all("div", class_="base-card")

            if not cards:
                break  # no more results

            for card in cards:
                try:
                    # Extract fields from card HTML
                    title_el   = card.find("h3", class_="base-search-card__title")
                    company_el = card.find("h4", class_="base-search-card__subtitle")
                    loc_el     = card.find("span", class_="job-search-card__location")
                    time_el    = card.find("time")
                    link_el    = card.find("a", class_="base-card__full-link")

                    title   = title_el.get_text(strip=True)   if title_el   else ""
                    company = company_el.get_text(strip=True) if company_el else ""
                    loc     = loc_el.get_text(strip=True)     if loc_el     else ""
                    url     = link_el["href"]                 if link_el    else "#"

                    # Timestamp from <time datetime="2026-03-17T18:30:00.000Z">
                    posted_utc = ""
                    age_str    = ""
                    if time_el:
                        posted_utc = time_el.get("datetime", "")
                        age_str    = time_el.get_text(strip=True)

                    # Job ID for detail fetch
                    entity_urn = card.get("data-entity-urn", "")
                    job_id     = entity_urn.split(":")[-1] if entity_urn else ""

                    jobs.append({
                        "title":       title,
                        "company":     company,
                        "location":    loc,
                        "salary":      "Not listed",  # not in card — detail fetch needed
                        "url":         url,
                        "description": "",            # fetched separately if needed
                        "posted_utc":  posted_utc,
                        "age_str":     age_str,
                        "job_id":      job_id,
                        "_source":     "LinkedIn",
                    })
                except Exception:
                    continue

            time.sleep(1)  # polite delay between pages

        except Exception as e:
            print(f"    LinkedIn '{keyword}' p{page} ERROR: {e}")
            break

    return jobs


def scrape_linkedin() -> list:
    print(f"  [LinkedIn] Scraping {len(ROLES)} roles (last 1 hour, 2 pages each)...")
    all_jobs = []
    for role in ROLES:
        jobs = scrape_linkedin_role(role, pages=2)
        print(f"    '{role}': {len(jobs)} results")
        all_jobs.extend(jobs)
        time.sleep(2)  # delay between roles to avoid rate limiting
    print(f"  [LinkedIn] {len(all_jobs)} total raw")
    return all_jobs


# ─────────────────────────────────────────────
# JSEARCH SCRAPER — supplement for non-LinkedIn sources
# ─────────────────────────────────────────────

def scrape_jsearch(is_tuesday: bool) -> list:
    if not RAPIDAPI_KEY:
        return []

    roles    = JSEARCH_ROLES_TUESDAY if is_tuesday else JSEARCH_ROLES_OTHER
    all_jobs = []

    print(f"  [JSearch] {len(roles)} queries (today only, supplement)...")
    for query in roles:
        try:
            resp = requests.get(
                JSEARCH_URL,
                headers=JSEARCH_HEADERS,
                params={
                    "query":            f"{query} in United States",
                    "page":             "1",
                    "num_pages":        "1",
                    "date_posted":      "today",
                    "employment_types": "FULLTIME",
                },
                timeout=20,
            )
            if not resp.ok:
                print(f"    '{query}' ERROR: {resp.status_code}")
                continue
            jobs = resp.json().get("data", [])
            for j in jobs:
                j["_source"] = "Google Jobs"
            all_jobs.extend(jobs)
            print(f"    '{query}': {len(jobs)} results")
        except Exception as e:
            print(f"    '{query}' ERROR: {e}")

    print(f"  [JSearch] {len(all_jobs)} total")
    return all_jobs


# ─────────────────────────────────────────────
# NORMALIZER — handles both LinkedIn and JSearch
# ─────────────────────────────────────────────

def normalize(job: dict) -> dict:
    source = job.get("_source", "")

    if source == "LinkedIn":
        return {
            "title":       job.get("title", "Unknown Role"),
            "company":     job.get("company", "Unknown Company"),
            "location":    job.get("location", "Unknown Location"),
            "salary":      "Not listed",
            "url":         job.get("url", "#"),
            "description": job.get("description", ""),
            "posted_utc":  job.get("posted_utc", ""),
            "age_str":     job.get("age_str", ""),
            "posted_ts":   None,
            "_source":     "LinkedIn",
            "_via":        "LinkedIn",
        }
    else:
        # JSearch fields
        city  = job.get("job_city", "") or ""
        state = job.get("job_state", "") or ""
        loc   = f"{city}, {state}" if city and state else city or state or "United States"
        if job.get("job_is_remote"): loc = f"Remote ({loc})" if loc else "Remote"

        s_min    = job.get("job_min_salary")
        s_max    = job.get("job_max_salary")
        s_period = (job.get("job_salary_period") or "year").lower()
        if s_min and s_max:
            salary = f"${int(s_min):,} – ${int(s_max):,}/{s_period}"
        elif s_min:
            salary = f"From ${int(s_min):,}/{s_period}"
        else:
            salary = "Not listed"

        posted_ts = job.get("job_posted_at_timestamp")
        age_str   = ""
        if posted_ts:
            try:
                delta = datetime.now(timezone.utc) - datetime.fromtimestamp(int(posted_ts), tz=timezone.utc)
                hrs   = delta.total_seconds() / 3600
                age_str = f"{int(delta.total_seconds()/60)}m ago" if hrs < 1 else f"{int(hrs)}h ago"
            except Exception:
                pass

        return {
            "title":       job.get("job_title", "Unknown Role"),
            "company":     job.get("employer_name", "Unknown Company"),
            "location":    loc,
            "salary":      salary,
            "url":         job.get("job_apply_link", job.get("job_google_link", "#")),
            "description": job.get("job_description", "") or "",
            "posted_utc":  job.get("job_posted_at_datetime_utc", ""),
            "age_str":     age_str,
            "posted_ts":   posted_ts,
            "_source":     "Google Jobs",
            "_via":        job.get("job_publisher", ""),
        }


# ─────────────────────────────────────────────
# FRESHNESS — LinkedIn uses real datetime stamps
# ─────────────────────────────────────────────

def is_fresh(job: dict) -> bool:
    # LinkedIn: posted_utc is ISO string like "2026-03-17T18:30:00.000Z"
    utc = job.get("posted_utc", "")
    if utc:
        try:
            posted = datetime.fromisoformat(utc.replace("Z", "+00:00"))
            delta  = datetime.now(timezone.utc) - posted
            # LinkedIn already filtered by f_TPR=r3600 (1hr) on the API side
            # but double-check here — keep jobs posted within last 4 hours
            return delta.total_seconds() <= (4 * 3600)
        except Exception:
            pass

    # JSearch: unix timestamp
    ts = job.get("posted_ts")
    if ts:
        try:
            delta = datetime.now(timezone.utc) - datetime.fromtimestamp(int(ts), tz=timezone.utc)
            return delta.total_seconds() <= (24 * 3600)  # JSearch is less precise
        except Exception:
            pass

    return True  # unknown → include


# ─────────────────────────────────────────────
# FILTERS
# ─────────────────────────────────────────────

DISQUALIFY_PHRASES = [
    "sponsorship not available","we do not offer sponsorship","no visa sponsorship",
    "unable to sponsor","will not sponsor","cannot sponsor","not able to sponsor",
    "must be a u.s. citizen","must be a us citizen","u.s. citizenship required",
    "us citizenship required","active security clearance","security clearance required",
    "secret clearance","top secret","green card required","gc required",
    "work authorization will not","no h-1b","no h1b",
]

BAD_TITLE_WORDS = [
    "cashier","sales representative","busser","waiter","driver","warehouse",
    "nurse","teacher","mechanic","customer service","call center","receptionist",
    "firmware engineer","hardware engineer","production manager","construction","now hiring",
]

SALARY_KEYWORDS = [
    "$120k","$125k","$130k","$140k","$150k","$160k","$170k","$180k","$190k","$200k",
    "120,000","125,000","130,000","140,000","150,000","160,000","170,000","180,000",
]


def job_hash(job: dict) -> str:
    key = (job["title"] or "").lower().strip() + (job["company"] or "").lower().strip()
    return hashlib.md5(key.encode()).hexdigest()


def is_bad_title(job: dict) -> bool:
    return any(kw in (job["title"] or "").lower() for kw in BAD_TITLE_WORDS)


def is_disqualified(job: dict) -> bool:
    return any(p in (job["description"] or "").lower() for p in DISQUALIFY_PHRASES)


def has_target_salary(job: dict) -> bool:
    txt = (job["salary"] or "").lower()
    if not txt or txt == "not listed": return True
    if any(kw.lower() in txt for kw in SALARY_KEYWORDS): return True
    for n in re.findall(r'\$?([\d,]+)', txt):
        try:
            v = int(n.replace(",",""))
            if v >= 120000 or 120 <= v <= 999: return True
        except ValueError: pass
    return False


def score_job(job: dict) -> int:
    score = 0
    text  = ((job["title"] or "") + " " + (job["description"] or "")).lower()
    for skill, pts in {
        "python":15,"sql":10,"machine learning":12,"power bi":8,"data science":10,
        "azure":8,"llm":12,"rag":10,"databricks":8,"tensorflow":8,"tableau":6,
        "pandas":5,"statistical":8,"etl":6,"forecasting":7,"nlp":8,
        "deep learning":8,"scikit":7,"spark":7,"snowflake":6,
    }.items():
        if skill in text: score += pts
    for t in ["data scientist","ai engineer","ml engineer","machine learning",
              "analytics engineer","applied scientist","quantitative"]:
        if t in (job["title"] or "").lower(): score += 15
    for p in ["1-3 years","2+ years","entry level","junior","0-2 years","new grad","associate"]:
        if p in text: score += 10
    for p in ["3-5 years","3+ years","senior"]:
        if p in text: score += 3
    for p in ["10+ years","8+ years","director","principal","vp of","staff engineer"]:
        if p in text: score -= 20
    for p in ["quant","trading","risk","portfolio","hedge fund","bloomberg","sharpe"]:
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
        key = (job["title"] or "")[:30].lower() + (job["company"] or "")[:20].lower()
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
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text,
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=20,
        )
        return r.ok
    except Exception as e:
        print(f"  [Telegram ERROR] {e}"); return False


def score_label(s): return "🟢 HIGH" if s>=70 else "🟡 MED" if s>=45 else "🔵 LOW"


def send_all_jobs(jobs: list, run_label: str):
    if not jobs:
        send_telegram(
            f"✅ <b>Pipeline ran — {run_label}</b>\n"
            f"🕐 {datetime.now(ET).strftime('%a %b %d, %I:%M %p ET')}\n"
            f"No new jobs posted in the last hour matching your profile."
        ); return

    send_telegram(
        f"🚨 <b>{len(jobs)} Fresh Job(s) — {run_label}</b>\n"
        f"🕐 {datetime.now(ET).strftime('%a %b %d, %I:%M %p ET')}\n"
        f"⏱ Posted within last 1 hour (LinkedIn) / today (Google)\n"
        f"All {len(jobs)} below 👇"
    )
    for i in range(0, len(jobs), 5):
        lines = []
        for j, job in enumerate(jobs[i:i+5], start=i+1):
            sal = job["salary"] if job["salary"] != "Not listed" else "Salary not listed"
            age = f" · {job['age_str']}" if job.get("age_str") else ""
            via = f" · {job['_via']}" if job.get("_via") and job["_via"] != job["_source"] else ""
            lines.append(
                f"{j}. {score_label(job['score'])} — <b>{job['title']}</b>\n"
                f"   🏢 {job['company']}  📍 {job['location']}\n"
                f"   💰 {sal}{age}  |  {job['_source']}{via}\n"
                f"   📊 Match: {job['score']}/100\n"
                f"   🔗 <a href='{job['url']}'>Apply Now →</a>\n"
            )
        send_telegram("\n".join(lines))

    lines = ["⭐ <b>Top picks:</b>"]
    for job in jobs[:3]:
        lines.append(f"• <a href='{job['url']}'>{job['title']} @ {job['company']}</a>")
    lines.append("\n🎯 Apply within the hour — first wave!")
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

    raw = scrape_linkedin() + scrape_jsearch(is_tuesday)
    print(f"  Total raw: {len(raw)}")

    filtered = filter_jobs(raw)
    send_all_jobs(filtered, run_label)
    print(f"  Done — {len(filtered)} jobs sent\n")


# ─────────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────────

def run_startup_checks() -> bool:
    print("\n--- Startup checks ---")
    missing = [k for k, v in {
        "TELEGRAM_BOT_TOKEN": TELEGRAM_BOT_TOKEN,
        "TELEGRAM_CHAT_ID":   TELEGRAM_CHAT_ID,
    }.items() if not v]
    if missing:
        print(f"  ❌ Missing: {', '.join(missing)}"); return False
    if not RAPIDAPI_KEY:
        print("  ⚠️  RAPIDAPI_KEY not set — JSearch supplement disabled, LinkedIn only")
    print("  ✅ All env vars set")

    ok = send_telegram(
        "✅ <b>Job Pipeline v8 is live!</b>\n\n"
        "🔍 <b>Sources:</b>\n"
        "• LinkedIn (direct scrape, last 1 HOUR)\n"
        "• Google Jobs/JSearch (today, supplement)\n\n"
        "🎯 <b>Filters:</b> $120K+ · No sponsorship · Fresh only\n\n"
        "📅 Mon/Wed/Thu: every 3h | Tuesday: every 2h"
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
    print("  Ashvin Job Pipeline v8")
    print(f"  {datetime.now(ET).strftime('%A %B %d, %I:%M %p ET')}")
    print("="*52)

    if not run_startup_checks(): exit(1)

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
