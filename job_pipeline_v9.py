"""
=============================================================
ASHVIN'S JOB PIPELINE v9 — FIXED SALARY + CLEARANCE + INDEED
=============================================================
What's fixed:
  ✓ LinkedIn: fetches job detail page for salary + full description
    (salary lives at /jobs/view/<id> not in the search card HTML)
  ✓ Indeed: direct scrape added, sort=date, genuinely real-time
  ✓ Clearance filter: 20+ phrases covering all common wordings
  ✓ Salary parsing: handles "120K-150K", "$83/hr", "From $90,000"
  ✓ Sponsorship filter: catches "must be authorized", "eligible to
    obtain", "polygraph", "TS/SCI" and more edge cases
  ✓ JSearch: kept as optional supplement (set RAPIDAPI_KEY or skip)

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

# ─────────────────────────────────────────────
# HEADERS
# ─────────────────────────────────────────────

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
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

# ─────────────────────────────────────────────
# LINKEDIN SCRAPER
# Two-step: search cards → detail page per job
# ─────────────────────────────────────────────

LINKEDIN_SEARCH = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
LINKEDIN_DETAIL = "https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"

def fetch_linkedin_detail(job_id: str) -> dict:
    """
    Fetch full job detail from LinkedIn's public API.
    Returns: description, salary, applicantsCount
    """
    if not job_id:
        return {}
    try:
        url  = LINKEDIN_DETAIL.format(job_id=job_id)
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if not resp.ok:
            return {}
        soup = BeautifulSoup(resp.text, "html.parser")

        # Description
        desc_el = soup.find("div", class_="description__text")
        description = desc_el.get_text(separator=" ", strip=True) if desc_el else ""

        # Salary — LinkedIn puts it in criteria list or salary section
        salary = ""
        # Method 1: dedicated salary section
        sal_el = soup.find("div", class_=re.compile(r"salary|compensation", re.I))
        if sal_el:
            salary = sal_el.get_text(strip=True)
        # Method 2: criteria list items
        if not salary:
            for li in soup.find_all("li", class_="description__job-criteria-item"):
                header = li.find("h3")
                val    = li.find("span")
                if header and val:
                    h = header.get_text(strip=True).lower()
                    if "salary" in h or "compensation" in h or "pay" in h:
                        salary = val.get_text(strip=True)
                        break
        # Method 3: look for $ patterns in full text
        if not salary:
            text = soup.get_text()
            match = re.search(
                r'\$[\d,]+(?:\s*[-–]\s*\$[\d,]+)?(?:\s*(?:per|/|a)\s*(?:year|yr|hour|hr|k))?',
                text, re.I
            )
            if match:
                salary = match.group(0).strip()

        # Applicant count
        applicants = None
        appl_el = soup.find(string=re.compile(r'\d+\s+applicant', re.I))
        if appl_el:
            nums = re.findall(r'\d+', str(appl_el))
            if nums:
                applicants = int(nums[0])

        return {
            "description": description,
            "salary":      salary,
            "applicants":  applicants,
        }
    except Exception:
        return {}


def scrape_linkedin_role(keyword: str, pages: int = 2) -> list:
    jobs = []
    for page in range(pages):
        try:
            resp = requests.get(
                LINKEDIN_SEARCH,
                headers=HEADERS,
                params={
                    "keywords": keyword,
                    "location": "United States",
                    "f_TPR":    "r3600",  # last 1 hour
                    "f_E":      "1,2",    # entry + associate
                    "f_JT":     "F",      # full-time
                    "start":    page * 25,
                },
                timeout=15,
            )
            if resp.status_code == 429:
                print(f"    LinkedIn rate limited — waiting 15s")
                time.sleep(15); continue
            if not resp.ok:
                break

            soup  = BeautifulSoup(resp.text, "html.parser")

            # Try multiple selectors — LinkedIn occasionally changes class names
            cards = (soup.find_all("div", class_="base-card") or
                     soup.find_all("li", class_=re.compile(r"jobs-search-results__list-item")) or
                     soup.find_all("div", attrs={"data-entity-urn": True}))

            if not cards:
                print(f"    LinkedIn no cards found (status {resp.status_code}, chars {len(resp.text)})")
                break

            for card in cards:
                try:
                    title_el   = card.find("h3", class_="base-search-card__title")
                    company_el = card.find("h4", class_="base-search-card__subtitle")
                    loc_el     = card.find("span", class_="job-search-card__location")
                    time_el    = card.find("time")
                    link_el    = card.find("a", class_="base-card__full-link")
                    entity_urn = card.get("data-entity-urn", "")
                    job_id     = entity_urn.split(":")[-1] if entity_urn else ""

                    title   = title_el.get_text(strip=True)   if title_el   else ""
                    company = company_el.get_text(strip=True) if company_el else ""
                    loc     = loc_el.get_text(strip=True)     if loc_el     else ""
                    url     = link_el["href"].split("?")[0]   if link_el    else "#"

                    posted_utc = time_el.get("datetime", "") if time_el else ""
                    age_str    = time_el.get_text(strip=True) if time_el else ""

                    if not title:
                        continue

                    jobs.append({
                        "title":       title,
                        "company":     company,
                        "location":    loc,
                        "url":         url,
                        "posted_utc":  posted_utc,
                        "age_str":     age_str,
                        "job_id":      job_id,
                        "_source":     "LinkedIn",
                        "_keyword":    keyword,
                    })
                except Exception:
                    continue

            time.sleep(1.5)

        except Exception as e:
            print(f"    LinkedIn '{keyword}' p{page} ERROR: {e}")
            break

    return jobs


def scrape_linkedin() -> list:
    print(f"  [LinkedIn] {len(ROLES)} roles × 2 pages (last 1hr)...")
    all_jobs = []
    for role in ROLES:
        jobs = scrape_linkedin_role(role, pages=2)
        print(f"    '{role}': {len(jobs)} cards")
        all_jobs.extend(jobs)
        time.sleep(2)

    # Fetch detail pages for salary + description
    # Limit to 30 detail fetches to avoid rate limiting
    print(f"  [LinkedIn] Fetching details for up to 30 jobs...")
    for i, job in enumerate(all_jobs[:30]):
        detail = fetch_linkedin_detail(job.get("job_id", ""))
        job["description"] = detail.get("description", "")
        job["salary"]      = detail.get("salary", "Not listed") or "Not listed"
        job["applicants"]  = detail.get("applicants")
        if i % 5 == 0:
            time.sleep(1)  # polite delay every 5 requests

    # Jobs beyond 30 get empty description — will still pass filter
    for job in all_jobs[30:]:
        job.setdefault("description", "")
        job.setdefault("salary", "Not listed")
        job.setdefault("applicants", None)

    print(f"  [LinkedIn] {len(all_jobs)} total raw")
    return all_jobs


# ─────────────────────────────────────────────
# INDEED SCRAPER — direct, real-time
# sort=date means newest first, genuinely fresh
# ─────────────────────────────────────────────

INDEED_BASE = "https://www.indeed.com/jobs"

def scrape_indeed_role(query: str) -> list:
    jobs = []
    try:
        resp = requests.get(
            INDEED_BASE,
            headers=HEADERS,
            params={
                "q":       f'"{query}"',  # exact title match
                "l":       "United States",
                "sort":    "date",        # newest first
                "fromage": "1",           # posted today
                "limit":   "25",
            },
            timeout=15,
        )
        if not resp.ok:
            return []

        soup = BeautifulSoup(resp.text, "html.parser")

        # Indeed job cards
        cards = soup.find_all("div", class_=re.compile(r"job_seen_beacon|resultContent"))
        if not cards:
            # Try alternate selectors
            cards = soup.find_all("td", class_="resultContent")

        for card in cards:
            try:
                # Title
                title_el = (card.find("h2", class_=re.compile(r"jobTitle")) or
                            card.find("a", attrs={"data-jk": True}))
                title = ""
                if title_el:
                    span = title_el.find("span")
                    title = span.get_text(strip=True) if span else title_el.get_text(strip=True)

                # Company
                company_el = card.find(attrs={"data-testid": "company-name"})
                if not company_el:
                    company_el = card.find("span", class_=re.compile(r"companyName"))
                company = company_el.get_text(strip=True) if company_el else ""

                # Location
                loc_el = card.find(attrs={"data-testid": "text-location"})
                if not loc_el:
                    loc_el = card.find("div", class_=re.compile(r"companyLocation"))
                location = loc_el.get_text(strip=True) if loc_el else ""

                # Salary
                sal_el = (card.find(attrs={"data-testid": "attribute_snippet_testid"}) or
                          card.find("div", class_=re.compile(r"salary|compensation", re.I)))
                salary = sal_el.get_text(strip=True) if sal_el else "Not listed"

                # URL + job key
                link_el = card.find("a", attrs={"data-jk": True})
                if not link_el:
                    link_el = card.find("a", class_=re.compile(r"jcs-JobTitle"))
                job_key = link_el.get("data-jk", "") if link_el else ""
                url     = f"https://www.indeed.com/viewjob?jk={job_key}" if job_key else "#"

                # Age string
                age_el  = card.find("span", class_=re.compile(r"date|posted", re.I))
                age_str = age_el.get_text(strip=True) if age_el else ""

                if not title or not company:
                    continue

                jobs.append({
                    "title":       title,
                    "company":     company,
                    "location":    location,
                    "salary":      salary,
                    "url":         url,
                    "description": "",   # would need detail page — skip for now
                    "posted_utc":  "",
                    "age_str":     age_str,
                    "applicants":  None,
                    "_source":     "Indeed",
                    "_keyword":    query,
                })
            except Exception:
                continue

    except Exception as e:
        print(f"    Indeed '{query}' ERROR: {e}")

    return jobs


def scrape_indeed() -> list:
    print(f"  [Indeed] {len(ROLES)} roles (direct scrape, sort=date)...")
    all_jobs = []
    for role in ROLES:
        jobs = scrape_indeed_role(role)
        print(f"    '{role}': {len(jobs)} results")
        all_jobs.extend(jobs)
        time.sleep(2)  # polite delay
    print(f"  [Indeed] {len(all_jobs)} total raw")
    return all_jobs


# ─────────────────────────────────────────────
# NORMALIZER
# ─────────────────────────────────────────────

def normalize(job: dict) -> dict:
    """Ensure all fields exist and are strings, never None."""
    return {
        "title":       (job.get("title")       or "Unknown Role").strip(),
        "company":     (job.get("company")     or "Unknown Company").strip(),
        "location":    (job.get("location")    or "Unknown Location").strip(),
        "salary":      (job.get("salary")      or "Not listed").strip(),
        "url":         (job.get("url")         or "#").strip(),
        "description": (job.get("description") or "").strip(),
        "posted_utc":  (job.get("posted_utc")  or ""),
        "age_str":     (job.get("age_str")     or ""),
        "applicants":  job.get("applicants"),
        "_source":     job.get("_source", ""),
        "_keyword":    job.get("_keyword", ""),
    }


# ─────────────────────────────────────────────
# SALARY PARSER
# Extracts min salary as integer for comparison
# ─────────────────────────────────────────────

def parse_salary_min(salary_str: str) -> int:
    """
    Returns the minimum salary as an integer.
    Handles: $120K, $120,000, $83/hr, 120K-150K, From $90,000/year
    Returns 0 if unparseable.
    """
    if not salary_str or salary_str.lower() == "not listed":
        return 0

    txt = salary_str.lower().replace(",", "")

    # Hourly → annualize (×2080)
    hourly = re.search(r'\$?([\d.]+)\s*(?:/\s*hr|per hour|/hour|an hour)', txt)
    if hourly:
        return int(float(hourly.group(1)) * 2080)

    # Find all dollar amounts
    amounts = re.findall(r'\$?([\d.]+)\s*k?', txt)
    if not amounts:
        return 0

    values = []
    for a in amounts:
        try:
            v = float(a)
            # "120k" style
            if "k" in txt[txt.find(a):txt.find(a)+len(a)+1]:
                v *= 1000
            # Raw number — if < 1000 assume it's thousands (e.g. "120" = $120K)
            elif v < 1000:
                v *= 1000
            values.append(int(v))
        except ValueError:
            pass

    return min(values) if values else 0


# ─────────────────────────────────────────────
# FILTERS — comprehensive clearance + sponsorship
# ─────────────────────────────────────────────

# All known ways companies phrase clearance/sponsorship requirements
DISQUALIFY_PHRASES = [
    # Sponsorship — explicit
    "sponsorship not available",
    "we do not offer sponsorship",
    "not able to provide sponsorship",
    "unable to provide visa sponsorship",
    "unable to sponsor",
    "will not sponsor",
    "cannot sponsor",
    "does not sponsor",
    "no visa sponsorship",
    "visa sponsorship is not",
    "not providing sponsorship",
    "sponsorship is not available",
    "we are unable to sponsor",
    # Work authorization — phrased as restriction
    "must be authorized to work in the united states without",
    "must be authorized to work in the us without",
    "authorized to work in the u.s. without sponsorship",
    "work authorization will not be sponsored",
    "employment sponsorship will not be provided",
    "no employment visa sponsorship",
    # Citizenship requirements
    "must be a u.s. citizen",
    "must be a us citizen",
    "must be united states citizen",
    "u.s. citizenship required",
    "us citizenship required",
    "united states citizenship is required",
    "requires u.s. citizenship",
    "requires us citizenship",
    # Green card
    "green card required",
    "gc required",
    "permanent resident required",
    "must be a permanent resident",
    # Security clearance
    "active security clearance",
    "security clearance required",
    "clearance required",
    "must have a clearance",
    "must hold a clearance",
    "must possess a clearance",
    "obtain a security clearance",
    "eligible to obtain a clearance",
    "secret clearance",
    "top secret",
    "ts/sci",
    "ts / sci",
    "sci clearance",
    "dod clearance",
    "public trust clearance",
    "polygraph",
    # H1B specific
    "no h-1b",
    "no h1b",
    "h1b transfers only",
    "not able to transfer h1b",
]

BAD_TITLE_WORDS = [
    "cashier","sales representative","busser","waiter","driver","warehouse",
    "nurse","teacher","mechanic","customer service","call center","receptionist",
    "firmware engineer","hardware engineer","production manager","construction",
    "now hiring","intern","internship","co-op",
]


def job_hash(job: dict) -> str:
    key = job["title"].lower().strip() + job["company"].lower().strip()
    return hashlib.md5(key.encode()).hexdigest()


def is_fresh(job: dict) -> bool:
    # LinkedIn ISO timestamp
    utc = job.get("posted_utc", "")
    if utc:
        try:
            posted = datetime.fromisoformat(utc.replace("Z", "+00:00"))
            delta  = datetime.now(timezone.utc) - posted
            return delta.total_seconds() <= (6 * 3600)  # within 6hrs
        except Exception:
            pass

    # Indeed age string: "Posted 2 hours ago", "Just posted", "Today"
    age = job.get("age_str", "").lower()
    if age:
        if any(w in age for w in ["just posted", "today", "active today"]):
            return True
        if "minute" in age:
            return True
        if "hour" in age:
            nums = re.findall(r'\d+', age)
            return int(nums[0]) <= 6 if nums else True
        if "day" in age:
            nums = re.findall(r'\d+', age)
            return int(nums[0]) <= 1 if nums else False

    return True  # no timestamp → include


def is_bad_title(job: dict) -> bool:
    return any(kw in job["title"].lower() for kw in BAD_TITLE_WORDS)


def is_disqualified(job: dict) -> bool:
    # Check both description AND title for clearance signals
    combined = (job["description"] + " " + job["title"] + " " + job["company"]).lower()
    return any(p in combined for p in DISQUALIFY_PHRASES)


def has_target_salary(job: dict) -> bool:
    """
    Pass if:
    - Salary not listed (include — filter manually)
    - Salary listed AND min >= $120K
    - Hourly rate that annualizes to >= $120K ($57.69/hr+)
    Block if salary is listed but clearly below $120K.
    """
    sal = job["salary"]
    if not sal or sal.lower() == "not listed":
        return True

    min_sal = parse_salary_min(sal)
    if min_sal == 0:
        return True   # couldn't parse → include
    return min_sal >= 120000


def score_job(job: dict) -> int:
    score = 0
    text  = (job["title"] + " " + job["description"]).lower()

    for skill, pts in {
        "python":15, "sql":10, "machine learning":12, "power bi":8,
        "data science":10, "azure":8, "llm":12, "rag":10,
        "databricks":8, "tensorflow":8, "tableau":6, "pandas":5,
        "statistical":8, "etl":6, "forecasting":7, "nlp":8,
        "deep learning":8, "scikit":7, "spark":7, "snowflake":6,
        "pytorch":8, "mlflow":6, "docker":5, "airflow":6,
    }.items():
        if skill in text: score += pts

    # Title match bonus
    for t in ["data scientist", "ai engineer", "ml engineer", "machine learning",
              "analytics engineer", "applied scientist", "quantitative"]:
        if t in job["title"].lower(): score += 15

    # Experience level
    for p in ["1-3 years","2+ years","entry level","junior",
              "0-2 years","new grad","associate","early career"]:
        if p in text: score += 10
    for p in ["3-5 years","3+ years"]:
        if p in text: score += 3
    for p in ["10+ years","8+ years","7+ years","director",
              "principal","vp of","head of","staff engineer"]:
        if p in text: score -= 20

    # Finance/quant bonus (algo trading background)
    for p in ["quant","trading","risk","portfolio","hedge fund",
              "bloomberg","sharpe","derivatives","options","futures"]:
        if p in text: score += 8

    # Salary bonus — reward jobs that explicitly show $120K+
    if parse_salary_min(job["salary"]) >= 120000:
        score += 10

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

    # Cross-source dedup
    seen_keys, deduped = set(), []
    for job in passed:
        key = job["title"].lower()[:35] + job["company"].lower()[:25]
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


def score_label(s: int) -> str:
    return "🟢 HIGH" if s >= 70 else "🟡 MED" if s >= 45 else "🔵 LOW"


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
        f"📡 LinkedIn (last 1hr) + Indeed (today)\n"
        f"All {len(jobs)} match(es) 👇"
    )

    for i in range(0, len(jobs), 5):
        lines = []
        for j, job in enumerate(jobs[i:i+5], start=i+1):
            sal  = job["salary"] if job["salary"] != "Not listed" else "Salary not listed"
            age  = f" · {job['age_str']}" if job.get("age_str") else ""
            appl = f" · 👥 {job['applicants']}" if job.get("applicants") else ""
            lines.append(
                f"{j}. {score_label(job['score'])} — <b>{job['title']}</b>\n"
                f"   🏢 {job['company']}  📍 {job['location']}\n"
                f"   💰 {sal}{age}{appl}  |  {job['_source']}\n"
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

    day_names = ["Monday","Tuesday","Wednesday","Thursday"]
    run_label = f"{day_names[weekday]} {now_et.strftime('%I:%M %p ET')}"
    print(f"\n{'='*52}\n  Run: {run_label}\n{'='*52}")

    raw = scrape_linkedin() + scrape_indeed()
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
    print("  ✅ Env vars OK")

    ok = send_telegram(
        "✅ <b>Job Pipeline v9 is live!</b>\n\n"
        "📡 <b>Sources:</b> LinkedIn (last 1hr) + Indeed (today)\n"
        "💰 <b>Salary:</b> Parsed properly — $120K+ filter active\n"
        "🚫 <b>Clearance filter:</b> 30+ phrases (TS/SCI, polygraph, etc)\n"
        "🚫 <b>Sponsorship filter:</b> 15+ phrases\n\n"
        "📅 Mon/Wed/Thu: 3h | Tuesday: 2h"
    )
    if not ok:
        print("  ❌ Telegram failed"); return False
    print("  ✅ Telegram OK")
    print("--- Ready ---\n")
    return True


# ─────────────────────────────────────────────
# SCHEDULER
# ─────────────────────────────────────────────

if __name__ == "__main__":
    print("="*52)
    print("  Ashvin Job Pipeline v9")
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
