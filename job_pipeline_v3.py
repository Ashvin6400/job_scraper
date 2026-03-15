"""
=============================================================
ASHVIN'S JOB PIPELINE v3 — 100% FREE NOTIFICATIONS
=============================================================
Removed: Zapier webhooks (premium)
Added  : 4 free notification options — pick any combo

  Option A: Telegram Bot       ← already set up, RECOMMENDED
  Option B: Gmail SMTP         ← free, good for daily digest
  Option C: Google Sheets      ← free, best for tracking
  Option D: ntfy.sh            ← free push notif, no account needed

Schedule:
  Mon / Wed / Thu  →  every 3 hours  (7am–9pm ET)
  Tuesday          →  every 2 hours  (7am–9pm ET)  ← peak day
  Fri / Sat / Sun  →  OFF

Freshness: only jobs posted within last 2 hours trigger alerts.

pip install requests apscheduler gspread google-auth
=============================================================
"""

import requests
import re
import hashlib
import smtplib
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

# ─────────────────────────────────────────────────────────
# STEP 1 — APIFY (required)
# ─────────────────────────────────────────────────────────
APIFY_API_TOKEN = "apify_api_eFuJZLHLfTUk38IS5A7OjWfcfsmck3256qEw"

# ─────────────────────────────────────────────────────────
# STEP 2 — CHOOSE YOUR FREE NOTIFICATION METHODS
# Set to True to enable, False to skip
# ─────────────────────────────────────────────────────────

# ── Option A: Telegram (RECOMMENDED — instant phone push) ─
USE_TELEGRAM       = True
TELEGRAM_BOT_TOKEN = "8642509436:AAEoJDfs23Eao8xmw-GSRsr8otCxlb6Y5pY"
TELEGRAM_CHAT_ID   = "5817941574"
# Setup: @BotFather → /newbot → copy token
# Get chat ID: https://api.telegram.org/bot<TOKEN>/getUpdates

# ── Option B: Gmail SMTP (free — good for EOD digest) ─────
USE_GMAIL          = False
GMAIL_SENDER       = "your.email@gmail.com"
GMAIL_APP_PASSWORD = "YOUR_APP_PASSWORD"   # NOT your real password!
GMAIL_RECIPIENT    = "your.email@gmail.com"
# Setup: Google Account → Security → 2FA on → App Passwords
# Create an App Password (select "Mail" + "Other")
# Paste the 16-char code above — works without Zapier

# ── Option C: Google Sheets (free — best job tracker) ─────
USE_GOOGLE_SHEETS     = False
GOOGLE_SHEETS_ID      = "YOUR_SPREADSHEET_ID"   # from the URL: /d/XXXXX/edit
GOOGLE_SERVICE_ACCOUNT_JSON = "service_account.json"  # download from GCP
# Setup guide at bottom of file

# ── Option D: ntfy.sh (free — no account, instant push) ───
USE_NTFY           = False
NTFY_TOPIC         = "ashvin-jobs-abc123"   # make this unique (anyone who knows it can subscribe)
# Setup: install ntfy app on phone → subscribe to your topic name
# Zero config — just set USE_NTFY = True and pick a unique topic name

# ─────────────────────────────────────────────────────────
# SETTINGS
# ─────────────────────────────────────────────────────────
ET                       = ZoneInfo("America/New_York")
FRESHNESS_WINDOW_MINUTES = 120
LINKEDIN_ACTOR           = "curious_coder/linkedin-jobs-scraper"
INDEED_ACTOR             = "valig/indeed-jobs-scraper"
SEEN_JOB_HASHES: set     = set()

# ─────────────────────────────────────────────────────────
# TARGET ROLES
# ─────────────────────────────────────────────────────────
SEARCH_QUERIES = [
    "Data Scientist",
    "AI Engineer",
    "Machine Learning Engineer",
    "Applied Scientist",
    "Senior Data Analyst",
    "Analytics Engineer",
    "Business Intelligence Engineer",
    "Quantitative Analyst",
    "Trade Operations Analyst",
    "Junior Quant",
    "Quantitative Research Analyst",
]
LOCATIONS = ["United States", "Remote"]

# ─────────────────────────────────────────────────────────
# DISQUALIFY LIST
# ─────────────────────────────────────────────────────────
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
    "$120k","$125k","$130k","$135k","$140k","$150k","$160k","$170k","$180k","$190k","$200k",
    "120,000","125,000","130,000","135,000","140,000","150,000",
    "160,000","170,000","180,000","190,000","200,000",
]

# ─────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────
def job_hash(job: dict) -> str:
    key = (
        str(job.get("title","")).lower().strip() +
        str(job.get("companyName", job.get("company",""))).lower().strip()
    )
    return hashlib.md5(key.encode()).hexdigest()

def is_fresh(job: dict) -> bool:
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=FRESHNESS_WINDOW_MINUTES)
    for field in ["postedAt","datePosted","publishedAt","listingDate","date","jobPostedAt"]:
        raw = job.get(field)
        if not raw:
            continue
        try:
            posted = (
                datetime.fromtimestamp(raw, tz=timezone.utc)
                if isinstance(raw, (int, float))
                else datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            )
            return posted >= cutoff
        except Exception:
            continue
    return True   # no timestamp → include (don't miss real new jobs)

def is_disqualified(job: dict) -> bool:
    desc = (job.get("description","") or "").lower()
    return any(p in desc for p in DISQUALIFY_PHRASES)

def has_target_salary(job: dict) -> bool:
    txt = (str(job.get("salary","") or "") + str(job.get("compensation","") or "")).lower()
    if not txt.strip():
        return True
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
    text  = (str(job.get("title","") or "") + " " + str(job.get("description","") or "")).lower()
    for skill, pts in {
        "python":15,"sql":10,"machine learning":12,"power bi":8,"data science":10,
        "azure":8,"llm":12,"rag":10,"databricks":8,"tensorflow":8,"tableau":6,
        "pandas":5,"numpy":5,"statistical":8,"etl":6,"forecasting":7,"nlp":8,"ai":5,
    }.items():
        if skill in text: score += pts
    for p in ["1-3 years","2+ years","entry level","associate","junior","0-2 years","new grad"]:
        if p in text: score += 10
    for p in ["3-5 years","senior","3+ years"]:
        if p in text: score += 3
    for p in ["10+ years","8+ years","director","principal staff"]:
        if p in text: score -= 15
    for p in ["quant","trading","risk","portfolio","hedge fund","bloomberg","sharpe"]:
        if p in text: score += 8
    return min(100, max(0, score))

def filter_jobs(raw: list) -> list:
    out = []
    for job in raw:
        h = job_hash(job)
        if h in SEEN_JOB_HASHES or not is_fresh(job) or is_disqualified(job) or not has_target_salary(job):
            continue
        job["relevance_score"] = score_job(job)
        SEEN_JOB_HASHES.add(h)
        out.append(job)
    return sorted(out, key=lambda j: j.get("relevance_score",0), reverse=True)

# ─────────────────────────────────────────────────────────
# SCRAPERS
# ─────────────────────────────────────────────────────────
def scrape_linkedin(query: str, location: str) -> list:
    try:
        r = requests.post(
            f"https://api.apify.com/v2/acts/{LINKEDIN_ACTOR}/run-sync-get-dataset-items",
            params={"token": APIFY_API_TOKEN},
            json={"searchTerms":[query],"location":location,"datePosted":"past24Hours","remoteFilter":"2","limit":25},
            timeout=120,
        )
        data = r.json() if r.ok else []
        for j in data: j["_source"] = "LinkedIn"
        return data
    except Exception as e:
        print(f"  [LinkedIn] {e}")
        return []

def scrape_indeed(query: str, location: str) -> list:
    try:
        r = requests.post(
            f"https://api.apify.com/v2/acts/{INDEED_ACTOR}/run-sync-get-dataset-items",
            params={"token": APIFY_API_TOKEN},
            json={"position":query,"country":"US","location":location,"maxAge":1,"maxItems":25},
            timeout=120,
        )
        data = r.json() if r.ok else []
        for j in data: j["_source"] = "Indeed"
        return data
    except Exception as e:
        print(f"  [Indeed] {e}")
        return []

# ─────────────────────────────────────────────────────────
# NOTIFICATION — A: TELEGRAM (instant phone push)
# ─────────────────────────────────────────────────────────
def notify_telegram(jobs: list, run_label: str):
    if not USE_TELEGRAM or not jobs:
        return

    def send(text):
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text,
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=15,
        )

    send(
        f"🚨 <b>{len(jobs)} New Job(s) — {run_label}</b>\n"
        f"🕐 {datetime.now(ET).strftime('%a %b %d, %I:%M %p ET')}\n"
        f"{'─'*28}"
    )
    for job in jobs[:8]:
        score  = job.get("relevance_score", 0)
        icon   = "🟢" if score >= 70 else "🟡" if score >= 45 else "🔵"
        url    = job.get("externalUrl", job.get("url", job.get("jobUrl","#")))
        send(
            f"{icon} <b>{job.get('title','?')}</b>\n"
            f"🏢 {job.get('companyName', job.get('company','?'))}\n"
            f"📍 {job.get('location','?')}\n"
            f"💰 {job.get('salary', job.get('compensation','Not listed'))}\n"
            f"📊 Match: {score}/100  |  {job.get('_source','')}\n"
            f"🔗 <a href='{url}'>Apply Now →</a>"
        )
    if len(jobs) > 8:
        send(f"⚠️ +{len(jobs)-8} more — check Apify dashboard.")

# ─────────────────────────────────────────────────────────
# NOTIFICATION — B: GMAIL SMTP (free, no Zapier)
# ─────────────────────────────────────────────────────────
def notify_gmail(jobs: list, run_label: str):
    if not USE_GMAIL or not jobs:
        return

    rows = ""
    for job in jobs:
        score = job.get("relevance_score", 0)
        color = "#1D9E75" if score >= 70 else "#BA7517" if score >= 45 else "#378ADD"
        url   = job.get("externalUrl", job.get("url", job.get("jobUrl","#")))
        rows += f"""
        <tr>
          <td style="padding:10px;border-bottom:1px solid #eee">
            <b>{job.get('title','?')}</b><br>
            <span style="color:#666">{job.get('companyName', job.get('company','?'))} · {job.get('location','?')}</span>
          </td>
          <td style="padding:10px;border-bottom:1px solid #eee;color:#666">
            {job.get('salary', job.get('compensation','Not listed'))}
          </td>
          <td style="padding:10px;border-bottom:1px solid #eee;color:{color};font-weight:bold">
            {score}/100
          </td>
          <td style="padding:10px;border-bottom:1px solid #eee">
            <a href="{url}" style="color:#185FA5">Apply →</a>
          </td>
        </tr>"""

    html = f"""
    <html><body style="font-family:Arial,sans-serif;max-width:700px;margin:auto">
      <h2 style="color:#1a1a1a">🚨 {len(jobs)} New Job(s) — {run_label}</h2>
      <p style="color:#666">{datetime.now(ET).strftime('%A, %B %d %Y · %I:%M %p ET')}</p>
      <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;border:1px solid #eee;border-radius:8px">
        <thead>
          <tr style="background:#f8f8f8">
            <th style="padding:10px;text-align:left">Role</th>
            <th style="padding:10px;text-align:left">Salary</th>
            <th style="padding:10px;text-align:left">Match</th>
            <th style="padding:10px;text-align:left">Link</th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
      <p style="color:#999;font-size:12px;margin-top:20px">
        Ashvin Job Pipeline v3 · Only showing jobs posted in last 2h · Sponsorship-filtered
      </p>
    </body></html>"""

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"🚨 {len(jobs)} New Jobs — {run_label}"
    msg["From"]    = GMAIL_SENDER
    msg["To"]      = GMAIL_RECIPIENT
    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_SENDER, GMAIL_APP_PASSWORD)
            server.sendmail(GMAIL_SENDER, GMAIL_RECIPIENT, msg.as_string())
        print(f"  [Gmail] Sent email with {len(jobs)} jobs")
    except Exception as e:
        print(f"  [Gmail error] {e}")

# ─────────────────────────────────────────────────────────
# NOTIFICATION — C: GOOGLE SHEETS (free job tracker)
# ─────────────────────────────────────────────────────────
def notify_google_sheets(jobs: list):
    if not USE_GOOGLE_SHEETS or not jobs:
        return
    try:
        import gspread
        from google.oauth2.service_account import Credentials

        creds = Credentials.from_service_account_file(
            GOOGLE_SERVICE_ACCOUNT_JSON,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        gc     = gspread.authorize(creds)
        sheet  = gc.open_by_key(GOOGLE_SHEETS_ID).sheet1

        # Add headers if sheet is empty
        if sheet.row_count == 0 or not sheet.cell(1, 1).value:
            sheet.append_row(["Date", "Title", "Company", "Location", "Salary",
                               "Match Score", "Source", "URL", "Status"])

        now_str = datetime.now(ET).strftime("%Y-%m-%d %H:%M ET")
        for job in jobs:
            url = job.get("externalUrl", job.get("url", job.get("jobUrl","")))
            sheet.append_row([
                now_str,
                job.get("title",""),
                job.get("companyName", job.get("company","")),
                job.get("location",""),
                job.get("salary", job.get("compensation","Not listed")),
                job.get("relevance_score", 0),
                job.get("_source",""),
                url,
                "To Apply",   # you can update this manually in Sheets
            ])
        print(f"  [Google Sheets] Appended {len(jobs)} rows")
    except ImportError:
        print("  [Google Sheets] Run: pip install gspread google-auth")
    except Exception as e:
        print(f"  [Google Sheets error] {e}")

# ─────────────────────────────────────────────────────────
# NOTIFICATION — D: ntfy.sh (free, no account, instant push)
# ─────────────────────────────────────────────────────────
def notify_ntfy(jobs: list, run_label: str):
    if not USE_NTFY or not jobs:
        return
    # Sends one push per batch (not per job — avoids spam)
    top     = jobs[0]
    score   = top.get("relevance_score", 0)
    url     = top.get("externalUrl", top.get("url", top.get("jobUrl","")))
    message = (
        f"{len(jobs)} new match(es)\n"
        f"Top: {top.get('title','?')} @ {top.get('companyName', top.get('company','?'))}\n"
        f"Match: {score}/100"
    )
    try:
        requests.post(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            data=message.encode("utf-8"),
            headers={
                "Title":    f"Job Alert — {run_label}",
                "Priority": "high" if score >= 70 else "default",
                "Tags":     "briefcase",
                "Click":    url,
            },
            timeout=10,
        )
        print(f"  [ntfy.sh] Push sent")
    except Exception as e:
        print(f"  [ntfy error] {e}")

# ─────────────────────────────────────────────────────────
# DISPATCH — send to all enabled channels
# ─────────────────────────────────────────────────────────
def dispatch_all(jobs: list, run_label: str):
    if not jobs:
        print("  No new matching jobs this run.")
        return
    notify_telegram(jobs, run_label)
    notify_gmail(jobs, run_label)
    notify_google_sheets(jobs)
    notify_ntfy(jobs, run_label)

# ─────────────────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────────────────
def run_pipeline():
    now_et  = datetime.now(ET)
    weekday = now_et.weekday()   # 0=Mon … 6=Sun
    hour    = now_et.hour

    if weekday > 3 or hour < 7 or hour > 21:
        print(f"[{now_et.strftime('%a %I:%M %p ET')}] Outside active window — skipping.")
        return

    day_names = ["Monday","Tuesday","Wednesday","Thursday"]
    run_label = f"{day_names[weekday]} {now_et.strftime('%I:%M %p ET')}"
    print(f"\n[{run_label}] Pipeline running...")

    # Tuesday: all 11 queries (peak day). Other days: top 7.
    queries = SEARCH_QUERIES if weekday == 1 else SEARCH_QUERIES[:7]

    all_jobs = []
    for query in queries:
        for loc in LOCATIONS:
            print(f"  → '{query}' / '{loc}'")
            all_jobs.extend(scrape_linkedin(query, loc))
            all_jobs.extend(scrape_indeed(query, loc))

    print(f"  Scraped {len(all_jobs)} raw →", end=" ")
    filtered = filter_jobs(all_jobs)
    print(f"{len(filtered)} passed")

    dispatch_all(filtered, run_label)

# ─────────────────────────────────────────────────────────
# SCHEDULER
# ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 56)
    print("  Ashvin Job Pipeline v3 — 100% Free Notifications")
    print("=" * 56)

    scheduler = BlockingScheduler(timezone=ET)

    # Mon / Wed / Thu — every 3 hours
    scheduler.add_job(run_pipeline, CronTrigger(
        day_of_week="mon,wed,thu", hour="7,10,13,16,19,21", minute=0, timezone=ET,
    ), id="mwt", name="Mon/Wed/Thu 3hr")

    # Tuesday — every 2 hours (peak posting day)
    scheduler.add_job(run_pipeline, CronTrigger(
        day_of_week="tue", hour="7,9,11,13,15,17,19,21", minute=0, timezone=ET,
    ), id="tue", name="Tuesday 2hr")

    print("  Schedule:")
    print("  ├─ Mon/Wed/Thu → 7am, 10am, 1pm, 4pm, 7pm, 9pm ET")
    print("  ├─ Tuesday     → 7am, 9am, 11am, 1pm, 3pm, 5pm, 7pm, 9pm ET")
    print("  └─ Fri–Sun     → OFF")

    active = [
        ("Telegram", USE_TELEGRAM), ("Gmail", USE_GMAIL),
        ("Google Sheets", USE_GOOGLE_SHEETS), ("ntfy.sh", USE_NTFY),
    ]
    enabled = [n for n, v in active if v]
    print(f"\n  Notifications: {', '.join(enabled) if enabled else 'NONE ENABLED'}")
    print(f"  Current time : {datetime.now(ET).strftime('%A %I:%M %p ET')}\n")

    # Run immediately if within active hours
    now = datetime.now(ET)
    if now.weekday() <= 3 and 7 <= now.hour <= 21:
        print("  Running initial scan now...\n")
        run_pipeline()

    try:
        scheduler.start()
    except KeyboardInterrupt:
        print("\n  Stopped.")

# ═══════════════════════════════════════════════════════════
# SETUP GUIDES FOR EACH FREE OPTION
# ═══════════════════════════════════════════════════════════
#
# ── OPTION A: TELEGRAM (5 min, easiest) ─────────────────
# 1. Open Telegram → search @BotFather → send /newbot
# 2. Follow prompts → copy the token into TELEGRAM_BOT_TOKEN
# 3. Send /start to YOUR new bot
# 4. Visit: https://api.telegram.org/bot<TOKEN>/getUpdates
# 5. Find "chat":{"id": XXXXXXX} → paste as TELEGRAM_CHAT_ID
# 6. Set USE_TELEGRAM = True
#
# ── OPTION B: GMAIL SMTP (5 min) ────────────────────────
# 1. Google Account → Security → enable 2-Step Verification
# 2. Security → App Passwords → select Mail + Other
# 3. Copy the 16-char password → paste as GMAIL_APP_PASSWORD
# 4. Fill in GMAIL_SENDER (your address) and GMAIL_RECIPIENT
# 5. Set USE_GMAIL = True
# Note: sends a clean HTML table email per alert
#
# ── OPTION C: GOOGLE SHEETS (10 min, best for tracking) ─
# 1. console.cloud.google.com → New Project
# 2. Enable "Google Sheets API"
# 3. IAM & Admin → Service Accounts → Create → download JSON
#    → save as service_account.json next to this script
# 4. Create a new Google Sheet → copy ID from URL (/d/XXXXX/)
# 5. Share the Sheet with the service account email (Editor)
# 6. Paste Sheet ID → GOOGLE_SHEETS_ID
# 7. Set USE_GOOGLE_SHEETS = True
# Note: adds a row per job — great for tracking status, notes
#
# ── OPTION D: ntfy.sh (2 min, zero account needed) ──────
# 1. Pick any unique topic name (e.g. "ashvin-jobs-x7k2m")
#    → paste as NTFY_TOPIC (keep it hard to guess — it's public)
# 2. Install ntfy app on your phone (iOS/Android — free)
# 3. In app → subscribe to your topic name
# 4. Set USE_NTFY = True
# Note: sends 1 push per batch, includes deep link to top job
#
# ═══════════════════════════════════════════════════════════
