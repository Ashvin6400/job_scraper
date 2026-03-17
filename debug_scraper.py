"""
DEBUG SCRIPT — Run this ONCE to see exactly what fields
the scrapers return. Paste output in chat so we can fix the pipeline.

Run: python3 debug_scraper.py
"""

import os
import json
from apify_client import ApifyClient

APIFY_API_TOKEN = os.environ.get("APIFY_API_TOKEN", "")

if not APIFY_API_TOKEN:
    print("ERROR: APIFY_API_TOKEN not set")
    exit(1)

client = ApifyClient(APIFY_API_TOKEN)

# ── TEST 1: LinkedIn — single query, 3 results, fetchJobDetails=True ──
print("\n" + "="*60)
print("TEST 1: LinkedIn scraper — checking field names")
print("="*60)

try:
    run = client.actor("curious_coder/linkedin-jobs-scraper").call(run_input={
        "urls": [
            "https://www.linkedin.com/jobs/search/?keywords=%22Data+Scientist%22"
            "&location=United+States&f_TPR=r86400&f_E=1%2C2"
        ],
        "count": 3,
        "fetchJobDetails": True,
    })
    items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
    print(f"Got {len(items)} items")

    if items:
        job = items[0]
        print("\nALL FIELD NAMES returned by LinkedIn actor:")
        for k, v in job.items():
            # Truncate long strings for readability
            display_v = str(v)[:80] + "..." if len(str(v)) > 80 else v
            print(f"  {k}: {display_v}")

        print("\nKey fields we care about:")
        for field in ["title","companyName","company","location","salary",
                      "applicantCount","numberOfApplicants","applicants",
                      "postedAt","datePosted","publishedAt","date",
                      "jobUrl","url","description"]:
            print(f"  {field}: {job.get(field, '--- NOT PRESENT ---')}")
    else:
        print("No items returned — check LinkedIn actor or API token")

except Exception as e:
    print(f"LinkedIn ERROR: {e}")


# ── TEST 2: Indeed (borderline) — single query, 3 results ──
print("\n" + "="*60)
print("TEST 2: Indeed (borderline) — checking field names")
print("="*60)

try:
    run = client.actor("borderline/indeed-scraper").call(run_input={
        "startUrls": [{"url":
            "https://www.indeed.com/jobs?q=%22data+scientist%22"
            "&l=United+States&fromage=1&sort=date"
        }],
        "maxItems": 3,
        "maxAge": 1,
    })
    items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
    print(f"Got {len(items)} items")

    if items:
        job = items[0]
        print("\nALL FIELD NAMES returned by Indeed (borderline) actor:")
        for k, v in job.items():
            display_v = str(v)[:80] + "..." if len(str(v)) > 80 else v
            print(f"  {k}: {display_v}")

        print("\nKey fields we care about:")
        for field in ["title","positionName","jobTitle","company","companyName",
                      "location","salary","applicantCount","numberOfApplicants",
                      "postedAt","datePosted","publishedAt","date","scrapedAt",
                      "jobUrl","url","description","jobDescription"]:
            print(f"  {field}: {job.get(field, '--- NOT PRESENT ---')}")
    else:
        print("No items returned — check Indeed actor")

except Exception as e:
    print(f"Indeed ERROR: {e}")

print("\n" + "="*60)
print("DONE — paste the output above in chat")
print("="*60)
