"""
DEBUG v2 — fixed inputs based on error messages
"""
import os
import json
from apify_client import ApifyClient

APIFY_API_TOKEN = os.environ.get("APIFY_API_TOKEN", "")
if not APIFY_API_TOKEN:
    print("ERROR: APIFY_API_TOKEN not set"); exit(1)

client = ApifyClient(APIFY_API_TOKEN)

# ── TEST 1: LinkedIn — count must be >= 10 ──
print("\n" + "="*60)
print("TEST 1: LinkedIn scraper")
print("="*60)
try:
    run = client.actor("curious_coder/linkedin-jobs-scraper").call(run_input={
        "urls": [
            "https://www.linkedin.com/jobs/search/?keywords=%22Data+Scientist%22"
            "&location=United+States&f_TPR=r86400&f_E=1%2C2"
        ],
        "count": 10,              # minimum is 10
        "fetchJobDetails": True,
    })
    items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
    print(f"Got {len(items)} items")

    if items:
        job = items[0]
        print("\nALL FIELDS from first LinkedIn job:")
        for k, v in job.items():
            display_v = str(v)[:100] + "..." if len(str(v)) > 100 else v
            print(f"  {k}: {display_v}")
    else:
        print("No items — LinkedIn may be blocking or query returned 0")
except Exception as e:
    print(f"LinkedIn ERROR: {e}")


# ── TEST 2: Indeed (borderline) — uses query+country, not startUrls ──
print("\n" + "="*60)
print("TEST 2: Indeed (borderline) — correct input format")
print("="*60)
try:
    run = client.actor("borderline/indeed-scraper").call(run_input={
        "query":    "data scientist",
        "country":  "us",
        "location": "United States",
        "maxItems": 5,
        "maxAge":   1,            # today only
    })
    items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
    print(f"Got {len(items)} items")

    if items:
        job = items[0]
        print("\nALL FIELDS from first Indeed job:")
        for k, v in job.items():
            display_v = str(v)[:100] + "..." if len(str(v)) > 100 else v
            print(f"  {k}: {display_v}")

        print("\nKey fields check:")
        for field in ["title","positionName","company","companyName","location",
                      "salary","applicantCount","numberOfApplicants","applyCount",
                      "postedAt","datePosted","publishedAt","date","scrapedAt",
                      "jobUrl","url","description","jobDescription","jobType"]:
            val = job.get(field, "--- NOT PRESENT ---")
            print(f"  {field}: {str(val)[:80]}")
    else:
        print("No items returned")
except Exception as e:
    print(f"Indeed ERROR: {e}")

print("\n" + "="*60)
print("DONE — paste output in chat")
print("="*60)
