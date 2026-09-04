# Task Summary — Tasks 1 to 5

**Prepared by:** Koushik Srinivasan

**For:** Jeremy Nagel

**Date:** 03 September 2026

## Task 1: Share the Analysis File

**Files I used:** - `ADHD_Review_Plan.md` (the 500 → 2,000 plan document)

**What I changed:** Nothing in the code — I just shared an already-written document.

**What happens as a result:** - Jeremy now has the plan explaining the mistake I found, how I fixed it, and the roadmap going forward. - This gave him the context he needed before assigning me the CSV export and keyword search tasks. - No further action needed on this one

------------------------------------------------------------------------

## Task 2: Export All Reviews to a CSV File

**Files I used:** - Script I created: `export_reviews.py` - Files it reads from: `data/reviews/reviews.jsonl`, `data/curated/reviews.csv`, `data/curated/manual_validation_sample.csv`, `data/curated/playstore_reviews_sentiment.csv`, `data/curated/ios_reviews_sentiment.csv` - Output file it creates: `data/curated/all_reviews_export.csv`

**What I changed:** No app code was changed. I wrote a new script that reads every review-containing file in the whole repo, combines them into one table, and removes true duplicates using each review's unique ID rather than just matching text.

**What happens as a result:** - Running `export_reviews.py` produces one single CSV file with every real, unique review in the repo — **42,623 reviews**. - I discovered along the way that no single file in the repo actually contained "all" the reviews — several files each had unique reviews the others were missing, so I had to combine all of them. - I also caught and fixed an early version of this script that was slightly over-merging different reviews that happened to share identical text — the final version avoids that by matching on review ID first. - This file is now the single source of truth I use for every keyword search and labeling task after this.

------------------------------------------------------------------------

## Task 3: Manually Search for ADHD Mentions

**Files I used:** - Script I created: `search_keywords.py` - File it reads from: `data/curated/all_reviews_export.csv` - Output file it creates: `data/curated/adhd_autism_keyword_search_results.csv`

**What I changed:** No app code was changed. I wrote a script that does a plain, simple text search for "adhd" and "autis" across every exported review — independent of any of the app's own detection logic, exactly as Jeremy asked for.

**What happens as a result:** - Running `search_keywords.py` confirms real ADHD/autism mentions genuinely exist in the data. - This directly answered Jeremy's concern from the meeting — the earlier "0 results" shown in the dashboard was a code problem, not a case of the data genuinely having nothing. - This gave me confidence to move on to actually fixing the detection logic, since I now knew for certain there was real content to catch.

------------------------------------------------------------------------

## Task 4: Add Autism as a Separate Label

**Files I used:** - File I changed: `scrapers/reviews_pipeline.py` - Script I created to test the change: `test_nd_labels.py` - Script I created to apply it to all data: `label_nd_categories.py` - File it reads from: `data/curated/all_reviews_export.csv` - Output file it creates: `data/curated/nd_labeled_reviews.csv`

**What I changed:** In `scrapers/reviews_pipeline.py`, I split the old single "is this ADHD-related?" check into three separate checks: ADHD, autism, and other neurodivergent terms (like dyslexia or sensory processing). I added a new function, `get_nd_categories()`, that returns exactly which category (or categories) a review belongs to, instead of just true/false.

**What happens as a result:** - Every review can now be labeled as ADHD, autism, both, or another related category — not lumped into one flag like before. - I tested this against 7 real example cases before trusting it, including a tricky one where a review mentions both ADHD and autism together — it correctly labels both. - Running `label_nd_categories.py` on the full dataset gives: **1,143 ADHD-labeled, 63 autism-labeled, 54 labeled as both, 56 other neurodivergent, 1,189 total.** - I kept the old `is_special_review()` function working exactly as before, so nothing else in the codebase broke from this change. - I also found that autism-related words were technically already in the old code, just never separated out — so part of this task was really about labeling clearly, not detecting from scratch.

------------------------------------------------------------------------

## Task 5: Raise the Reviews-Per-App Scrape Limit

**Files I used:** - File I changed: `scrapers/reviews_pipeline.py`

**What I changed:** I increased two default settings: - `--max-per-app`: 300 → **2,000** (how many reviews get collected per app) - `--since-days`: 365 → **1,095** (how far back in time it looks — about 3 years instead of 1)

**What happens as a result:** - Every future review scrape automatically collects far more reviews per app, and reaches much further back in time, without anyone needing to remember to type extra settings each time. - I found that the actual old default was 300 per app, not 500 like Jeremy mentioned in the meeting — worth telling him, since the real starting point was even lower than he thought. - I also added "autism" to the app-discovery keyword list (`scrapers/discovery.yml`) as part of getting ready for the bigger scrape, so the tool now actively looks for autism-focused apps too, not just ADHD ones. - This change is what makes Task 6 (working toward 5,000 reviews) actually possible — without it, scraping more would still be capped at the old, much lower limits.
