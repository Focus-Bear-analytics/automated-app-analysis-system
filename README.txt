Automated App Analysis System

1. PROJECT STRUCTURE

  automated-app-analysis-system/
  |
  +-- scrapers/
  |   +-- scrape_pipeline.py
  |   +-- reviews_pipeline.py
  |   +-- store_play.py
  |   +-- store_ios.py
  |   +-- store_cws.py
  |   +-- discovery/
  |   |   +-- search_play.py
  |   |   +-- search_ios.py
  |   |   +-- search_cws.py
  |   +-- seeds.yml
  |   +-- discovery.yml
  |
  +-- etl/
  |   +-- normalize_apps.py
  |   +-- clean_apps.py
  |   +-- scrape_websites.py
  |   +-- build_app_cards.py
  |   +-- aggregate_review_insights.py
  |   +-- build_feature_matrix.py
  |   +-- validate_lyngs_classification.py
  |
  +-- llm/
  |   +-- sentiment_pipeline.py
  |   +-- feature_llm.py
  |   +-- feature_extract.py
  |   +-- feature_flags.py
  |   +-- taxonomy.yml
  |
  +-- scripts/
  |   +-- improved_sentiment_analysis.py
  |   +-- adhd_sentiment_analysis.py
  |   +-- adhd_theme_detection.py
  |   +-- llm_adhd_classifier.py
  |
  +-- data/
  |   +-- input/
  |   +-- curated/
  |       +-- apps_all_clean.csv
  |       +-- reviews.csv
  |       +-- reviews_with_sentiment.csv
  |       +-- reviews_with_ml_sentiment.csv
  |       +-- adhd_reviews_analysis.csv
  |       +-- adhd_theme_analysis.csv
  |       +-- llm_adhd_behaviour_analysis.csv
  |       +-- app_metrics.csv
  |       +-- features_llm.csv
  |       +-- features_matrix_flags.csv
  |       +-- features_matrix_confidence.csv
  |       +-- features_matrix_review_hits.csv
  |       +-- lyngs_validation_groundtruth.csv
  |
  +-- survivorship_analysis.py
  +-- app.py
  +-- README.md

2. SETUP

2.1 Install dependencies

    pip install -r requirements.txt

Key packages: streamlit, pandas, plotly, google-play-scraper, transformers, torch, openai,
google-generativeai, requests, tqdm, pyyaml.

2.2 Set environment variables

    $env:OPENAI_API_KEY = "sk-..."

3. PIPELINE WALKTHROUGH

All commands are run from the project root.

Step 1 -- App Discovery & Scraping

Discovers and scrapes app metadata from seeds defined in seeds.yml and discovery.yml.

    python -m scrapers.scrape_pipeline full --discover

Step 2 -- Normalise & Clean

Normalisation is run automatically by Step 1. To run manually:

    python -m etl.normalize_apps --in data/input/full_dump.jsonl --out data/curated/apps_all.csv

    python -m etl.clean_apps --in data/curated/apps_all.csv --out data/curated/apps_clean.csv

    python -m etl.build_feature_matrix --in-dir data/curated --out-dir data/curated

Step 3 -- Scrape Reviews

    python -m scrapers.reviews_pipeline all --in data/curated/apps_clean.csv --out-csv data/curated/reviews.csv --max-per-app 2000 --stores play,ios,cws --countries au,us,gb,ca,nz,ie --langs en --since-days 3650 --overwrite

Use the sweet-spot profiler to calibrate before a full run:

    python -m scrapers.reviews_pipeline sweet-spot `
      --in data/curated/apps_all_clean.csv `
      --sample-apps 5 `
      --caps 200,500,1000,2000

Step 4 -- Sentiment & ADHD Labelling

    python -m llm.sentiment_pipeline --in data/curated/reviews.csv --out data/curated/reviews_with_sentiment.csv --backend roberta --resume

Step 5 -- Aggregate Review Insights

    python -m etl.aggregate_review_insights --reviews data/curated/reviews_with_sentiment.csv --apps data/curated/apps_clean.csv --out data/curated/app_metrics.csv

Step 6 -- Feature Flags

    # All features at once
    python -m llm.feature_flags `
      --feature all `
      --apps data/curated/apps_clean.csv `
      --reviews data/curated/reviews_with_sentiment.csv `
      --out-dir data/curated/

    # Single feature
    python -m llm.feature_flags `
      --feature adhdsupport `
      --apps data/curated/apps_clean.csv `
      --out data/curated/features_adhdsupport.csv

Step 7 -- Feature Extraction (LLM)

    python -m llm.feature_extract --apps data/curated/apps_clean.csv --out-jsonl data/curated/features.jsonl --out-csv data/curated/features.csv --model openai:gpt-4.1-mini --max-apps 102 --sleep 0.6

Step 8 -- LLM Feature Classification

    python -m llm.feature_llm --apps data/curated/apps_clean.csv --web data/curated/websites.csv --reviews data/curated/reviews.csv --taxonomy llm/taxonomy.yml --out data/curated/features_llm.csv --app-type auto

Step 9 -- Build Feature Matrix

    python -m etl.build_feature_matrix --in-dir data/curated --out-dir data/curated --taxonomy llm/taxonomy.yml --llm-csv data/curated/features_llm.csv --bundle-apps --apps-csv data/curated/apps_all_clean.csv --bundle-sent --sent-csv data/curated/app_metrics.csv

Step 10 -- Validate Lyngs Classification

    python -m etl.validate_lyngs_classification --ground-truth data/curated/lyngs_validation_groundtruth.csv --predictions data/curated/features_llm.csv --out-dir data/curated/validation

Step 11 -- Launch Dashboard

    streamlit run app.py


4. Dashboard Updates

New ADHD-specific components added to app.py:

  ADHD Behavioural Insights
  ADHD Theme Analysis
  Automated ADHD Insight Summary
  ADHD Behavioural Recommendations

The automated insight section reports the most common behavioural themes, secondary themes,
common patterns, and ADHD review statistics without requiring manual review of individual comments.

5. SURVIVORSHIP ANALYSIS

The Survivorship Analysis page (survivorship_analysis.py) uses a 2019 point-in-time review
snapshot from three CSV files:

  2019_03_19-play_reviews_unique.csv    -- Google Play      -- Up to March 2019
  2019-03-21_apple_reviews_unique.csv   -- Apple App Store  -- Up to March 2019
  2019-02-08-chrome_reviews_unique.csv  -- Chrome Web Store -- Up to February 2019

Apps are mapped to the Lyngs (2022) self-regulation app registry and manually verified for
survival status as of May 2026 (Active / Stale / Discontinued).

The page provides:
  Tab 1 -- App status table
  Tab 2 -- Feature mention rates: Active vs Discontinued (% of reviews mentioning each feature bucket)
  Tab 3 -- Average rating trends over time per survival status
  Tab 4 -- Explore individual reviews by app and feature
  Tab 5 -- Methodology and limitations

Key finding: Active apps show ~20% higher mention rates for Reward features and ~5% higher for
ADHD features; Nudge features are ~13% more common in discontinued apps.


6. DASHBOARD PAGES

  Overview
  Competitors
  Sentiment Analysis
  Feature Matrix
  ADHD Analysis
  Summary
  Survivorship Analysis

The Feature Matrix page displays:

  Top Detected Features
  Number of Apps by Feature
  Feature Distribution by App Area
  Feature Counts by App Area
  Apps with the Most Detected Features
  Insights Summary

7. ENVIRONMENT VARIABLES

  OPENAI_API_KEY  -- Required for LLM feature classification and extraction


8. KNOWN ISSUES & FIXES

  Issue: TypeError: Path.replace() takes 2 positional arguments in feature_extract.py line 183
  Fix:   Change out_csv.replace(".csv","_rollup.csv") to
         out_csv.with_name(out_csv.stem + "_rollup.csv")

  Issue: unrecognized arguments: --cap --limit in reviews_pipeline.py
  Fix:   Use --max-per-app instead of --cap; there is no --limit argument -- filter the input
         CSV to limit apps

  Issue: ADHD review count too low
  Fix:   Increase --max-per-app, --since-days, and --countries in the reviews pipeline step

  Issue: Survivorship page not loading
  Fix:   Ensure the three 2019 review CSV files are present in the project root or data/ subfolder

  Issue: Dashboard showing "0.0" or "nan" for rating/installs
  Fix:   Use format_rating() and format_installs() helpers -- see Module 5 above
