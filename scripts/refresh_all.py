# scripts/refresh_all.py
from __future__ import annotations
import argparse
import os
import subprocess
import sys
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parents[1]  # repo root
DATA_DIR     = PROJECT_ROOT / "data"
CURATED_DIR  = DATA_DIR / "curated"
INPUT_DIR    = DATA_DIR / "input"

# Canonical file locations
FULL_DUMP_JSONL     = INPUT_DIR / "full_dump.jsonl"
APPS_ALL_CSV        = CURATED_DIR / "apps_all.csv"
APPS_CLEAN_CSV      = CURATED_DIR / "apps_clean.csv"
APPS_DROPPED_CSV    = CURATED_DIR / "apps_dropped.csv"
WEBSITES_CSV        = CURATED_DIR / "websites.csv"
REVIEWS_CSV         = CURATED_DIR / "reviews.csv"
REVIEWS_SENT_CSV    = CURATED_DIR / "reviews_with_sentiment.csv"
APP_SENTIMENT_CSV   = CURATED_DIR / "app_sentiment.csv"

# Feature outputs
FEATURES_DIR                 = CURATED_DIR
FEATURES_MATRIX_FLAGS_CSV    = CURATED_DIR / "features_matrix_flags.csv"
FEATURES_MATRIX_CONF_CSV     = CURATED_DIR / "features_matrix_confidence.csv"
FEATURES_MATRIX_HITS_CSV     = CURATED_DIR / "features_matrix_review_hits.csv"
FEATURES_LONG_CSV            = CURATED_DIR / "features_long.csv"
FEATURES_BUNDLE_CSV          = CURATED_DIR / "features_bundle.csv"
FEATURES_LLM_CSV             = CURATED_DIR / "features_llm.csv"

# --------------- runner ---------------
def run(cmd: list[str], dry: bool = False, continue_on_error: bool = False, cwd: Path | None = None):
    print("\n>>> RUN:", " ".join(cmd))
    if dry:
        return 0
    try:
        subprocess.run(cmd, check=True, cwd=str(cwd or PROJECT_ROOT))
        return 0
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] step failed (exit {e.returncode}):", " ".join(e.cmd))
        if not continue_on_error:
            sys.exit(e.returncode)
        return e.returncode

# --------------- steps ---------------
def step_scrape(discover: bool, out_jsonl: Path, dry: bool, coe: bool):
    out_jsonl.parent.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, "-m", "scrapers.scrape_pipeline", "full", "--out", str(out_jsonl)]
    if discover:
        cmd.append("--discover")
    return run(cmd, dry, coe)

def step_normalize(in_jsonl: Path, out_csv: Path, dry: bool, coe: bool,
                   include_terms: str | None = None, exclude_terms: str | None = None):
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, "etl/normalize_apps.py", "--in", str(in_jsonl), "--out", str(out_csv)]
    if include_terms:
        cmd += ["--include-terms", include_terms]
    if exclude_terms:
        cmd += ["--exclude-terms", exclude_terms]
    return run(cmd, dry, coe)

def step_clean(in_csv: Path, out_clean: Path, out_dropped: Path,
               min_rating_count: int | None, min_play_installs: int | None,
               min_cws_users: int | None, min_relevance: float | None,
               dry: bool, coe: bool):
    out_clean.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable, "-m", "etl.clean_apps",
        "--in", str(in_csv),
        "--out", str(out_clean),
        "--out-dropped", str(out_dropped),
    ]
    if min_rating_count is not None:
        cmd += ["--min-rating-count", str(min_rating_count)]
    if min_play_installs is not None:
        cmd += ["--min-play-installs", str(min_play_installs)]
    if min_cws_users is not None:
        cmd += ["--min-cws-users", str(min_cws_users)]
    if min_relevance is not None:
        cmd += ["--min-relevance", str(min_relevance)]
    return run(cmd, dry, coe)

def step_websites(apps_csv: Path, out_csv: Path,
                  resume: bool, sleep: float,
                  js_fallback: bool, js_min_len: int | None,
                  fresh: bool,
                  dry: bool, coe: bool, max_sites: int | None = None):
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    if fresh and not dry and out_csv.exists():
        print(f"[web] removing existing {out_csv} (fresh run)")
        out_csv.unlink()
    cmd = [
        sys.executable, "-m", "etl.scrape_websites",
        "--apps", str(apps_csv),
        "--out", str(out_csv),
        "--sleep", str(sleep),
    ]
    if resume:
        cmd.append("--resume")
    if js_fallback:
        cmd.append("--js-fallback")
    if js_min_len is not None:
        cmd += ["--js-min-len", str(js_min_len)]
    if max_sites is not None:
        cmd += ["--max", str(max_sites)]
    return run(cmd, dry, coe)

def step_reviews(inp_apps_or_dump: Path, out_csv: Path,
                 stores: str, countries: str, langs: str,
                 max_per_app: int, since_days: int, flush_every: int,
                 dedupe_scope: str, overwrite: bool,
                 dry: bool, coe: bool):
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable, "-m", "scrapers.reviews_pipeline", "all",
        "--in", str(inp_apps_or_dump),
        "--out-csv", str(out_csv),
        "--stores", stores,
        "--countries", countries,
        "--langs", langs,
        "--max-per-app", str(max_per_app),
        "--since-days", str(since_days),
        "--flush-every", str(flush_every),
        "--dedupe-scope", dedupe_scope,
    ]
    if overwrite:
        cmd.append("--overwrite")
    return run(cmd, dry, coe)

def step_sentiment(in_reviews: Path, out_reviews_scored: Path, out_app_agg: Path,
                   engine: str, since_days: int, min_words: int,
                   dry: bool, coe: bool):
    out_reviews_scored.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable, "-m", "llm.sentiment_pipeline", "run",
        "--in", str(in_reviews),
        "--out-reviews", str(out_reviews_scored),
        "--out-apps", str(out_app_agg),
        "--engine", engine,
        "--since-days", str(since_days),
        "--min-words", str(min_words),
    ]
    return run(cmd, dry, coe)

def step_feature_flags(apps_csv: Path, web_csv: Path, reviews_with_sent_csv: Path,
                       out_dir: Path, dry: bool, coe: bool):
    out_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable, "-m", "llm.feature_flags",
        "--feature", "all",
        "--apps", str(apps_csv),
        "--web", str(web_csv),
        "--reviews", str(reviews_with_sent_csv),
        "--out-dir", str(out_dir),
    ]
    return run(cmd, dry, coe)

def step_feature_llm(apps_csv: Path, web_csv: Path, reviews_with_sent_csv: Path,
                     out_csv: Path, model: str, api_key: str | None, base_url: str | None,
                     dry: bool, coe: bool):
    """
    Optional: LLM-powered feature extraction (OpenAI-compatible).
    If api_key is None/empty, the caller should decide to skip.
    """
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable, "-m", "llm.feature_llm",
        "--apps", str(apps_csv),
        "--web", str(web_csv),
        "--reviews", str(reviews_with_sent_csv),
        "--out", str(out_csv),
        "--model", model,
    ]
    if api_key:
        cmd += ["--api-key", api_key]
    if base_url:
        cmd += ["--base-url", base_url]
    return run(cmd, dry, coe)

# ✅ FIXED HERE
def step_feature_matrix(in_dir: Path, out_dir: Path,
                        min_conf: float, min_hits: int,
                        bundle_apps: Path | None, bundle_sent: Path | None,
                        dry: bool, coe: bool):
    out_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable, "-m", "etl.build_feature_matrix",
        "--in-dir", str(in_dir),
        "--out-dir", str(out_dir),
        "--min-confidence", str(min_conf),
        "--min-review-hits", str(min_hits),
    ]
    # ✅ Replace old --bundle-* args with correct ones
    if bundle_apps:
        cmd += ["--apps-csv", str(bundle_apps)]
    if bundle_sent:
        cmd += ["--sent-csv", str(bundle_sent)]
    return run(cmd, dry, coe)

# --------------- CLI ---------------
def main():
    ap = argparse.ArgumentParser(
        description="One-click refresh: scrape → normalize → clean → websites → reviews → sentiment → features (heuristic + optional LLM)"
    )
    # Global
    ap.add_argument("--dry-run", action="store_true", help="Print commands only; do not execute")
    ap.add_argument("--continue-on-error", "--coe", action="store_true", help="Do not stop on first failure")

    # Step toggles
    ap.add_argument("--skip-scrape", action="store_true")
    ap.add_argument("--skip-normalize", action="store_true")
    ap.add_argument("--skip-clean", action="store_true")
    ap.add_argument("--skip-websites", action="store_true")
    ap.add_argument("--skip-reviews", action="store_true")
    ap.add_argument("--skip-sentiment", action="store_true")
    ap.add_argument("--skip-feature-flags", action="store_true")
    ap.add_argument("--skip-feature-llm", action="store_true")
    ap.add_argument("--skip-feature-matrix", action="store_true")

    # Scrape
    ap.add_argument("--discover", action="store_true")

    # Normalize (optional terms)
    ap.add_argument("--include-terms", default=None)
    ap.add_argument("--exclude-terms", default=None)

    # Clean thresholds
    ap.add_argument("--min-rating-count", type=int, default=25)
    ap.add_argument("--min-play-installs", type=int, default=50_000)
    ap.add_argument("--min-cws-users", type=int, default=10_000)
    ap.add_argument("--min-relevance", type=float, default=0.15)

    # Websites
    ap.add_argument("--web-resume", action="store_true")
    ap.add_argument("--web-fresh", action="store_true")
    ap.add_argument("--web-js", action="store_true")
    ap.add_argument("--web-js-min-len", type=int, default=1)
    ap.add_argument("--web-sleep", type=float, default=1.0)
    ap.add_argument("--web-max", type=int, default=None)

    # Reviews
    ap.add_argument("--stores", default="play,ios")
    ap.add_argument("--countries", default="au,us")
    ap.add_argument("--langs", default="en")
    ap.add_argument("--max-per-app", type=int, default=200)
    ap.add_argument("--since-days", type=int, default=365)
    ap.add_argument("--flush-every", type=int, default=200)
    ap.add_argument("--dedupe-scope", choices=["per-app", "global"], default="global")
    ap.add_argument("--reviews-overwrite", action="store_true")

    # Sentiment
    ap.add_argument("--sent-engine", default="vader", choices=["vader"])
    ap.add_argument("--sent-since-days", type=int, default=365)
    ap.add_argument("--sent-min-words", type=int, default=3)

    # Heuristic→Matrix thresholds
    ap.add_argument("--feat-min-confidence", type=float, default=0.60)
    ap.add_argument("--feat-min-review-hits", type=int, default=5)

    # Optional LLM feature extraction
    ap.add_argument("--use-llm", action="store_true", help="Enable LLM feature extraction step")
    ap.add_argument("--llm-model", default="gpt-4.1-mini")
    ap.add_argument("--llm-api-key", default=None, help="If omitted, will read OPENAI_API_KEY")
    ap.add_argument("--llm-base-url", default=None, help="Optional: custom OpenAI-compatible base URL")

    args = ap.parse_args()

    print(f"=== Refresh started @ {datetime.now().isoformat(timespec='seconds')} ===")

    # 1) Scrape
    if not args.skip_scrape:
        step_scrape(args.discover, FULL_DUMP_JSONL, args.dry_run, args.continue_on_error)
    else:
        print("[skip] scrape")

    # 2) Normalize
    if not args.skip_normalize:
        step_normalize(
            FULL_DUMP_JSONL, APPS_ALL_CSV, args.dry_run, args.continue_on_error,
            include_terms=args.include_terms, exclude_terms=args.exclude_terms
        )
    else:
        print("[skip] normalize")

    # 3) Clean
    if not args.skip_clean:
        step_clean(
            APPS_ALL_CSV, APPS_CLEAN_CSV, APPS_DROPPED_CSV,
            args.min_rating_count, args.min_play_installs, args.min_cws_users, args.min_relevance,
            args.dry_run, args.continue_on_error
        )
    else:
        print("[skip] clean")

    # 4) Websites
    if not args.skip_websites:
        step_websites(
            APPS_CLEAN_CSV, WEBSITES_CSV,
            resume=args.web_resume, sleep=args.web_sleep,
            js_fallback=args.web_js, js_min_len=args.web_js_min_len,
            fresh=args.web_fresh,
            dry=args.dry_run, coe=args.continue_on_error, max_sites=args.web_max
        )
    else:
        print("[skip] websites")

    # 5) Reviews
    if not args.skip_reviews:
        step_reviews(
            APPS_CLEAN_CSV, REVIEWS_CSV,
            args.stores, args.countries, args.langs,
            args.max_per_app, args.since_days, args.flush_every,
            args.dedupe_scope, args.reviews_overwrite,
            args.dry_run, args.continue_on_error
        )
    else:
        print("[skip] reviews")

    # 6) Sentiment
    if not args.skip_sentiment:
        step_sentiment(
            REVIEWS_CSV, REVIEWS_SENT_CSV, APP_SENTIMENT_CSV,
            args.sent_engine, args.sent_since_days, args.sent_min_words,
            args.dry_run, args.continue_on_error
        )
    else:
        print("[skip] sentiment")

    # 7) Heuristic feature flags
    if not args.skip_feature_flags:
        step_feature_flags(
            APPS_CLEAN_CSV, WEBSITES_CSV, REVIEWS_SENT_CSV,
            FEATURES_DIR, args.dry_run, args.continue_on_error
        )
    else:
        print("[skip] feature-flags")

    # 8) Optional LLM feature extraction
    if not args.skip_feature_llm and args.use_llm:
        api_key = args.llm_api_key or os.environ.get("OPENAI_API_KEY")
        if not api_key:
            print("[skip] feature-llm: no API key provided (pass --llm-api-key or set OPENAI_API_KEY)")
        else:
            step_feature_llm(
                APPS_CLEAN_CSV, WEBSITES_CSV, REVIEWS_SENT_CSV,
                FEATURES_LLM_CSV, args.llm_model, api_key, args.llm_base_url,
                args.dry_run, args.continue_on_error
            )
    else:
        print("[skip] feature-llm (disabled)")

    # 9) Feature matrix consolidation (+ bundle for dashboard)
    if not args.skip_feature_matrix:
        step_feature_matrix(
            FEATURES_DIR, FEATURES_DIR,
            args.feat_min_confidence, args.feat_min_review_hits,
            bundle_apps=APPS_CLEAN_CSV, bundle_sent=APP_SENTIMENT_CSV,
            dry=args.dry_run, coe=args.continue_on_error
        )
    else:
        print("[skip] feature-matrix")

    print(f"\n=== Refresh finished @ {datetime.now().isoformat(timespec='seconds')} ===")
    print("Outputs:")
    print("-", FULL_DUMP_JSONL.relative_to(PROJECT_ROOT))
    print("-", APPS_ALL_CSV.relative_to(PROJECT_ROOT))
    print("-", APPS_CLEAN_CSV.relative_to(PROJECT_ROOT))
    print("-", APPS_DROPPED_CSV.relative_to(PROJECT_ROOT))
    print("-", WEBSITES_CSV.relative_to(PROJECT_ROOT))
    print("-", REVIEWS_CSV.relative_to(PROJECT_ROOT))
    print("-", REVIEWS_SENT_CSV.relative_to(PROJECT_ROOT))
    print("-", APP_SENTIMENT_CSV.relative_to(PROJECT_ROOT))
    print("-", FEATURES_MATRIX_FLAGS_CSV.relative_to(PROJECT_ROOT))
    print("-", FEATURES_MATRIX_CONF_CSV.relative_to(PROJECT_ROOT))
    print("-", FEATURES_MATRIX_HITS_CSV.relative_to(PROJECT_ROOT))
    print("-", FEATURES_LONG_CSV.relative_to(PROJECT_ROOT))
    if FEATURES_LLM_CSV.exists():
        print("-", FEATURES_LLM_CSV.relative_to(PROJECT_ROOT))

if __name__ == "__main__":
    main()
