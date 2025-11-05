# etl/build_feature_matrix.py
from __future__ import annotations
import argparse
from pathlib import Path
import re
import pandas as pd

FEATURE_PREFIX = "features_"
FEATURE_EXCLUDES = {
    "features_matrix_flags.csv",
    "features_matrix_confidence.csv",
    "features_matrix_review_hits.csv",
    "features_long.csv",
    "features_bundle.csv",
}

SAFE_FEATURE_COLS = {"app_key", "flag", "confidence", "review_hits"}

def find_feature_files(in_dir: Path) -> list[Path]:
    files = []
    for p in sorted(in_dir.glob(f"{FEATURE_PREFIX}*.csv")):
        if p.name in FEATURE_EXCLUDES:
            continue
        files.append(p)
    return files

def feature_name_from_filename(name: str) -> str:
    # features_blocking.csv -> blocking
    base = name.rsplit(".", 1)[0]
    return re.sub(r"^features_", "", base)

def load_feature_long(in_dir: Path, min_conf: float, min_hits: int) -> pd.DataFrame:
    rows = []
    files = find_feature_files(in_dir)
    if not files:
        raise SystemExit(f"No feature files found under {in_dir}")

    for p in files:
        f = feature_name_from_filename(p.name)
        df = pd.read_csv(p)
        # standardize expected cols if present
        cols = {c.lower(): c for c in df.columns}
        # Project onto safe cols (ignore title/store/etc to avoid collisions later)
        use = {}
        if "app_key" in df.columns:
            use["app_key"] = df["app_key"].astype(str)
        else:
            # try to synthesize from store/id if present (rare)
            if {"store", "id"}.issubset(df.columns):
                use["app_key"] = (df["store"].fillna("").str.lower().str.replace("appstore", "ios", regex=False).str.replace("playstore", "play", regex=False).str.replace("chromews", "cws", regex=False)
                                  + ":" + df["id"].astype(str))
            else:
                continue

        # flag/confidence/review_hits with defaults
        use["confidence"]   = pd.to_numeric(df.get("confidence"), errors="coerce").fillna(0.0)
        use["review_hits"]  = pd.to_numeric(df.get("review_hits"), errors="coerce").fillna(0).astype(int)
        # flag present? otherwise compute from thresholds
        if "flag" in df.columns:
            use["flag"] = pd.to_numeric(df["flag"], errors="coerce").fillna(0).astype(int)
        else:
            use["flag"] = ((use["confidence"] >= min_conf) & (use["review_hits"] >= min_hits)).astype(int)

        tmp = pd.DataFrame(use)
        tmp["feature"] = f
        rows.append(tmp[["app_key", "feature", "flag", "confidence", "review_hits"]])

    long_df = pd.concat(rows, ignore_index=True).drop_duplicates(["app_key", "feature"], keep="last")
    return long_df

def write_matrices(long_df: pd.DataFrame, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)

    flags = long_df.pivot_table(index="app_key", columns="feature", values="flag", aggfunc="max", fill_value=0).sort_index(axis=1)
    conf  = long_df.pivot_table(index="app_key", columns="feature", values="confidence", aggfunc="max", fill_value=0.0).sort_index(axis=1)
    hits  = long_df.pivot_table(index="app_key", columns="feature", values="review_hits", aggfunc="sum", fill_value=0).sort_index(axis=1)

    flags.to_csv(out_dir / "features_matrix_flags.csv")
    conf.to_csv(out_dir / "features_matrix_confidence.csv")
    hits.to_csv(out_dir / "features_matrix_review_hits.csv")
    long_df.to_csv(out_dir / "features_long.csv", index=False)

    print("[features-matrix] wrote: features_matrix_flags.csv, features_matrix_confidence.csv, features_matrix_review_hits.csv, features_long.csv")

def maybe_bundle(out_dir: Path, long_df: pd.DataFrame,
                 bundle_apps: bool, apps_csv: str | None,
                 bundle_sent: bool, sent_csv: str | None):
    if not (bundle_apps or bundle_sent):
        return

    bundle = long_df.copy()

    # Add app metadata (safe selection) if requested
    if bundle_apps:
        apps_path = Path(apps_csv) if apps_csv else None
        if not apps_path or not apps_path.exists():
            # try defaults
            for cand in (out_dir / "apps_clean.csv", out_dir / "apps_all.csv"):
                if cand.exists():
                    apps_path = cand
                    break
        if apps_path and apps_path.exists():
            apps = pd.read_csv(apps_path, dtype=str)
            keep_cols = [c for c in ["app_key", "store", "title", "rating_avg", "rating_count", "installs_or_users", "relevance_score"] if c in apps.columns]
            apps = apps[keep_cols].drop_duplicates("app_key")
            # Avoid duplicate columns by suffixing app metadata if clashes ever appear
            bundle = bundle.merge(apps, on="app_key", how="left", suffixes=("", "_apps"))

    # Add sentiment aggregates if requested
    if bundle_sent:
        sent_path = Path(sent_csv) if sent_csv else None
        if not sent_path or not sent_path.exists():
            cand = out_dir / "app_sentiment.csv"
            if cand.exists():
                sent_path = cand
        if sent_path and sent_path.exists():
            s = pd.read_csv(sent_path)
            # if country-level rows exist, collapse to app_key
            if "country" in s.columns:
                gcols = [c for c in s.columns if c not in {"country"}]
                s = (s.groupby("app_key", as_index=False)
                       .agg({c: "mean" for c in s.columns if c not in {"app_key", "store", "country"}}))
            keep = [c for c in ["app_key", "avg_rating", "mean_compound", "pct_positive", "pct_negative", "n_reviews", "n_nd"] if c in s.columns]
            s = s[keep].drop_duplicates("app_key")
            bundle = bundle.merge(s, on="app_key", how="left", suffixes=("", "_sent"))

    out_path = out_dir / "features_bundle.csv"
    bundle.to_csv(out_path, index=False)
    print(f"[features-matrix] wrote bundle -> {out_path}")

def build_matrices(in_dir: str, out_dir: str,
                   min_conf: float, min_hits: int,
                   bundle_apps: bool, bundle_sent: bool,
                   apps_csv: str | None, sent_csv: str | None):
    in_dir_p = Path(in_dir)
    out_dir_p = Path(out_dir)
    long_df = load_feature_long(in_dir_p, min_conf, min_hits)
    write_matrices(long_df, out_dir_p)
    maybe_bundle(out_dir_p, long_df, bundle_apps, apps_csv, bundle_sent, sent_csv)

def main():
    ap = argparse.ArgumentParser(description="Combine per-feature CSVs into matrices and optional bundle")
    ap.add_argument("--in-dir", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--min-confidence", type=float, default=0.0)
    ap.add_argument("--min-review-hits", type=int, default=0)

    # Optional bundle outputs
    ap.add_argument("--bundle-apps", action="store_true", help="Merge app metadata into features_long bundle")
    ap.add_argument("--apps-csv", default=None, help="Path to apps_clean.csv (optional)")
    ap.add_argument("--bundle-sent", action="store_true", help="Merge sentiment aggregates into bundle")
    ap.add_argument("--sent-csv", default=None, help="Path to app_sentiment.csv (optional)")

    args = ap.parse_args()
    build_matrices(
        args.in_dir, args.out_dir,
        args.min_confidence, args.min_review_hits,
        args.bundle_apps, args.bundle_sent,
        args.apps_csv, args.sent_csv
    )

if __name__ == "__main__":
    main()
