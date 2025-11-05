# etl/build_app_cards.py
import argparse
from pathlib import Path
import pandas as pd

def _safe_read_csv(path: str, usecols=None) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(p, usecols=usecols) if usecols else pd.read_csv(p)
    except Exception:
        # fall back to broad read if usecols mismatch
        return pd.read_csv(p)

def _weighted_mean(series: pd.Series, weights: pd.Series) -> float:
    series = pd.to_numeric(series, errors="coerce").fillna(0.0)
    weights = pd.to_numeric(weights, errors="coerce").fillna(0.0)
    ws = float(weights.sum())
    if ws <= 0:
        return float(series.mean()) if len(series) else 0.0
    return float((series * weights).sum() / ws)

def _aggregate_sentiment(sent: pd.DataFrame) -> pd.DataFrame:
    """
    Collapse per-country rows to one row per (app_key, store) using n_reviews as weights.
    Keeps: n_reviews (sum), n_nd (sum), weighted means for rating/compound/percentages.
    """
    if sent.empty:
        return sent

    # Ensure expected columns exist
    for c in ["app_key", "store", "country", "n_reviews", "n_nd",
              "avg_rating", "mean_compound", "pct_positive", "pct_negative"]:
        if c not in sent.columns:
            # create missing columns with neutral defaults
            sent[c] = 0 if c in {"n_reviews", "n_nd"} else None

    rows = []
    gcols = ["app_key", "store"] if "store" in sent.columns else ["app_key"]
    for key, g in sent.groupby(gcols, dropna=False):
        if not isinstance(key, tuple):
            key = (key,)
        rec = {gcols[i]: key[i] for i in range(len(gcols))}
        w = pd.to_numeric(g.get("n_reviews", 0), errors="coerce").fillna(0.0)

        rec["n_reviews"]     = int(pd.to_numeric(g.get("n_reviews", 0), errors="coerce").fillna(0).sum())
        rec["n_nd"]          = int(pd.to_numeric(g.get("n_nd", 0), errors="coerce").fillna(0).sum())
        rec["avg_rating"]    = _weighted_mean(g.get("avg_rating"), w)
        rec["mean_compound"] = _weighted_mean(g.get("mean_compound"), w)
        rec["pct_positive"]  = _weighted_mean(g.get("pct_positive"), w)
        rec["pct_negative"]  = _weighted_mean(g.get("pct_negative"), w)
        rows.append(rec)

    return pd.DataFrame(rows)

def main():
    ap = argparse.ArgumentParser(description="Build unified app cards for the dashboard.")
    ap.add_argument("--apps", default="data/curated/apps_clean.csv")
    ap.add_argument("--bundle", default="data/curated/features_bundle.csv")
    ap.add_argument("--sent", default="data/curated/app_sentiment.csv")
    ap.add_argument("--insights", default="data/curated/review_app_insights.csv")
    ap.add_argument("--out", default="data/curated/app_cards.csv")
    args = ap.parse_args()

    # Required base
    apps = _safe_read_csv(args.apps)
    if apps.empty or "app_key" not in apps.columns:
        raise SystemExit(f"[app-cards] {args.apps} missing or lacks app_key")

    # Optional: features bundle -> wide flags per feature
    bndl = _safe_read_csv(args.bundle)
    if not bndl.empty and {"app_key", "feature", "flag"}.issubset(bndl.columns):
        flags = (
            bndl.pivot_table(index="app_key", columns="feature", values="flag", aggfunc="max")
                .fillna(0).astype(int).reset_index()
        )
        apps = apps.merge(flags, on="app_key", how="left")
    else:
        # ensure we at least have the column for downstream UI robustness if needed
        pass

    # Optional: sentiment (aggregate across countries, then merge)
    sent = _safe_read_csv(args.sent)
    if not sent.empty:
        sent_agg = _aggregate_sentiment(sent)
        merge_keys = ["app_key", "store"] if "store" in sent_agg.columns and "store" in apps.columns else ["app_key"]
        apps = apps.merge(sent_agg, on=merge_keys, how="left")

    # Optional: LLM review insights per app
    ins = _safe_read_csv(args.insights)
    if not ins.empty and "app_key" in ins.columns:
        apps = apps.merge(ins, on="app_key", how="left")

    # Write
    outp = Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)
    apps.to_csv(outp, index=False)
    print(f"[app-cards] wrote -> {outp}")

if __name__ == "__main__":
    main()
