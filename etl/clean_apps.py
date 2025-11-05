# etl/clean_apps.py
import argparse, re
from pathlib import Path
import pandas as pd
from dateutil import parser as dparser

def to_iso_date(val):
    if val is None or (isinstance(val,float) and pd.isna(val)) or (isinstance(val,str) and not val.strip()):
        return pd.NA
    try: return dparser.parse(str(val)).date().isoformat()
    except Exception: return pd.NA

def norm_cols(df):
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower().str.replace(r"[^a-z0-9]+","_", regex=True)
    for c in df.select_dtypes(include=["object"]).columns:
        df[c] = df[c].astype(str).str.strip().replace({"": pd.NA, "nan": pd.NA})
    return df

def map_store(s):
    if not isinstance(s,str): return pd.NA
    t = s.strip().lower()
    if t in {"playstore","googleplay","google_play","android"}: return "PlayStore"
    if t in {"appstore","ios","apple_app_store","apple"}: return "AppStore"
    if t in {"chromews","chrome_web_store","chromewebstore","cws"}: return "ChromeWS"
    return s

def main(in_csv, out_keep, out_drop,
         min_rating_count, min_play_installs, min_cws_users,
         min_relevance):
    df = pd.read_csv(in_csv, low_memory=False)
    df = norm_cols(df)

    if "store" in df.columns: df["store"] = df["store"].apply(map_store)
    for c in ("rating_count","installs_or_users"):
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in ("release_date","last_update","scraped_at"):
        if c in df.columns: df[c] = df[c].apply(to_iso_date)

    if "app_key" not in df.columns:
        if {"store","id"}.issubset(df.columns): df["app_key"] = df["store"].astype(str)+"::"+df["id"].astype(str)
        else: df["app_key"] = df.index.astype(str)

    # minimal viability: title or description must exist; avoid junk 1-char titles
    have_text = (df.get("title").notna()) | (df.get("description").notna())
    junk = df.get("title", pd.Series([""]*len(df))).astype(str).str.len() < 2
    need_ok = have_text & ~junk

    # dedupe by app_key -> keep latest scraped_at then highest rating_count
    if "scraped_at" in df.columns:
        df["_t"] = pd.to_datetime(df["scraped_at"], errors="coerce")
    else:
        df["_t"] = pd.NaT
    df["_rc"] = pd.to_numeric(df.get("rating_count"), errors="coerce")
    df = (df.sort_values(["_t","_rc"], ascending=[True, True])
            .drop_duplicates(subset=["app_key"], keep="last")
            .drop(columns=["_t","_rc"]))

    # quality gates (store-aware popularity) + relevance
    rc  = pd.to_numeric(df.get("rating_count"), errors="coerce").fillna(0)
    ins = pd.to_numeric(df.get("installs_or_users"), errors="coerce").fillna(0)
    store = df.get("store").fillna("")

    ok_signal = (
        (rc >= min_rating_count) |
        ((store.eq("PlayStore")) & (ins >= min_play_installs)) |
        ((store.eq("ChromeWS")) & (ins >= min_cws_users))
    )
    rel = pd.to_numeric(df.get("relevance_score"), errors="coerce").fillna(0)
    ok_relevance = rel >= min_relevance

    keep_mask = need_ok & ok_signal & ok_relevance
    kept = df[keep_mask].copy()
    dropped = df[~keep_mask].copy()

    # explain drops
    reasons = []
    for i in dropped.index:
        why = []
        if not need_ok.loc[i]: why.append("missing_title_and_description")
        if not ok_signal.loc[i]: why.append("below_popularity_thresholds")
        if not ok_relevance.loc[i]: why.append("low_relevance")
        reasons.append(",".join(why) or "unknown")
    dropped["drop_reason"] = reasons

    Path(out_keep).parent.mkdir(parents=True, exist_ok=True)
    kept.to_csv(out_keep, index=False)
    dropped.to_csv(out_drop, index=False)
    print(f"[clean_apps] kept={len(kept)} dropped={len(dropped)} -> {out_keep} / {out_drop}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_csv", default="data/curated/apps_all.csv")
    ap.add_argument("--out", dest="out_keep", default="data/curated/apps_clean.csv")
    ap.add_argument("--out-dropped", dest="out_drop", default="data/curated/apps_dropped.csv")
    ap.add_argument("--min-rating-count", type=int, default=10)
    ap.add_argument("--min-play-installs", type=int, default=50000)
    ap.add_argument("--min-cws-users", type=int, default=10000)
    ap.add_argument("--min-relevance", type=float, default=0.15)
    args = ap.parse_args()
    main(args.in_csv, args.out_keep, args.out_drop,
         args.min_rating_count, args.min_play_installs, args.min_cws_users,
         args.min_relevance)
