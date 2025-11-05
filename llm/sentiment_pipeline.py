# llm/sentiment_pipeline.py
"""
Run sentiment over reviews.csv and produce:
- data/curated/reviews_with_sentiment.csv  (row-level, adds sentiment_score/label)
- data/curated/app_sentiment.csv           (per-app aggregates)

Examples:
# full set (past year, skip ultra-short reviews)
python -m llm.sentiment_pipeline run \
  --in data/curated/reviews.csv \
  --out-reviews data/curated/reviews_with_sentiment.csv \
  --out-apps data/curated/app_sentiment.csv \
  --engine vader \
  --since-days 365 \
  --min-words 3

# only ND-tagged (“special”) reviews
python -m llm.sentiment_pipeline run \
  --in data/curated/reviews.csv \
  --out-reviews data/curated/reviews_with_sentiment__special.csv \
  --out-apps data/curated/app_sentiment__special.csv \
  --engine vader \
  --special-only \
  --since-days 365 \
  --min-words 3
"""

import argparse
import re
from pathlib import Path
from typing import Optional, Tuple, List

import numpy as np
import pandas as pd

# progress bar (noop fallback if tqdm missing)
try:
    from tqdm import tqdm
except Exception:
    def tqdm(x, **k):  # type: ignore
        return x

# -------- text utilities --------

_URL_RX = re.compile(r"https?://\S+|www\.\S+", re.I)

def compose_text(title: Optional[str], body: Optional[str]) -> str:
    t = (title or "").strip()
    b = (body or "").strip()
    if t and b:
        return f"{t}. {b}"
    return t or b

def clean_text(s: str) -> str:
    if not isinstance(s, str):
        return ""
    # strip URLs and compress whitespace
    s = _URL_RX.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

# -------- sentiment engines --------

class VaderEngine:
    def __init__(self):
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        self.an = SentimentIntensityAnalyzer()
    def score(self, text: str) -> Tuple[float, str]:
        vs = self.an.polarity_scores(text or "")
        c = float(vs.get("compound", 0.0))
        label = "positive" if c >= 0.05 else ("negative" if c <= -0.05 else "neutral")
        return c, label

class HFEngine:
    """
    Optional heavy model (will fallback to VADER if transformers/torch missing).
    """
    def __init__(self, model_name: str = "cardiffnlp/twitter-roberta-base-sentiment"):
        try:
            from transformers import pipeline  # type: ignore
        except Exception as e:
            raise RuntimeError("transformers not installed") from e
        self.pipe = pipeline("sentiment-analysis", model=model_name)
    def score(self, text: str) -> Tuple[float, str]:
        if not text:
            return 0.0, "neutral"
        res = self.pipe(text[:512])[0]  # keep it quick
        lab = str(res["label"]).lower()
        sc = float(res.get("score", 0.0))
        if "pos" in lab:
            return sc, "positive"
        if "neg" in lab:
            return -sc, "negative"
        return 0.0, "neutral"

def load_engine(name: str):
    name = (name or "vader").lower()
    if name == "vader":
        return VaderEngine()
    if name in ("hf", "transformer", "roberta"):
        try:
            return HFEngine()
        except Exception:
            # Fall back silently to VADER if HF unavailable
            return VaderEngine()
    return VaderEngine()

# -------- core runner --------

def run_sentiment(
    inp: str,
    out_reviews: str,
    out_apps: str,
    engine_name: str = "vader",
    since_days: Optional[int] = None,
    min_words: int = 0,
    min_chars: int = 0,
    special_only: bool = False,
    drop_neutrals_for_agg: bool = False,
):
    inp_p = Path(inp)
    if not inp_p.exists():
        raise SystemExit(f"Input not found: {inp_p}")

    # read header once to decide usecols robustly
    header_cols: List[str] = pd.read_csv(inp_p, nrows=0).columns.tolist()
    wanted = [
        "app_key","store","app_id","country","lang","review_id",
        "user_name","rating","title","body","version","at","special_reviews"
    ]
    usecols = [c for c in wanted if c in header_cols]

    # stable dtypes to avoid warnings
    df = pd.read_csv(inp_p, usecols=usecols, dtype=str, low_memory=False).fillna("")
    # rating to numeric
    if "rating" in df.columns:
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")

    # optional date filter
    if "at" in df.columns and since_days and since_days > 0:
        ts = pd.to_datetime(df["at"].replace("", np.nan), errors="coerce", utc=True)
        cutoff = pd.Timestamp.utcnow() - pd.Timedelta(days=since_days)
        df = df[ts >= cutoff].copy()

    # compose + clean review text
    titles = df["title"] if "title" in df.columns else ""
    bodies = df["body"] if "body" in df.columns else ""
    df["text"] = [clean_text(compose_text(t, b)) for t, b in zip(titles, bodies)]

    # length filters
    if min_words > 0:
        df = df[df["text"].str.split().str.len().fillna(0) >= min_words]
    if min_chars > 0:
        df = df[df["text"].str.len().fillna(0) >= min_chars]

    # special-only filter
    if special_only and "special_reviews" in df.columns:
        df = df[df["special_reviews"].fillna("").astype(bool)]

    # if nothing left, write empty but valid outputs
    if df.empty:
        Path(out_reviews).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_reviews, index=False)
        pd.DataFrame(columns=[
            "app_key","store","country","n_reviews","n_nd","avg_rating",
            "mean_compound","pct_positive","pct_negative"
        ]).to_csv(out_apps, index=False)
        print("[sentiment] no rows after filters; wrote empty outputs.")
        return

    # engine
    eng = load_engine(engine_name)

    # score
    compounds, labels = [], []
    for txt in tqdm(df["text"].tolist(), desc="Scoring reviews"):
        c, lbl = eng.score(txt)
        compounds.append(c)
        labels.append(lbl)
    df["sentiment_score"] = compounds
    df["sentiment_label"] = labels

    # row-level save (keep nice set of columns if present)
    out_reviews_p = Path(out_reviews)
    out_reviews_p.parent.mkdir(parents=True, exist_ok=True)
    keep_cols = [
        "app_key","store","app_id","country","lang","review_id",
        "user_name","rating","title","body","version","at",
        "special_reviews","sentiment_score","sentiment_label","text"
    ]
    cols_out = [c for c in keep_cols if c in df.columns]
    df[cols_out].to_csv(out_reviews_p, index=False)

    # ----- aggregates (per app_key, store, country) -----
    gcols = [c for c in ["app_key","store","country"] if c in df.columns]
    agg_df = df.copy()

    if drop_neutrals_for_agg:
        agg_df = agg_df[agg_df["sentiment_label"] != "neutral"].copy()

    # make an explicit boolean ND column to avoid lambdas referencing outer vars
    if "special_reviews" in agg_df.columns:
        agg_df["is_nd"] = agg_df["special_reviews"].fillna("").astype(bool)
    else:
        agg_df["is_nd"] = False

    # groupby.agg with named aggregations (no FutureWarning)
    grp = agg_df.groupby(gcols, dropna=False)
    app_agg = grp.agg(
        n_reviews     = ('sentiment_score', 'size'),
        n_nd          = ('is_nd', 'sum'),
        avg_rating    = ('rating', lambda s: pd.to_numeric(s, errors='coerce').mean()),
        mean_compound = ('sentiment_score', 'mean'),
        pct_positive  = ('sentiment_label', lambda s: (s == 'positive').mean()),
        pct_negative  = ('sentiment_label', lambda s: (s == 'negative').mean()),
    ).reset_index()

    out_apps_p = Path(out_apps)
    out_apps_p.parent.mkdir(parents=True, exist_ok=True)
    app_agg.to_csv(out_apps_p, index=False)

    print(f"[sentiment] wrote reviews -> {out_reviews_p}")
    print(f"[sentiment] wrote app aggregates -> {out_apps_p}")

# -------- CLI --------

def main():
    ap = argparse.ArgumentParser(description="Sentiment over reviews.csv (row + app aggregates)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("run")
    p.add_argument("--in", dest="inp", default="data/curated/reviews.csv")
    p.add_argument("--out-reviews", default="data/curated/reviews_with_sentiment.csv")
    p.add_argument("--out-apps", default="data/curated/app_sentiment.csv")
    p.add_argument("--engine", default="vader", help="vader | hf")
    p.add_argument("--since-days", type=int, default=None, help="only include reviews within the last N days")
    p.add_argument("--min-words", type=int, default=0, help="drop reviews with fewer than N words")
    p.add_argument("--min-chars", type=int, default=0, help="drop reviews with fewer than N characters")
    p.add_argument("--special-only", action="store_true", help="keep only rows where special_reviews is truthy")
    p.add_argument("--drop-neutrals-for-agg", action="store_true", help="exclude neutral rows when computing app aggregates")

    args = ap.parse_args()
    if args.cmd == "run":
        run_sentiment(
            inp=args.inp,
            out_reviews=args.out_reviews,
            out_apps=args.out_apps,
            engine_name=args.engine,
            since_days=args.since_days,
            min_words=args.min_words,
            min_chars=args.min_chars,
            special_only=args.special_only,
            drop_neutrals_for_agg=args.drop_neutrals_for_agg,
        )

if __name__ == "__main__":
    main()
