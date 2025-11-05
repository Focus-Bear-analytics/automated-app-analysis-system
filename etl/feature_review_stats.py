import argparse
from pathlib import Path
import pandas as pd
from llm.feature_flags import COMPILED_PATTERNS  # reuse your regex library

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reviews", default="data/curated/reviews_with_sentiment.csv")
    ap.add_argument("--out", default="data/curated/feature_review_stats.csv")
    args = ap.parse_args()

    rpath = Path(args.reviews)
    if not rpath.exists():
        print("[feature-review-stats] SKIP (no reviews)")
        return
    df = pd.read_csv(rpath, usecols=["app_key","rating","sentiment_score","title","body"])
    df["txt"] = (df["title"].fillna("") + " " + df["body"].fillna("")).str.lower()

    rows = []
    for feature, pats in COMPILED_PATTERNS.items():
        def hit(s: str) -> bool:
            for p in pats:
                if p.search(s): return True
            return False
        hits = df[df["txt"].apply(hit)]
        if hits.empty: continue
        agg = hits.groupby("app_key").agg(
            review_hits=("txt","size"),
            avg_rating=("rating","mean"),
            mean_compound=("sentiment_score","mean"),
        ).reset_index()
        agg["feature"] = feature
        rows.append(agg)

    if not rows:
        print("[feature-review-stats] nothing to write")
        return
    out = pd.concat(rows, ignore_index=True)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, index=False)
    print(f"[feature-review-stats] wrote -> {args.out}")

if __name__ == "__main__":
    main()
