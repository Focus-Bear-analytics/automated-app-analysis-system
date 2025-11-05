import argparse
from pathlib import Path
import pandas as pd
from collections import Counter

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", default="data/curated/review_labels.csv")
    ap.add_argument("--out", default="data/curated/review_app_insights.csv")
    args = ap.parse_args()

    p = Path(args.inp)
    if not p.exists():
        print("[agg-insights] SKIP (no review_labels.csv)")
        return

    df = pd.read_csv(p)
    rows = []
    for ak, g in df.groupby("app_key"):
        type_counts = g["type"].value_counts().to_dict()
        aspects = [a for s in g["aspects"].fillna("") for a in str(s).split(";") if a]
        top_aspects = [k for k,_ in Counter(aspects).most_common(8)]
        rows.append({
            "app_key": ak,
            "n_reviews_labeled": int(len(g)),
            "pains": int(type_counts.get("pain",0)),
            "praises": int(type_counts.get("praise",0)),
            "unmet": int(type_counts.get("unmet",0)),
            "top_aspects": ";".join(top_aspects),
        })

    out = Path(args.out)
    pd.DataFrame(rows).to_csv(out, index=False)
    print(f"[agg-insights] wrote -> {out}")

if __name__ == "__main__":
    main()
