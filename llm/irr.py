import argparse
from pathlib import Path
import pandas as pd
from sklearn.metrics import cohen_kappa_score

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", default="data/curated/features_llm.csv")
    ap.add_argument("--out", default="data/curated/feature_irr.csv")
    ap.add_argument("--human", default=None, help="Optional CSV with app_key,feature,present")
    args = ap.parse_args()

    p = Path(args.inp)
    if not p.exists():
        print("[irr] SKIP (no features_llm.csv)")
        return
    df = pd.read_csv(p)
    need = {"app_key","feature","present","model"}
    if not need.issubset(df.columns):
        print("[irr] SKIP (bad columns)")
        return

    # pivot model predictions
    piv = df.pivot_table(index=["app_key","feature"], columns="model", values="present", aggfunc="max").fillna(0).astype(int)
    rows = []
    models = [c for c in piv.columns]
    if len(models) >= 2:
        for i in range(len(models)):
            for j in range(i+1, len(models)):
                a, b = models[i], models[j]
                kappa = cohen_kappa_score(piv[a], piv[b])
                rows.append({"rater_a": a, "rater_b": b, "kappa": round(float(kappa), 3), "n": int(len(piv))})

    # optional human
    if args.human and Path(args.human).exists():
        h = pd.read_csv(args.human)  # columns: app_key,feature,present
        hv = h.pivot_table(index=["app_key","feature"], values="present", aggfunc="max")
        for m in models:
            joined = piv[[m]].join(hv, how="inner", rsuffix="_human").dropna()
            if not joined.empty:
                kappa = cohen_kappa_score(joined[m].astype(int), joined["present"].astype(int))
                rows.append({"rater_a": m, "rater_b": "human", "kappa": round(float(kappa), 3), "n": int(len(joined))})

    if not rows:
        print("[irr] SKIP (no pairs)")
        return

    out = Path(args.out)
    pd.DataFrame(rows).to_csv(out, index=False)
    print(f"[irr] wrote -> {out}")

if __name__ == "__main__":
    main()
