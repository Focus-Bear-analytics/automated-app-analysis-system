import argparse, json, os
from pathlib import Path
import pandas as pd
from . import providers

def load_taxonomy(path: Path) -> dict:
    import yaml
    obj = yaml.safe_load(path.read_text())
    feats = list(obj["features"].keys())
    return {"features": feats, "definitions": obj["features"]}

def build_text_row(title, desc, website_text):
    parts = []
    if isinstance(title,str) and title.strip(): parts.append(f"TITLE: {title}")
    if isinstance(desc,str) and desc.strip(): parts.append(f"DESCRIPTION: {desc}")
    if isinstance(website_text,str) and website_text.strip(): parts.append(f"WEBSITE: {website_text}")
    return "\n\n".join(parts)[:12000]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apps", required=True)
    ap.add_argument("--web", required=True)
    ap.add_argument("--taxonomy", default="llm/taxonomy.yml")
    ap.add_argument("--out", default="data/curated/features_llm.csv")
    ap.add_argument("--openai-model", default="gpt-4o-mini")
    ap.add_argument("--gemini-model", default="gemini-1.5-flash")
    args = ap.parse_args()

    apps = pd.read_csv(args.apps)
    web  = pd.read_csv(args.web) if Path(args.web).exists() else pd.DataFrame({"app_key":[], "website_text":[]})
    if "website_text" not in web.columns:
        # handle your scraper output columns
        if any(c.startswith("website_text") for c in web.columns):
            c = [c for c in web.columns if c.startswith("website_text")][0]
            web = web.rename(columns={c:"website_text"})
        else:
            web["website_text"] = ""

    df = apps.merge(web[["app_key","website_text"]], on="app_key", how="left")
    tax = load_taxonomy(Path(args.taxonomy))
    feats = tax["features"]

    # Decide which provider(s) are available
    use_openai = providers.have_openai()
    use_gemini = providers.have_gemini()
    if not (use_openai or use_gemini):
        print("[feature-llm] SKIP (no OPENAI_API_KEY or GEMINI_API_KEY)")
        return

    rows = []
    for t in df.itertuples(index=False):
        text = build_text_row(getattr(t,"title",""), getattr(t,"description",""), getattr(t,"website_text",""))
        merged = {}
        if use_openai:
            r = providers.classify_with_openai(text, tax, model=args.openai_model)
            if isinstance(r, dict): merged["openai"] = r
        if use_gemini:
            r = providers.classify_with_gemini(text, tax, model=args.gemini_model)
            if isinstance(r, dict): merged["gemini"] = r

        # collapse into one row per (app,feature,model)
        for model_name, payload in merged.items():
            for f in feats:
                rec = payload.get(f, {})
                rows.append({
                    "app_key": getattr(t,"app_key"),
                    "feature": f,
                    "present": int(bool(rec.get("present", False))),
                    "confidence": float(rec.get("confidence", 0.0)),
                    "model": model_name,
                })

    if not rows:
        print("[feature-llm] nothing produced (provider returned empty?)")
        return

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(out, index=False)
    print(f"[feature-llm] wrote -> {out}")

if __name__ == "__main__":
    main()
