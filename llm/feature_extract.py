# llm/feature_extract.py
import os, json, time, argparse, math, re
from pathlib import Path
from typing import Dict, Any, List, Tuple

import pandas as pd

# ---------- LLM backends (OpenAI / Gemini) ----------
def call_openai(prompt: str, model: str = "gpt-4.1-mini") -> str:
    from openai import OpenAI
    api = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    r = api.chat.completions.create(
        model=model,
        messages=[
            {"role":"system","content":
             "You are a careful researcher. Output ONLY valid JSON that matches the schema."},
            {"role":"user","content": prompt}
        ],
        temperature=0.2,
    )
    return r.choices[0].message.content

def call_gemini(prompt: str, model: str = "gemini-1.5-flash"):
    import google.generativeai as genai
    genai.configure(api_key=os.environ.get("GOOGLE_API_KEY"))
    m = genai.GenerativeModel(model)
    r = m.generate_content(prompt)
    return r.text

def call_llm(prompt: str, model_spec: str) -> str:
    """
    model_spec examples:
      openai:gpt-4.1-mini
      openai:gpt-4o-mini
      gemini:gemini-1.5-flash
    """
    vendor, model = (model_spec.split(":",1)+[""])[:2]
    if vendor == "gemini":
        return call_gemini(prompt, model)
    return call_openai(prompt, model)

# ---------- helpers ----------
def norm_text(x):
    if pd.isna(x): return ""
    s = str(x).strip()
    return re.sub(r"\s+", " ", s)

TAXON = [
    "Core Focus Tools",      # blockers, timers, pomodoro, DND, app limits
    "Motivation Tools",      # points, streaks, rewards, pets, visuals
    "Guidance Tools",        # routines, nudges, task breakdown, prompts
    "Tracking Tools",        # analytics, screen-time, reports, history
    "Accessibility Tools",   # ND-friendly design, flexibility, pacing, sensory
]

JSON_SCHEMA = """
Return JSON with this exact shape:
{
  "app_key": "<play:pkg | ios:id123 | cws:xxxx>",
  "features": [
    {
      "name": "<short feature>",
      "category": "Core Focus Tools|Motivation Tools|Guidance Tools|Tracking Tools|Accessibility Tools",
      "support": "supportive|undermining|neutral",
      "nd_specific": true|false,
      "evidence": "<1-2 sentence justification based on the text>"
    }
  ],
  "notes": "<optional extra notes>",
  "goldilocks_score": {
    "supportiveness_mean": -1.0,
    "supportiveness_explain": "<how you judged supportiveness overall>"
  }
}
Rules:
- Be conservative. If text is vague, mark neutral.
- Prefer fewer, high-confidence features (3–10).
- If you infer an undermining pattern (e.g., lootboxy mechanics, excessive notifications), justify it.
- Output JSON ONLY. No markdown or prose.
"""

def build_prompt(row: pd.Series) -> str:
    title = norm_text(row.get("title"))
    desc  = norm_text(row.get("description"))
    price = norm_text(row.get("pricing_raw"))
    iap   = []
    if not pd.isna(row.get("iap_min")): iap.append(f"min ${row['iap_min']:.2f}")
    if not pd.isna(row.get("iap_max")): iap.append(f"max ${row['iap_max']:.2f}")
    iap_str = ", ".join(iap) if iap else "n/a"
    ctx = f"""App: {title}
Store: {row.get('store')}
Category: {row.get('category')}
Pricing: {price or 'n/a'} | IAP range: {iap_str}
Store description (may contain noise): {desc[:4000]}
----
Classify features using the Goldilocks Support taxonomy:
- {", ".join(TAXON)}.
For each feature, judge whether it is supportive, undermining, or neutral for self-regulation.
Also flag whether it seems ND-specific (explicitly mentions ADHD/autism/flexibility for sensory/attention differences etc.)
{JSON_SCHEMA}
"""
    return ctx

def safe_json(s: str) -> Dict[str, Any]:
    # strip code fences if the model adds them
    s = s.strip()
    s = re.sub(r"^```(json)?", "", s).strip()
    s = re.sub(r"```$", "", s).strip()
    return json.loads(s)

def to_rows(app_key: str, obj: Dict[str, Any]) -> List[Dict[str, Any]]:
    feats = obj.get("features", []) or []
    rows = []
    for f in feats:
        sup = f.get("support","neutral")
        val = {"supportive": 1, "neutral": 0, "undermining": -1}.get(sup, 0)
        rows.append({
            "app_key": app_key,
            "feature_name": f.get("name","").strip()[:100],
            "category": f.get("category",""),
            "support": sup,
            "support_val": val,
            "nd_specific": bool(f.get("nd_specific", False)),
            "evidence": f.get("evidence",""),
        })
    return rows

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apps", default="data/curated/apps.csv")
    ap.add_argument("--out-jsonl", default="data/curated/features.jsonl")
    ap.add_argument("--out-csv",   default="data/curated/features.csv")
    ap.add_argument("--model", default="openai:gpt-4.1-mini")
    ap.add_argument("--max-apps", type=int, default=99999)
    ap.add_argument("--sleep", type=float, default=0.6)
    args = ap.parse_args()

    apps = pd.read_csv(args.apps)
    need_cols = {"app_key","title","description","store","category","pricing_raw","iap_min","iap_max"}
    missing = [c for c in need_cols if c not in apps.columns]
    if missing:
        raise SystemExit(f"apps.csv missing {missing}")

    out_jsonl = Path(args.out_jsonl); out_jsonl.parent.mkdir(parents=True, exist_ok=True)
    out_csv   = Path(args.out_csv)

    all_rows: List[Dict[str,Any]] = []
    with out_jsonl.open("w", encoding="utf-8") as fjson:
        for i, row in enumerate(apps.itertuples(index=False), start=1):
            if i > args.max_apps: break
            ak = getattr(row, "app_key")
            prompt = build_prompt(pd.Series(row._asdict()))
            try:
                txt = call_llm(prompt, args.model)
                obj = safe_json(txt)
                if not obj.get("app_key"):
                    obj["app_key"] = ak
            except Exception as e:
                # fallback: very safe neutral stub
                obj = {
                    "app_key": ak,
                    "features": [],
                    "notes": f"LLM error: {e}",
                    "goldilocks_score": {"supportiveness_mean": 0.0, "supportiveness_explain": "fallback"}
                }
            # write jsonl
            fjson.write(json.dumps(obj, ensure_ascii=False) + "\n")
            # collect flat rows
            all_rows.extend(to_rows(ak, obj))
            time.sleep(args.sleep)

    # flat CSV + per-app summary
    feat_df = pd.DataFrame(all_rows)
    feat_df.to_csv(out_csv, index=False)

    # quick rollup for later plotting (optional convenience)
    if not feat_df.empty:
        roll = (feat_df.groupby(["app_key","category"], as_index=False)
                .agg(features=("feature_name","nunique"),
                     supportiveness=("support_val","mean"),
                     nd_share=("nd_specific","mean")))
        roll.to_csv(out_csv.replace(".csv","_rollup.csv"), index=False)

    print(f"[features] wrote -> {out_jsonl} and {out_csv}")

if __name__ == "__main__":
    main()
