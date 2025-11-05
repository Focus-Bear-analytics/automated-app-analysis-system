# llm/feature_llm.py
from __future__ import annotations
import argparse, os, json, time, re
from pathlib import Path
from typing import Dict, List, Any, Tuple
import pandas as pd

# ---- Config ----
DEFAULT_FEATURES = [
    "blocking","timer","pomodoro","habit_tracking","focus_mode",
    "screen_time","parental_control","adhd_support","reminders","analytics"
]

def _load_csv(p: str, cols: List[str]) -> pd.DataFrame:
    df = pd.read_csv(p)
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise SystemExit(f"{p} missing columns: {missing}")
    return df

def _take(s: str | float | None, n: int) -> str:
    if not isinstance(s, str): return ""
    s = re.sub(r"\s+", " ", s).strip()
    return s[:n]

def build_app_context(app, web_map: Dict[str,str], reviews_map: Dict[str,List[str]]) -> Dict[str,Any]:
    ak = app["app_key"]
    ctx = {
        "app_key": ak,
        "title": _take(str(app.get("title","")), 160),
        "description": _take(str(app.get("description","")), 2000),
        "website_excerpt": _take(web_map.get(ak,""), 4000),
        "sample_reviews": [ _take(t, 600) for t in reviews_map.get(ak, [])[:6] ],
    }
    return ctx

def default_prompt(features: List[str], ctx: Dict[str,Any]) -> str:
    return f"""
You are labeling whether an app supports specific features. Return strict JSON.

Features to check: {features}.

App:
- Title: {ctx['title']}
- Description: {ctx['description']}
- Website: {ctx['website_excerpt']}
- Sample user reviews (may be noisy): {ctx['sample_reviews']}

Rules:
- If evidence is strong the feature exists -> flag=true, confidence 0.7-0.95 (higher if very explicit).
- If clearly NOT present -> flag=false, confidence 0.7-0.9.
- If unclear/insufficient -> flag=false, confidence 0.5.
- Keep evidence short: a few words or a short phrase.
- JSON only. Use this schema:

{{
  "app_key": "{ctx['app_key']}",
  "features": [
    {{"name":"blocking","flag":true/false,"confidence":0.0-1.0,"evidence":"..."}},
    ...
  ]
}}
""".strip()

# ------------- LLM call (OpenAI-compatible Chat Completions) -------------
import requests

def call_chat_completions(base_url: str, api_key: str, model: str, prompt: str,
                          temperature: float=0.0, max_tokens: int=400) -> str:
    url = base_url.rstrip("/") + "/v1/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": model,
        "messages": [{"role":"system","content":"You are a careful labeling assistant."},
                     {"role":"user","content": prompt}],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    r = requests.post(url, headers=headers, json=payload, timeout=60)
    r.raise_for_status()
    data = r.json()
    return data["choices"][0]["message"]["content"]

def _extract_json(s: str) -> dict:
    # Try strict parse; else find the first {...} block
    try:
        return json.loads(s)
    except Exception:
        m = re.search(r"\{(?:[^{}]|(?R))*\}", s, flags=re.S)
        if m:
            try: return json.loads(m.group(0))
            except Exception: pass
    raise ValueError("LLM did not return valid JSON")

def run_llm_labeling(apps_csv: str, web_csv: str, reviews_csv: str,
                     out_csv: str, features: List[str],
                     base_url: str, api_key: str, model: str,
                     batch: int=12, sleep_s: float=0.6, dry: bool=False):
    apps = _load_csv(apps_csv, ["app_key","title","description"])
    web = _load_csv(web_csv, ["app_key","website_text"])
    rev = _load_csv(reviews_csv, ["app_key","body"])

    # pack website text + list of review bodies per app
    web_map = web.groupby("app_key")["website_text"].apply(lambda s: " ".join(s.dropna().astype(str))).to_dict()
    reviews_map: Dict[str,List[str]] = rev.groupby("app_key")["body"].apply(lambda s: [x for x in s.dropna().astype(str).tolist()]).to_dict()

    rows: List[Dict[str,Any]] = []
    for i, t in enumerate(apps.itertuples(index=False), start=1):
        ak = getattr(t,"app_key")
        ctx = build_app_context(t._asdict(), web_map, reviews_map)
        prompt = default_prompt(features, ctx)

        if dry:
            print(f"[DRY] would label {ak}"); 
            continue

        try:
            text = call_chat_completions(base_url, api_key, model, prompt)
            obj = _extract_json(text)
            for f in obj.get("features", []):
                if f.get("name") not in features: 
                    continue
                rows.append({
                    "app_key": ak,
                    "feature": f.get("name"),
                    "llm_flag": bool(f.get("flag")),
                    "llm_confidence": float(f.get("confidence") or 0.0),
                    "llm_evidence": str(f.get("evidence") or "").strip(),
                })
        except Exception as e:
            rows.append({
                "app_key": ak, "feature": None,
                "llm_flag": None, "llm_confidence": 0.0, "llm_evidence": f"ERROR: {e}"
            })
        if (i % batch) == 0:
            time.sleep(sleep_s)

    out = Path(out_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows, columns=["app_key","feature","llm_flag","llm_confidence","llm_evidence"]).to_csv(out, index=False)
    print(f"[llm-features] wrote -> {out}")

def main():
    ap = argparse.ArgumentParser(description="LLM feature extraction (OpenAI-compatible)")
    ap.add_argument("--apps", required=True)
    ap.add_argument("--web", required=True)
    ap.add_argument("--reviews", required=True)
    ap.add_argument("--out", default="data/curated/features_llm.csv")
    ap.add_argument("--features", default=",".join(DEFAULT_FEATURES))
    ap.add_argument("--model", default="gpt-4.1-mini")
    ap.add_argument("--base-url", default=os.environ.get("OPENAI_BASE_URL","https://api.openai.com"))
    ap.add_argument("--api-key", default=os.environ.get("OPENAI_API_KEY",""))
    ap.add_argument("--batch", type=int, default=12)
    ap.add_argument("--sleep", type=float, default=0.6)
    ap.add_argument("--dry", action="store_true")
    args = ap.parse_args()

    feats = [f.strip() for f in args.features.split(",") if f.strip()]
    if not args.api_key:
        raise SystemExit("No API key. Pass --api-key or set OPENAI_API_KEY")

    run_llm_labeling(
        apps_csv=args.apps, web_csv=args.web, reviews_csv=args.reviews,
        out_csv=args.out, features=feats,
        base_url=args.base_url, api_key=args.api_key, model=args.model,
        batch=args.batch, sleep_s=args.sleep, dry=args.dry
    )

if __name__ == "__main__":
    main()
