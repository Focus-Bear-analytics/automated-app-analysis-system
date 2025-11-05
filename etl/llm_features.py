# etl/llm_features.py
import argparse, os, time, json, re
from pathlib import Path

import pandas as pd
from tqdm import tqdm
import httpx

# ------------ text helpers ------------
def coerce_text(x) -> str:
    """Robustly turn any value (incl. NaN) into a safe string."""
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x)

def trim(x, limit: int | None) -> str:
    t = coerce_text(x).strip()
    return t if limit is None else t[:limit]

# ------------ prompting ------------
PROMPT_TEMPLATE = """You are an analyst classifying app features using the Goldilocks Support model.
Extract concrete product features and map each to one of:
- strong_support, neutral, undermining (how it affects self-regulation)

APP: {title}

WEBSITE TEXT (snippets):
{website_text}

STORE DESCRIPTION (snippets):
{store_desc}

Return STRICT JSON with keys:
- "features": string[]  (feature names)
- "goldilocks_support": object   (feature -> "strong_support" | "neutral" | "undermining")
- "summary": string  (1-2 sentence overview)
"""

def make_prompt(title: str, website_text: str, store_desc: str) -> str:
    return PROMPT_TEMPLATE.format(
        title=trim(title, 120),
        website_text=trim(website_text, 3500),
        store_desc=trim(store_desc, 1200),
    )

# ------------ model backends ------------
def ask_ollama(model: str, prompt: str, temperature: float = 0.2, timeout=300) -> str:
    host = os.environ.get("OLLAMA_HOST", "http://127.0.0.1:11434").rstrip("/")
    url = f"{host}/api/generate"
    payload = {
        "model": model,
        "prompt": prompt,
        "options": {"temperature": temperature},
        "stream": False,
    }
    with httpx.Client(timeout=timeout) as c:
        r = c.post(url, json=payload)
        r.raise_for_status()
        data = r.json()
    return coerce_text(data.get("response", "")).strip()

def ask_openai(model: str, prompt: str, temperature: float = 0.2, timeout=180) -> str:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set")
    url = "https://api.openai.com/v1/chat/completions"
    payload = {
        "model": model,  # e.g., "gpt-4o-mini"
        "messages": [{"role": "user", "content": prompt}],
        "temperature": temperature,
    }
    headers = {"Authorization": f"Bearer {api_key}"}
    with httpx.Client(timeout=timeout) as c:
        r = c.post(url, json=payload, headers=headers)
        r.raise_for_status()
        data = r.json()
    return data["choices"][0]["message"]["content"]

def ask_gemini(model: str, prompt: str, temperature: float = 0.2, timeout=180) -> str:
    api_key = os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        raise RuntimeError("GOOGLE_API_KEY not set")
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": temperature},
    }
    with httpx.Client(timeout=timeout) as c:
        r = c.post(url, json=payload)
        r.raise_for_status()
        data = r.json()
    return data["candidates"][0]["content"]["parts"][0]["text"]

# ------------ response parsing ------------
def parse_response(text: str) -> dict:
    """Pull a JSON object out of the model response, or fall back to a minimal structure."""
    text = coerce_text(text)
    # fenced JSON
    m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, flags=re.S)
    if not m:
        # any lone JSON object
        m = re.search(r"(\{.*\})", text, flags=re.S)
    if m:
        try:
            return json.loads(m.group(1))
        except Exception:
            pass
    return {"features": [], "goldilocks_support": {}, "summary": text[:2000]}

# ------------ main ------------
def main(apps_csv, web_csv, out_csv, model, vendor, max_apps, sleep, resume):
    apps = pd.read_csv(apps_csv, dtype=str)
    web = pd.read_csv(web_csv, dtype=str)

    # Keep only what we need from websites and de-dupe per app
    web = web[["app_key", "website_text"]].drop_duplicates("app_key")

    df = apps.merge(web, on="app_key", how="left")
    if "description" not in df.columns:
        df["description"] = ""
    # Robust string columns
    df["title"] = df["title"].map(coerce_text)
    df["description"] = df["description"].map(coerce_text)
    df["website_text"] = df["website_text"].map(coerce_text)

    # Resume support
    done = set()
    out_path = Path(out_csv)
    if resume and out_path.exists():
        try:
            prev = pd.read_csv(out_path, usecols=["app_key"], dtype=str)
            done = set(prev["app_key"].dropna().astype(str))
        except Exception:
            done = set()

    rows = []
    total = len(df) if not max_apps else min(max_apps, len(df))

    for _, r in tqdm(df.head(total).iterrows(), total=total, desc="LLM features"):
        ak = r["app_key"]
        if resume and ak in done:
            continue

        prompt = make_prompt(r["title"], r["website_text"], r["description"])

        if vendor == "ollama":
            resp = ask_ollama(model=model, prompt=prompt)
        elif vendor == "openai":
            resp = ask_openai(model=model, prompt=prompt)
        elif vendor == "gemini":
            resp = ask_gemini(model=model, prompt=prompt)
        else:
            raise SystemExit(f"Unknown --vendor {vendor}")

        parsed = parse_response(resp)
        rows.append({
            "app_key": ak,
            "title": r["title"],
            "vendor": vendor,
            "model": model,
            "features_json": json.dumps(parsed, ensure_ascii=False),
            "raw": resp,
        })
        time.sleep(sleep)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(out_path, index=False)
    print(f"[features] wrote {len(rows)} rows -> {out_path}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apps", default="data/curated/apps.csv")
    ap.add_argument("--web",  default="data/curated/websites.csv")
    ap.add_argument("--out",  default="data/curated/features.csv")
    ap.add_argument("--vendor", default="ollama", choices=["ollama","openai","gemini"])
    ap.add_argument("--model", default="deepseek-llm:7b")
    ap.add_argument("--max", type=int, default=None)
    ap.add_argument("--sleep", type=float, default=0.5)
    ap.add_argument("--resume", action="store_true")
    args = ap.parse_args()
    main(args.apps, args.web, args.out, args.model, args.vendor, args.max, args.sleep, args.resume)
