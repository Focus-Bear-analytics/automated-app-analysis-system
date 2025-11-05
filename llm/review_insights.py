import argparse, os, json
from pathlib import Path
import pandas as pd
from . import providers

PROMPT = """Label the review.
Return JSON: {"type": "praise|pain|unmet", "aspects": ["..."], "summary": "1 sentence"}.
Review:
"""

def ask_openai(txt: str, model="gpt-4o-mini"):
    import openai
    openai.api_key = os.environ["OPENAI_API_KEY"]
    r = openai.chat.completions.create(
        model=model, temperature=0,
        response_format={"type":"json_object"},
        messages=[{"role":"user","content":PROMPT + txt[:4000]}]
    )
    import json
    return json.loads(r.choices[0].message.content)

def ask_gemini(txt: str, model="gemini-1.5-flash"):
    import google.generativeai as genai
    genai.configure(api_key=os.environ["GEMINI_API_KEY"])
    out = genai.GenerativeModel(model).generate_content(PROMPT + txt[:4000])
    import json
    return json.loads(out.text)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", default="data/curated/reviews_with_sentiment.csv")
    ap.add_argument("--out", default="data/curated/review_labels.csv")
    ap.add_argument("--max-per-app", type=int, default=60)
    ap.add_argument("--openai-model", default="gpt-4o-mini")
    ap.add_argument("--gemini-model", default="gemini-1.5-flash")
    args = ap.parse_args()

    p = Path(args.inp)
    if not p.exists():
        print("[review-llm] SKIP (no reviews)")
        return
    have = providers.have_openai() or providers.have_gemini()
    if not have:
        print("[review-llm] SKIP (no API keys)")
        return

    df = pd.read_csv(p, usecols=["app_key","body","rating","sentiment_label"]).dropna(subset=["body"])
    # sample per app for cost
    sampled = df.groupby("app_key").head(args.max_per_app).copy()

    rows = []
    for t in sampled.itertuples(index=False):
        text = str(getattr(t,"body",""))
        try:
            if providers.have_openai():
                ans = ask_openai(text, model=args.openai_model)
            else:
                ans = ask_gemini(text, model=args.gemini_model)
        except Exception:
            ans = {}
        rows.append({
            "app_key": getattr(t,"app_key"),
            "type": ans.get("type"),
            "aspects": ";".join(ans.get("aspects", [])),
            "summary": ans.get("summary"),
            "rating": getattr(t,"rating"),
            "sentiment_label": getattr(t,"sentiment_label"),
        })

    out = Path(args.out)
    pd.DataFrame(rows).to_csv(out, index=False)
    print(f"[review-llm] wrote -> {out}")

if __name__ == "__main__":
    main()
