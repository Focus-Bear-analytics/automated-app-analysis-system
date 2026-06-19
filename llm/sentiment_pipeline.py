"""
llm/sentiment_pipeline.py — v2

3-class sentiment (Positive / Negative / Mixed) for app reviews.
Primary : RoBERTa (cardiffnlp/twitter-roberta-base-sentiment-latest)
Fallback : LLM (OpenAI gpt-4o-mini or Gemini 1.5-flash)

Fixes applied:
✓ Chunked reader — never loads full file into memory
✓ Resume: builds seen-ID set from review_id column only (not full CSV reload)
✓ Output written with csv.QUOTE_ALL — prevents CSV parser errors on reload
✓ 3-class output: Positive / Negative / Mixed
✓ Explicit is_adhd_review boolean (expanded keyword set)
✓ Adds body_len column
"""

import argparse
import csv
import json
import os
import re
import time
from pathlib import Path

import pandas as pd

ADHD_PATTERN = re.compile(
    r"\b(adhd|add\b|a\.d\.h\.d|a\.d\.d|adderall|ritalin|vyvanse|concerta|"
    r"neurodivergent|neurodiversity|neurodivers\w+|nd[- ]?friendly|"
    r"executive function\w*|dopamine|hyperfocus|"
    r"can.?t focus|autism|autistic|asd|asperger|"
    r"working memory|task initiation|brain fog|sensory processing|"
    r"dyslexia|dyscalculia|dyspraxia|tourette)\b",
    re.IGNORECASE,
)

def flag_adhd(text: str) -> bool:
    return bool(ADHD_PATTERN.search(str(text)))

_roberta_pipe = None

def _get_roberta():
    global _roberta_pipe
    if _roberta_pipe is None:
        from transformers import pipeline as hf_pipeline
        _roberta_pipe = hf_pipeline(
            "text-classification",
            model="cardiffnlp/twitter-roberta-base-sentiment-latest",
            truncation=True, max_length=512, top_k=None,
        )
    return _roberta_pipe

ROBERTA_LABEL_MAP = {"positive": "Positive", "negative": "Negative", "neutral": "Mixed"}

def classify_roberta(texts: list, batch_size: int = 64) -> list:
    pipe = _get_roberta()
    results = []
    for i in range(0, len(texts), batch_size):
        for out in pipe(texts[i: i + batch_size]):
            top = max(out, key=lambda x: x["score"])
            label = ROBERTA_LABEL_MAP.get(top["label"].lower(), "Mixed")
            results.append({
                "sentiment_label": label,
                "sentiment_score": round(top["score"], 4)
            })
    return results

LLM_PROMPT = """Classify the sentiment of this app review into exactly one of:
Positive, Negative, or Mixed.

Rules:
- "Positive" = overall happy, recommends the app
- "Negative" = overall unhappy, does NOT recommend
- "Mixed" = balanced or unclear

Return ONLY valid JSON: {{"sentiment": "Positive"|"Negative"|"Mixed", "confidence": "high"|"medium"|"low"}}

Review:
{text}"""

def classify_llm_single(text: str, model: str = "gpt-4o-mini") -> dict:
    prompt = LLM_PROMPT.format(text=str(text)[:2000])
    try:
        if os.environ.get("OPENAI_API_KEY"):
            import openai
            openai.api_key = os.environ["OPENAI_API_KEY"]
            resp = openai.chat.completions.create(
                model=model,
                temperature=0,
                response_format={"type": "json_object"},
                messages=[{"role": "user", "content": prompt}],
            )
            raw = json.loads(resp.choices[0].message.content)
        elif os.environ.get("GEMINI_API_KEY"):
            import google.generativeai as genai
            genai.configure(api_key=os.environ["GEMINI_API_KEY"])
            raw_text = (
                genai.GenerativeModel("gemini-1.5-flash")
                .generate_content(prompt).text
                .strip().lstrip("```json").lstrip("```").rstrip("```").strip()
            )
            raw = json.loads(raw_text)
        else:
            return {"sentiment_label": "Mixed", "sentiment_score": 0.0}

        label = raw.get("sentiment", "Mixed")
        if label not in {"Positive", "Negative", "Mixed"}:
            label = "Mixed"
        score = {"high": 0.9, "medium": 0.7, "low": 0.5}.get(
            raw.get("confidence", "medium"), 0.7
        )
        return {"sentiment_label": label, "sentiment_score": score}
    except Exception as e:
        return {"sentiment_label": "Mixed", "sentiment_score": 0.0, "error": str(e)}

def iter_jsonl(path: Path, chunksize: int):
    buf = []
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                buf.append(json.loads(line))
            except json.JSONDecodeError:
                continue
            if len(buf) >= chunksize:
                yield pd.DataFrame(buf)
                buf = []
    if buf:
        yield pd.DataFrame(buf)

def iter_csv(path: Path, chunksize: int):
    for chunk in pd.read_csv(path, chunksize=chunksize, low_memory=False, on_bad_lines="skip"):
        yield chunk

def load_done_ids(out_path: Path) -> set:
    done: set = set()
    if not out_path.exists():
        return done
    try:
        col_df = pd.read_csv(out_path, usecols=["review_id"], low_memory=False, on_bad_lines="skip")
        return set(col_df["review_id"].dropna().astype(str).tolist())
    except Exception:
        pass
    try:
        with out_path.open("r", encoding="utf-8", errors="replace") as f:
            header = f.readline().strip().split(",")
            if "review_id" in header:
                idx = header.index("review_id")
                for line in f:
                    parts = line.strip().split(",")
                    if len(parts) > idx:
                        done.add(parts[idx].strip('"'))
    except Exception:
        pass
    return done

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", dest="out", default="data/curated/reviews_with_sentiment.csv")
    ap.add_argument("--backend", choices=["roberta", "llm"], default="roberta")
    ap.add_argument("--min-body-len", type=int, default=15)
    ap.add_argument("--chunksize", type=int, default=10_000)
    ap.add_argument("--llm-model", default="gpt-4o-mini")
    ap.add_argument("--llm-sleep", type=float, default=0.05)
    ap.add_argument("--resume", action="store_true")
    args = ap.parse_args()

    inp = Path(args.inp)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    done_ids: set = set()
    write_header: bool = True

    if args.resume and out_path.exists():
        done_ids = load_done_ids(out_path)
        write_header = False
        print(f"[sentiment] resuming — {len(done_ids):,} already done")

    if not inp.exists():
        print(f"[sentiment] ERROR: input not found: {inp}")
        return

    reader = iter_jsonl if inp.suffix.lower() in {".jsonl", ".txt"} else iter_csv
    chunk_n = total_written = 0

    with out_path.open("a", encoding="utf-8", newline="") as fout:
        writer = None

        for chunk in reader(inp, args.chunksize):
            chunk_n += 1
            chunk.columns = [c.strip() for c in chunk.columns]

            if "body" not in chunk.columns and "text" in chunk.columns:
                chunk = chunk.rename(columns={"text": "body"})
            if "body" not in chunk.columns:
                continue

            chunk = chunk.dropna(subset=["body"]).copy()
            chunk["body"] = chunk["body"].astype(str)
            chunk = chunk[chunk["body"].str.len() >= args.min_body_len]

            if "review_id" in chunk.columns and done_ids:
                chunk = chunk[~chunk["review_id"].astype(str).isin(done_ids)]
            if chunk.empty:
                continue

            chunk["is_adhd_review"] = chunk["body"].apply(flag_adhd)
            chunk["body_len"] = chunk["body"].str.len()

            texts = chunk["body"].tolist()
            if args.backend == "roberta":
                sent = classify_roberta(texts)
            else:
                sent = []
                for t in texts:
                    sent.append(classify_llm_single(t, model=args.llm_model))
                    time.sleep(args.llm_sleep)

            chunk["sentiment_label"] = [r["sentiment_label"] for r in sent]
            chunk["sentiment_score"] = [r.get("sentiment_score", 0.0) for r in sent]

            if writer is None:
                writer = csv.DictWriter(
                    fout,
                    fieldnames=list(chunk.columns),
                    quoting=csv.QUOTE_ALL,
                    extrasaction="ignore",
                    lineterminator="\n",
                )
                if write_header:
                    writer.writeheader()

            for _, row in chunk.iterrows():
                writer.writerow(row.to_dict())

            total_written += len(chunk)
            print(f"[sentiment] chunk {chunk_n}: +{len(chunk):,} (total: {total_written:,})")

    print(f"[sentiment] done — {total_written:,} rows → {out_path}")

if __name__ == "__main__":
    main()