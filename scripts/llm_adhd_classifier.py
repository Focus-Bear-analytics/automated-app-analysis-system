import pandas as pd
from transformers import pipeline

INPUT_FILE = "data/curated/reviews_with_ml_sentiment.csv"
OUTPUT_FILE = "data/curated/llm_adhd_behaviour_analysis.csv"

df = pd.read_csv(INPUT_FILE)

if "text" in df.columns:
    texts = df["text"].fillna("").astype(str)
else:
    title = df["title"].fillna("").astype(str) if "title" in df.columns else ""
    body = df["body"].fillna("").astype(str) if "body" in df.columns else ""
    texts = title + " " + body

candidate_labels = [
    "Attention Regulation",
    "Task Management",
    "Distraction Management",
    "Motivation and Reward",
    "Habit Formation",
    "Time Management",
    "Not ADHD Related"
]

print("Loading zero-shot classification model...")

classifier = pipeline(
    "zero-shot-classification",
    model="valhalla/distilbart-mnli-12-1"
)

def classify_review(text):
    text = str(text).strip()

    if not text or len(text.split()) < 3:
        return "Not ADHD Related"

    try:
        result = classifier(
            text[:512],
            candidate_labels=candidate_labels
        )

        return result["labels"][0]

    except Exception:
        return "Not ADHD Related"

print("Classifying reviews into ADHD behavioural themes...")

df["LLM_ADHD_Theme"] = texts.apply(classify_review)

print("\nLLM ADHD Behaviour Theme Distribution:")
print(df["LLM_ADHD_Theme"].value_counts())

df.to_csv(OUTPUT_FILE, index=False)

print(f"\nLLM ADHD behaviour analysis saved to: {OUTPUT_FILE}")