import pandas as pd
from transformers import pipeline

INPUT_FILE = "data/curated/reviews_with_sentiment.csv"
OUTPUT_FILE = "data/curated/reviews_with_ml_sentiment.csv"

df = pd.read_csv(INPUT_FILE)

# Use text column if available, otherwise combine title + body
if "text" in df.columns:
    texts = df["text"].fillna("").astype(str)
else:
    title = df["title"].fillna("").astype(str) if "title" in df.columns else ""
    body = df["body"].fillna("").astype(str) if "body" in df.columns else ""
    texts = title + " " + body

print("Loading ML sentiment model...")

sentiment_model = pipeline(
    "sentiment-analysis",
    model="cardiffnlp/twitter-roberta-base-sentiment-latest",
    truncation=True
)

def predict_sentiment(text):
    text = str(text).strip()
    if not text:
        return "Neutral"

    try:
        result = sentiment_model(text[:512])[0]
        label = result["label"].lower()

        if "positive" in label:
            return "Positive"
        elif "negative" in label:
            return "Negative"
        else:
            return "Neutral"
    except Exception:
        return "Neutral"

print("Running ML sentiment prediction...")

df["ML_Sentiment"] = texts.apply(predict_sentiment)

print("ML Sentiment distribution:")
print(df["ML_Sentiment"].value_counts())

df.to_csv(OUTPUT_FILE, index=False)

print(f"ML sentiment file created successfully: {OUTPUT_FILE}")