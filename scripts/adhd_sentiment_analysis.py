import pandas as pd

INPUT_FILE = "data/curated/reviews_with_ml_sentiment.csv"
OUTPUT_FILE = "data/curated/adhd_reviews_analysis.csv"

df = pd.read_csv(INPUT_FILE)

# ADHD / focus related keywords
keywords = [
    "adhd",
    "focus",
    "focused",
    "attention",
    "concentration",
    "productive",
    "productivity",
    "distracting",
    "distraction",
    "dopamine"
]

if "text" in df.columns:
    df["combined_text"] = df["text"].fillna("").astype(str)
else:
    title = df["title"].fillna("").astype(str) if "title" in df.columns else ""
    body = df["body"].fillna("").astype(str) if "body" in df.columns else ""
    df["combined_text"] = title + " " + body

pattern = "|".join(keywords)

adhd_reviews = df[
    df["combined_text"].str.lower().str.contains(pattern, na=False)
]

print("\nADHD / Focus Related Reviews Found:")
print(len(adhd_reviews))

print("\nSentiment Distribution:")
print(adhd_reviews["ML_Sentiment"].value_counts())

# Save filtered dataset
adhd_reviews.to_csv(OUTPUT_FILE, index=False)

print(f"\nFiltered ADHD review dataset saved to: {OUTPUT_FILE}")