import pandas as pd

INPUT_FILE = "data/curated/adhd_reviews_analysis.csv"
OUTPUT_FILE = "data/curated/adhd_theme_analysis.csv"

# Load ADHD review dataset
df = pd.read_csv(INPUT_FILE)

# Combine text safely
if "text" in df.columns:
    df["combined_text"] = df["text"].fillna("").astype(str)
else:
    title = df["title"].fillna("").astype(str) if "title" in df.columns else ""
    body = df["body"].fillna("").astype(str) if "body" in df.columns else ""
    df["combined_text"] = title + " " + body

# ADHD-related theme categories
themes = {

    "Attention Support": [
        "focus",
        "focused",
        "attention",
        "concentrate",
        "concentration",
        "study",
        "task",
        "stay on track"
    ],

    "Task Management": [
        "productive",
        "productivity",
        "efficient",
        "workflow",
        "routine",
        "planning",
        "organised",
        "organized"
    ],

    "Behavioural Motivation": [
        "dopamine",
        "motivation",
        "motivated",
        "reward",
        "habit",
        "discipline",
        "rewarding"
    ],

    "Distraction Complaint": [
        "distracting",
        "distraction",
        "interrupt",
        "notification"
    ],

    "Subscription Complaint": [
        "subscription",
        "premium",
        "expensive",
        "price",
        "paywall"
    ],

    "Performance Issue": [
        "bug",
        "crash",
        "freeze",
        "glitch",
        "slow",
        "lag"
    ]
}

# Theme detection function
def detect_theme(text):

    text = str(text).lower()

    for theme, keywords in themes.items():

        for keyword in keywords:

            if keyword in text:
                return theme

    return "Other"

# Apply theme detection
df["ADHD_Theme"] = df["combined_text"].apply(detect_theme)

# Print theme counts
print("\nADHD Theme Distribution:\n")
print(df["ADHD_Theme"].value_counts())

# Save results
df.to_csv(OUTPUT_FILE, index=False)

print(f"\nTheme analysis file saved to: {OUTPUT_FILE}")