import pandas as pd
# pandas is a library that helps us read and work with tables of data (like Excel sheets)


# This list will hold every review we collect from all the files
all_reviews = []

# This set will help us remember which reviews we've already seen,
# so we don't accidentally add the same review twice
seen_before = set()


def read_reviews_with_body_column(file_name, source_name):
    # This function reads a file where the review text is stored in a column called "body"

    # Check if the file is a CSV or a JSONL file, and read it the right way
    if file_name.endswith(".csv"):
        data = pd.read_csv(file_name)
    else:
        data = pd.read_json(file_name, lines=True)

    # Go through the file one row (one review) at a time
    for i in range(len(data)):
        row = data.iloc[i]

        # Pull out the pieces of information we care about
        app_id = row["app_id"]
        title = row["title"]
        body = row["body"]

        # If title or body is empty/missing, just treat it as an empty piece of text
        if pd.isna(title):
            title = ""
        if pd.isna(body):
            body = ""

        # Make sure both are proper text (strings), not numbers or other types
        title = str(title)
        body = str(body)

        # If there's no text at all in this review, skip it — nothing to check
        if title == "" and body == "":
            continue

        # Otherwise, try adding this review to our final list
        add_review_if_new(source_name, app_id, title, body, row.get("store"), row.get("rating"), row.get("review_id"))


def read_reviews_with_content_column(file_name, source_name):
    # Same idea as above, but for files that call the review text "content" instead of "body"

    data = pd.read_csv(file_name)

    for i in range(len(data)):
        row = data.iloc[i]

        app_id = row["app_id"]
        title = row["title"]
        body = row["content"]  # this file uses "content" as the column name for the review text

        if pd.isna(title):
            title = ""
        if pd.isna(body):
            body = ""

        title = str(title)
        body = str(body)

        if title == "" and body == "":
            continue

        add_review_if_new(source_name, app_id, title, body, row.get("platform"), row.get("score"), row.get("review_id"))


def add_review_if_new(source_name, app_id, title, body, store, rating, review_id):
    # If this review has a real ID, use that as the fingerprint — it's the most reliable
    # way to know two rows are truly the same review.
    if pd.isna(review_id) or str(review_id).strip() == "":
        # No ID available, so fall back to matching by app + text (less reliable, but the
        # only option we have for this particular row).
        combined_text = (title + " " + body).strip().lower()
        fingerprint = str(app_id) + "||TEXT||" + combined_text
    else:
        fingerprint = str(app_id) + "||ID||" + str(review_id)

    if fingerprint in seen_before:
        return

    seen_before.add(fingerprint)

    all_reviews.append({
        "source_file": source_name,
        "store": store,
        "app_id": app_id,
        "title": title,
        "body": body,
        "rating": rating,
        "review_id": review_id,
    })

# ---- Step 1: Read every review file we know about ----
# Some files store review text under "body", others under "content" — we handle both kinds

read_reviews_with_body_column("data/reviews/reviews.jsonl", "reviews.jsonl")
read_reviews_with_body_column("data/curated/reviews.csv", "reviews.csv")
read_reviews_with_body_column("data/curated/manual_validation_sample.csv", "manual_validation_sample.csv")

read_reviews_with_content_column("data/curated/playstore_reviews_sentiment.csv", "playstore_reviews_sentiment.csv")
read_reviews_with_content_column("data/curated/ios_reviews_sentiment.csv", "ios_reviews_sentiment.csv")


# ---- Step 2: Show how many unique reviews we ended up with ----

print("Total unique reviews collected:", len(all_reviews))


# ---- Step 3: Save everything into one single CSV file ----

final_table = pd.DataFrame(all_reviews)          # turn our list of reviews into a proper table
final_table.to_csv("data/curated/all_reviews_export.csv", index=False)  # save that table as a CSV file

print("Saved to data/curated/all_reviews_export.csv")
