import pandas as pd
from textblob import TextBlob

# Loading the existing reviews file
df = pd.read_csv("data/curated/reviews_with_sentiment.csv")

# Function to generate improved sentiment
def get_sentiment(text):
    try:
        polarity = TextBlob(str(text)).sentiment.polarity
        
        if polarity > 0.2:
            return "Positive"
        elif polarity < -0.2:
            return "Negative"
        else:
            return "Neutral"
            
    except:
        return "Neutral"

# Applying to see if new sentiment generates
df["Improved_Sentiment"] = df["text"].apply(get_sentiment)

print("Sentiment distribution:")
print(df['sentiment_label'].value_counts())

# Saving the new file
df.to_csv("data/reviews_with_improved_sentiment.csv", index=False)

print("Improved sentiment file created successfully!")