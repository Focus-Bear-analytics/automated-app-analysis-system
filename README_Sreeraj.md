# Sreeraj Raju – Contribution Report

## Overview

My contribution focused on sentiment analysis enhancement, ADHD behavioural analysis, theme detection, dashboard integration, and LLM-based behavioural classification for focus application reviews.

---

## Work Completed

### RoBERTa Sentiment Analysis

- Replaced VADER sentiment analysis with RoBERTa transformer model.
- Generated Positive, Neutral, and Negative sentiment classifications.
- Improved sentiment classification quality for user reviews.

### Manual Validation

- Created a manual validation dataset.
- Compared model predictions against manually reviewed samples.
- Verified sentiment classification performance.

### ADHD Review Detection

- Developed ADHD-related review filtering pipeline.
- Extracted ADHD-specific reviews from the complete review dataset.
- Analysed ADHD review sentiment distribution.

### ADHD Behavioural Theme Analysis

- Implemented behavioural theme classification.
- Identified common ADHD-related themes:
  - Attention Support
  - Task Management
- Generated behavioural theme visualisations.

### Dashboard Integration

- Added ADHD behavioural analysis section to the Streamlit dashboard.
- Added behavioural insight summaries.
- Added behavioural recommendation section.
- Integrated ADHD visualisations into the Sentiment Analysis page.

### LLM-Based Behaviour Classification

- Implemented zero-shot classification using DistilBART MNLI.
- Identified additional ADHD behavioural patterns that keyword matching could not detect.
- Classified reviews into:
  - Time Management
  - Motivation and Reward
  - Task Management
  - Distraction Management

---

## Challenges Encountered

### Sentiment Model Limitations

- Initial VADER-based sentiment analysis produced inconsistent results for app reviews.
- Resolved by replacing VADER with a transformer-based RoBERTa model.

### ADHD Theme Detection

- Many ADHD-related reviews used indirect language and did not explicitly mention ADHD.
- Required behavioural pattern analysis instead of simple keyword matching.

### LLM Processing Time

- Zero-shot classification required downloading large transformer models.
- Processing time increased significantly compared to rule-based approaches.

### Dashboard Integration

- Multiple visualisation sections required formatting adjustments to maintain dashboard consistency.

---

## Key Improvements Delivered

- Improved sentiment analysis accuracy using RoBERTa.
- Added ADHD-specific behavioural analysis.
- Added behavioural theme detection.
- Added automated behavioural insights.
- Added behavioural recommendations.
- Added LLM-based behavioural classification.
- Expanded analysis beyond basic sentiment detection.

---

## Future Work

- Integrate OpenAI or Gemini APIs for richer behavioural explanations.
- Implement automatic review summarisation.
- Add clustering for behavioural pattern discovery.
- Support multi-app behavioural comparisons.
- Improve behavioural classification accuracy using fine-tuned models.

---

## Files Modified

### Scripts

- scripts/improved_sentiment_analysis.py
- scripts/adhd_sentiment_analysis.py
- scripts/adhd_theme_detection.py
- scripts/llm_adhd_classifier.py

### Dashboard

- app.py

### Generated Datasets

- data/curated/reviews_with_ml_sentiment.csv
- data/curated/adhd_reviews_analysis.csv
- data/curated/adhd_theme_analysis.csv
- data/curated/llm_adhd_behaviour_analysis.csv
