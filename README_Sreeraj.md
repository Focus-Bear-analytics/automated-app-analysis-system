# Automated App Analysis System – Sentiment Analysis and ADHD Behavioural Analysis Development README

## 1. Project Overview

This development work focused on improving review analysis within the Automated App Analysis System. The primary objectives were to enhance sentiment analysis accuracy, identify ADHD-related user behaviour patterns, and extend behavioural analysis using Large Language Models (LLMs).

The system analyses app reviews collected from focus and productivity applications and generates insights that can be displayed through the Streamlit dashboard.

---

## 2. Main Development Goal

The main goals of this development stage were:

1. Replace the existing VADER sentiment analysis approach with a transformer-based model.
2. Improve sentiment classification quality for app reviews.
3. Detect ADHD-related reviews from the review dataset.
4. Analyse behavioural themes present in ADHD-related reviews.
5. Generate behavioural insights and recommendations.
6. Extend behavioural analysis using LLM-based classification techniques.
7. Integrate all results into the Streamlit dashboard.

---

## 3. Files Updated or Created

### Scripts

- scripts/improved_sentiment_analysis.py
- scripts/adhd_sentiment_analysis.py
- scripts/adhd_theme_detection.py
- scripts/llm_adhd_classifier.py

### Dashboard

- app.py

### Generated Data Files

- data/curated/reviews_with_ml_sentiment.csv
- data/curated/adhd_reviews_analysis.csv
- data/curated/adhd_theme_analysis.csv
- data/curated/llm_adhd_behaviour_analysis.csv

---

## 4. Sentiment Analysis Improvement

The original sentiment analysis system used the VADER sentiment analyser.

Several limitations were identified:

- Difficulty handling app-review language.
- Inconsistent classification of short reviews.
- Reduced effectiveness on modern review text.

To address this issue, the sentiment analysis pipeline was upgraded to use a transformer-based RoBERTa model.

### Improvements

- Replaced VADER with RoBERTa.
- Generated Positive, Neutral and Negative sentiment labels.
- Improved sentiment classification consistency.
- Created a new sentiment dataset for dashboard integration.

Output file:

data/curated/reviews_with_ml_sentiment.csv

---

## 5. Manual Validation

A manual validation process was performed to evaluate sentiment predictions.

The process included:

1. Creating a validation sample dataset.
2. Reviewing model predictions manually.
3. Comparing predictions with human judgement.
4. Identifying classification errors.

This validation helped confirm that the transformer-based model provided more reliable sentiment predictions than the previous VADER approach.

---

## 6. ADHD Review Detection

An ADHD-focused review analysis pipeline was developed.

The objective was to identify reviews discussing ADHD-related experiences, attention regulation, productivity challenges, and concentration support.

### Results

- Total ADHD-related reviews identified: 252
- Positive ADHD reviews: 180
- Neutral ADHD reviews: 24
- Negative ADHD reviews: 48

Output file:

data/curated/adhd_reviews_analysis.csv

---

## 7. ADHD Behavioural Theme Analysis

After identifying ADHD-related reviews, behavioural theme detection was performed.

A rule-based classification approach was used to identify common behavioural patterns discussed by users.

### Detected Themes

- Attention Support
- Task Management

### Findings

The majority of ADHD-related reviews focused on:

- Improving concentration
- Supporting attention regulation
- Assisting task completion
- Improving productivity habits

Output file:

data/curated/adhd_theme_analysis.csv

---

## 8. Dashboard Updates

The Streamlit dashboard was updated to include ADHD-related analysis sections.

### New Dashboard Components

- ADHD Behavioural Insights
- ADHD Theme Analysis
- Automated ADHD Insight Summary
- ADHD Behavioural Recommendations

These additions allow users to view ADHD-specific behavioural patterns directly within the dashboard.

---

## 9. Automated ADHD Insight Generation

An automated insight section was developed to summarise behavioural findings.

The system automatically reports:

- Most common behavioural themes
- Secondary behavioural themes
- Common behavioural patterns
- ADHD-related review statistics

This provides a high-level overview of ADHD-related user behaviour without requiring manual review of individual comments.

---

## 10. ADHD Behavioural Recommendations

A recommendation section was added to provide practical insights derived from behavioural patterns.

Example recommendations include:

- Structured attention-support systems.
- Productivity reinforcement mechanisms.
- Routine-building features.
- Motivation and engagement techniques.

These recommendations are automatically displayed within the dashboard.

---

## 11. LLM-Based ADHD Behaviour Detection

To extend behavioural analysis beyond keyword matching, a Large Language Model (LLM) approach was implemented.

The system uses zero-shot classification with DistilBART MNLI.

Unlike rule-based classification, the LLM can identify behavioural patterns even when ADHD-related keywords are not explicitly present.

### Behaviour Categories

- Time Management
- Motivation and Reward
- Task Management
- Distraction Management

### Results

The LLM identified 72 ADHD-related behavioural reviews distributed across these categories.

Output file:

data/curated/llm_adhd_behaviour_analysis.csv

### Benefit

This approach allows behavioural patterns to be identified based on review meaning rather than keyword presence alone.

---

## 12. Challenges Encountered

### Sentiment Classification

The original VADER model produced inconsistent results for app-review data.

### ADHD Detection

Many ADHD-related reviews did not explicitly mention ADHD, making detection difficult using simple keyword matching.

### LLM Processing

Large transformer models required significant download and processing time.

### Dashboard Integration

Multiple behavioural analysis sections required formatting and layout adjustments to maintain dashboard consistency.

---

## 13. Summary of Completed Work

The following development work was completed:

1. Replaced VADER with RoBERTa sentiment analysis.
2. Created a manual validation process.
3. Developed ADHD review detection.
4. Generated ADHD sentiment statistics.
5. Implemented ADHD behavioural theme detection.
6. Added ADHD behavioural insights.
7. Added ADHD behavioural recommendations.
8. Integrated ADHD analysis into the Streamlit dashboard.
9. Implemented LLM-based behavioural classification.
10. Generated LLM behavioural analysis datasets.
11. Added LLM behavioural visualisations to the dashboard.

---

## 14. Current Results

### ADHD Review Analysis

- Total ADHD Reviews: 252
- Positive: 180
- Neutral: 24
- Negative: 48

### LLM Behaviour Detection

- Time Management: 36
- Motivation and Reward: 24
- Task Management: 6
- Distraction Management: 6

Total LLM-detected behavioural reviews: 72

---

## 15. Next Steps

Recommended future improvements:

1. Expand behavioural categories.
2. Fine-tune behavioural classification models.
3. Improve review summarisation.
4. Add behavioural clustering techniques.
5. Integrate advanced LLM-generated explanations.
6. Perform final dashboard integration and testing.

---

## 16. Supervisor Update Summary

I improved the review analysis pipeline by replacing VADER with a RoBERTa-based sentiment analysis model and validating sentiment predictions through manual review. I developed ADHD-specific review analysis, behavioural theme detection, automated insight generation, and dashboard integration. I also implemented LLM-based behavioural classification using DistilBART MNLI to identify behavioural patterns that cannot be detected through simple keyword matching. These improvements expanded the project beyond sentiment analysis and provided deeper behavioural insights into focus app usage.
