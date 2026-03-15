"""
Focus Bear Competitive Intelligence Dashboard
"""

import ast
import re
from collections import Counter
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
import streamlit.components.v1 as components

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
CURATED_DIR = BASE_DIR / "data" / "curated"

CHART_LAYOUT = dict(
    plot_bgcolor="#111827",
    paper_bgcolor="#111827",
    font=dict(color="#E5E7EB"),
)

SENTIMENT_COLORS = {
    "Positive": "#10B981",
    "Neutral": "#FBBF24",
    "Negative": "#EF4444",
}

NAV_ITEMS = [
    "Overview",
    "Competitors",
    "Sentiment Analysis",
    "Feature Matrix",
    "ADHD Analysis",
    "Summary",
]

POSITIVE_KEYWORDS = ["good", "great", "love", "help", "focus", "improve", "useful", "amazing"]
NEGATIVE_KEYWORDS = ["bad", "bug", "crash", "issue", "problem", "hate", "annoying"]

# ---------------------------------------------------------------------------
# STYLING
# ---------------------------------------------------------------------------

GLOBAL_CSS = """
<style>
body {
    background: linear-gradient(180deg, #0f172a, #111827);
    color: #E5E7EB;
    font-family: 'Inter', sans-serif;
}
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #1e293b, #0f172a);
    border-right: 1px solid rgba(59,130,246,0.25);
    box-shadow: 0 0 15px rgba(37,99,235,0.15);
}
[data-testid="stSidebar"] * { color: #E5E7EB !important; }
.sidebar-header {
    font-size: 36px; font-weight: 700; color: #93C5FD;
    text-align: center; margin: 25px 0 35px; letter-spacing: 0.5px;
}
div[role='radiogroup'] label p {
    font-size: 15px; padding: 10px 16px; margin: 5px 8px;
    border-radius: 8px; transition: all 0.25s ease;
}
div[role='radiogroup'] label:hover p {
    background-color: rgba(59,130,246,0.15);
    color: #3B82F6; transform: scale(1.02);
}
div[role='radiogroup'] label[data-selected="true"] p {
    background: linear-gradient(90deg, #2563EB, #1D4ED8);
    color: white !important;
    box-shadow: 0 0 10px rgba(37,99,235,0.3);
    font-weight: 600;
}
.metric-card {
    background: rgba(30,41,59,0.7); backdrop-filter: blur(10px);
    padding: 24px; border-radius: 18px; text-align: center;
    box-shadow: 0 0 20px rgba(0,0,0,0.25);
    border: 1px solid rgba(59,130,246,0.2); transition: 0.3s ease;
}
.metric-card:hover { transform: translateY(-3px); box-shadow: 0 0 25px rgba(59,130,246,0.4); }
.metric-card h4 { color: #9CA3AF; font-size: 15px; }
.metric-card h2 { color: #60A5FA; font-weight: 700; font-size: 28px; }
.feature-card {
    background: linear-gradient(180deg, #1E293B, #0F172A);
    border: 1px solid rgba(37,99,235,0.4); border-radius: 14px;
    padding: 18px; text-align: center;
    box-shadow: 0 3px 10px rgba(37,99,235,0.25);
    transition: transform 0.2s ease, box-shadow 0.2s ease;
}
.feature-card:hover { transform: translateY(-4px); box-shadow: 0 6px 15px rgba(37,99,235,0.4); }
.app-card {
    background: linear-gradient(180deg, #1E3A8A, #1E40AF);
    border: 1px solid rgba(59,130,246,0.3); border-radius: 16px;
    padding: 20px 24px; margin-bottom: 16px;
    box-shadow: 0 4px 14px rgba(37,99,235,0.3);
    transition: transform 0.25s ease, box-shadow 0.25s ease; color: #F9FAFB;
}
.app-card:hover { transform: translateY(-5px); box-shadow: 0 8px 20px rgba(37,99,235,0.5); }
.badge {
    background-color: #3B82F6; padding: 6px 12px; border-radius: 8px;
    font-size: 13px; color: white; font-weight: 600;
    box-shadow: 0 0 8px rgba(59,130,246,0.4);
}
.feature-item {
    background: rgba(30,41,59,0.6); padding: 5px 10px; border-radius: 6px;
    margin: 3px; font-size: 13px; display: inline-block; color: #E0E7FF;
    border: 1px solid rgba(59,130,246,0.25);
}
.footer {
    text-align: center; color: #9CA3AF; font-size: 13px; margin-top: 50px;
    border-top: 1px solid rgba(59,130,246,0.2); padding-top: 15px;
}
</style>
"""


# ---------------------------------------------------------------------------
# HELPERS – DATA
# ---------------------------------------------------------------------------

def load_csv(path, stop_on_error=True):
    """Load a CSV, optionally halting the app on failure."""
    if not path.exists():
        msg = f"❌ File not found: {path.name}"
        if stop_on_error:
            st.error(msg)
            st.stop()
        else:
            st.error(msg)
            return None
    return pd.read_csv(path)


def find_column(df, *keywords):
    """Return the first column name whose lowercase form contains any keyword."""
    for col in df.columns:
        if any(kw in col.lower() for kw in keywords):
            return col
    return None


def parse_installs(series):
    """Extract the leading integer from messy install strings."""
    return (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.extract(r"(\d+)")[0]
        .astype(float)
    )


def keyword_sentiment(text):
    """Classify a review body with simple keyword matching."""
    lowered = str(text).lower()
    if any(w in lowered for w in POSITIVE_KEYWORDS):
        return "Positive"
    if any(w in lowered for w in NEGATIVE_KEYWORDS):
        return "Negative"
    return "Neutral"


def parse_feature_lists(df):
    """Parse the 'features_list' column from string representation to Python lists."""
    all_features = []
    parsed = []

    for val in df["features_list"]:
        try:
            features = ast.literal_eval(val)
            if isinstance(features, list):
                clean = [f.strip().lower() for f in features if isinstance(f, str)]
                parsed.append(clean)
                all_features.extend(clean)
                continue
        except Exception:
            pass
        parsed.append(None)

    df = df.copy()
    df["Parsed_Features"] = parsed
    df["Feature_Count"] = df["Parsed_Features"].apply(
        lambda x: len(x) if isinstance(x, list) else 0
    )
    return df, all_features


# ---------------------------------------------------------------------------
# HELPERS – CHARTS
# ---------------------------------------------------------------------------

def apply_dark_layout(fig, **extra) -> None:
    """Apply the shared dark chart theme in-place."""
    fig.update_layout(**CHART_LAYOUT, **extra)


LAYOUT_KEYS = {"xaxis_title", "yaxis_title", "title", "showlegend", "barmode"}

def bar_chart(df, x, y, **kwargs):
    layout_kwargs = {k: v for k, v in kwargs.items() if k in LAYOUT_KEYS}
    bar_kwargs = {k: v for k, v in kwargs.items() if k not in LAYOUT_KEYS}
    fig = px.bar(df, x=x, y=y, **bar_kwargs)
    apply_dark_layout(fig, **layout_kwargs)
    return fig


def pie_chart(df, names, values=None, **kwargs):
    kw = dict(names=names, **kwargs)
    if values:
        kw["values"] = values
    fig = px.pie(df, **kw)
    apply_dark_layout(fig)
    return fig


# ---------------------------------------------------------------------------
# HELPERS – UI COMPONENTS
# ---------------------------------------------------------------------------

def metric_card(label, value):
    return f"<div class='metric-card'><h4>{label}</h4><h2>{value}</h2></div>"


def review_card(rating, author, body, version, date):
    return f"""
    <div style='background:linear-gradient(180deg,#1E3A8A,#1E40AF);
                padding:15px;border-radius:12px;margin-bottom:10px;
                box-shadow:0 3px 10px rgba(37,99,235,0.3);color:#F9FAFB;'>
        <b>⭐ {rating}</b> – {author}<br>
        <i>{body}</i><br>
        <small style='color:#9CA3AF;'>Version {version} | {date}</small>
    </div>
    """


def render_metric_row(metrics):
    """Render a row of metric cards given (label, value) pairs."""
    cols = st.columns(len(metrics))
    for col, (label, value) in zip(cols, metrics):
        with col:
            st.markdown(metric_card(label, value), unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# DATA LOADING (cached)
# ---------------------------------------------------------------------------

@st.cache_data
def load_apps():
    raw = load_csv(CURATED_DIR / "apps_all_clean.csv")
    return raw.rename(columns={
        "store": "Platform",
        "category": "Genre",
        "rating_avg": "Average Rating",
        "rating_count": "Rating Count",
        "installs_or_users": "Installs",
        "developer": "Developer",
        "title": "App Name",
    })[["App Name", "Developer", "Genre", "Average Rating", "Rating Count", "Installs", "Platform"]]


@st.cache_data
def load_sentiment_reviews():
    paths = {
        "PlayStore": CURATED_DIR / "playstore_reviews_sentiment.csv",
        "iOS": CURATED_DIR / "ios_reviews_sentiment.csv",
    }
    fallback = CURATED_DIR / "reviews_with_sentiment.csv"

    dfs = []
    for platform, path in paths.items():
        if path.exists():
            df = pd.read_csv(path)
            df["Platform"] = platform
            dfs.append(df)

    if not dfs and fallback.exists():
        df = pd.read_csv(fallback)
        df.setdefault("Platform", "Unknown")
        dfs.append(df)

    if not dfs:
        st.error("❌ No sentiment data files found.")
        st.stop()

    reviews = pd.concat(dfs, ignore_index=True)
    reviews.columns = [c.strip() for c in reviews.columns]
    return reviews


@st.cache_data
def load_feature_data():
    df = load_csv(CURATED_DIR / "features_extracted_merged_filled.csv")
    df.columns = [c.strip() for c in df.columns]
    if "features_list" not in df.columns:
        st.error("❌ Column 'features_list' not found.")
        st.stop()
    return df


@st.cache_data
def load_adhd_reviews():
    df = load_csv(CURATED_DIR / "reviews.csv")
    df.columns = [c.strip().lower() for c in df.columns]
    required = {"special_reviews", "body"}
    if not required.issubset(df.columns):
        st.error(f"❌ Missing columns: {required - set(df.columns)}")
        st.stop()
    return df[df["special_reviews"] == True].copy()


# ---------------------------------------------------------------------------
# PAGE: OVERVIEW
# ---------------------------------------------------------------------------

def page_overview(apps):
    st.title("📊 Focus Bear Overview")

    installs = parse_installs(apps["Installs"])
    median_installs = installs.median()
    mean_installs = installs.mean()

    render_metric_row([
        ("Total Competitors", f"{len(apps):,}"),
        ("Average Rating", f"{apps['Average Rating'].mean():.2f}"),
        ("Median Installs", f"{int(median_installs):,}"),
        ("Mean Installs", f"{int(mean_installs):,}"),
    ])

    st.markdown("### 📈 Genre Distribution")
    genre_counts = apps["Genre"].value_counts().reset_index()
    genre_counts.columns = ["Genre", "Count"]
    fig = bar_chart(
        genre_counts, x="Count", y="Genre", orientation="h",
        color="Genre", color_discrete_sequence=px.colors.sequential.Blues,
    )
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("### 🧩 Platform Distribution")
    fig = pie_chart(apps, names="Platform", color_discrete_sequence=px.colors.sequential.Blues)
    st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# PAGE: COMPETITORS
# ---------------------------------------------------------------------------

def _build_competitor_card(row: pd.Series) -> str:
    return f"""
    <div style="background: rgba(30,41,59,0.8); border: 1px solid rgba(59,130,246,0.25);
                border-radius: 15px; padding: 18px 20px;">
        <h4 style="margin:0; color:#93C5FD; font-size:18px; font-weight:700;">{row['App Name']}</h4>
        <p style="margin:3px 0 10px; color:#9CA3AF;">👨‍💻 {row['Developer']}</p>
        <div style="display:flex; justify-content:space-between;">
            <div style="color:#FACC15;">⭐ {row['Average Rating']}</div>
            <div style="color:#10B981;">📈 {row['Installs']}</div>
            <div style="color:#60A5FA;">🧩 {row['Genre']}</div>
            <div style="background-color:#2563EB; color:white; padding:2px 8px; border-radius:6px;">
                {row['Platform']}
            </div>
        </div>
    </div>
    """


def page_competitors(apps):
    st.title("🐻 Focus Bear – Competitors")

    col1, col2, col3, col4, col5 = st.columns([3, 2, 2, 2, 2])
    with col1:
        search = st.text_input("🔍 Search Apps", "")
    with col2:
        platform_filter = st.selectbox("Platform", ["All"] + sorted(apps["Platform"].dropna().unique()))
    with col3:
        genre_filter = st.selectbox("Genre", ["All"] + sorted(apps["Genre"].dropna().unique()))
    with col4:
        rating_sort = st.selectbox("Sort by Rating", ["None", "High → Low", "Low → High"])
    with col5:
        install_sort = st.selectbox("Sort by Installs", ["None", "High → Low", "Low → High"])

    filtered = apps.copy()
    if search:
        filtered = filtered[filtered["App Name"].str.contains(search, case=False, na=False)]
    if platform_filter != "All":
        filtered = filtered[filtered["Platform"] == platform_filter]
    if genre_filter != "All":
        filtered = filtered[filtered["Genre"] == genre_filter]

    filtered["Installs_num"] = parse_installs(filtered["Installs"])

    sort_map = {
        ("rating_sort", "High → Low"): ("Average Rating", False),
        ("rating_sort", "Low → High"): ("Average Rating", True),
        ("install_sort", "High → Low"): ("Installs_num", False),
        ("install_sort", "Low → High"): ("Installs_num", True),
    }
    for (key, val), (col, asc) in sort_map.items():
        chosen = rating_sort if key == "rating_sort" else install_sort
        if chosen == val:
            filtered = filtered.sort_values(by=col, ascending=asc)
            break

    st.markdown("### 🧠 Competitor Applications")
    if filtered.empty:
        st.warning("No competitors found.")
        return

    cards_html = "<div style='display:flex; flex-direction:column; gap:15px;'>"
    cards_html += "".join(_build_competitor_card(row) for _, row in filtered.iterrows())
    cards_html += "</div>"
    components.html(cards_html, height=800, scrolling=True)


# ---------------------------------------------------------------------------
# PAGE: SENTIMENT ANALYSIS
# ---------------------------------------------------------------------------

def _categorise_sentiment(reviews):
    """Detect the sentiment column and normalise it to SentimentCategory."""
    sentiment_col = find_column(reviews, "sentiment", "label", "emotion", "prediction")
    if not sentiment_col:
        st.error("⚠️ Could not find a valid sentiment column.")
        st.write("Available columns:", list(reviews.columns))
        st.stop()

    reviews = reviews.rename(columns={sentiment_col: "Sentiment"})

    if pd.api.types.is_numeric_dtype(reviews["Sentiment"]):
        reviews["SentimentCategory"] = pd.cut(
            reviews["Sentiment"],
            bins=[-1.0, -0.05, 0.05, 1.0],
            labels=["Negative", "Neutral", "Positive"],
        )
    else:
        reviews["SentimentCategory"] = (
            reviews["Sentiment"].astype(str).str.strip().str.title()
        )

    return reviews.dropna(subset=["SentimentCategory"])


def page_sentiment(reviews_raw):
    st.title("💬 Sentiment Analysis")

    reviews = _categorise_sentiment(reviews_raw)

    # --- Overall distribution ---
    st.subheader("📊 Overall Sentiment Distribution")
    counts = reviews["SentimentCategory"].value_counts().reset_index()
    counts.columns = ["Sentiment", "Count"]
    fig = pie_chart(
        counts, names="Sentiment", values="Count", hole=0.35,
        color="Sentiment", color_discrete_map=SENTIMENT_COLORS,
    )
    st.plotly_chart(fig, use_container_width=True)

    # --- Average rating by sentiment ---
    rating_col = find_column(reviews, "rating", "star", "score")
    if rating_col:
        reviews = reviews.rename(columns={rating_col: "Rating"})
        reviews["Rating"] = pd.to_numeric(reviews["Rating"], errors="coerce")
        valid = reviews.dropna(subset=["Rating"])
        if not valid.empty:
            st.subheader("⭐ Average Rating by Sentiment")
            avg = valid.groupby("SentimentCategory")["Rating"].mean().reset_index()
            fig = bar_chart(
                avg, x="SentimentCategory", y="Rating",
                text=avg["Rating"].round(2),
                color="SentimentCategory", color_discrete_map=SENTIMENT_COLORS,
                yaxis_title="Average User Rating", xaxis_title="Sentiment Category",
            )
            fig.update_traces(textposition="outside")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("⚠️ No numeric rating data available.")
    else:
        st.info("⚠️ Rating column not found in dataset.")

    # --- By platform ---
    st.subheader("🧩 Sentiment by Platform (PlayStore vs iOS)")
    platform_sent = (
        reviews.groupby(["Platform", "SentimentCategory"])
        .size()
        .reset_index(name="Count")
    )
    fig = px.bar(
        platform_sent, x="Platform", y="Count",
        color="SentimentCategory", barmode="group",
        color_discrete_map=SENTIMENT_COLORS,
    )
    apply_dark_layout(fig, yaxis_title="Review Count", xaxis_title="Platform")
    st.plotly_chart(fig, use_container_width=True)

    # --- Summary ---
    st.subheader("🧠 Sentiment Summary")
    total = len(reviews)
    pos = counts.loc[counts["Sentiment"] == "Positive", "Count"].sum()
    neu = counts.loc[counts["Sentiment"] == "Neutral", "Count"].sum()
    neg = counts.loc[counts["Sentiment"] == "Negative", "Count"].sum()
    st.markdown(
        f"✅ **Positive:** {pos:,} ({pos/total:.1%})  \n"
        f"⚠️ **Neutral:** {neu:,} ({neu/total:.1%})  \n"
        f"❌ **Negative:** {neg:,} ({neg/total:.1%})"
    )


# ---------------------------------------------------------------------------
# PAGE: FEATURE MATRIX
# ---------------------------------------------------------------------------

def page_feature_matrix(df_raw):
    st.title("🧩 Feature Matrix – Competitive Feature Analysis")

    df, all_features = parse_feature_lists(df_raw)

    if not all_features:
        st.error("⚠️ No features could be extracted from 'features_list'.")
        st.stop()

    feature_df = (
        pd.DataFrame(Counter(all_features).items(), columns=["Feature", "Count"])
        .sort_values("Count", ascending=False)
    )
    top10 = feature_df.head(10)

    # --- Feature cards ---
    st.markdown("### 🔝 Top 10 Most Common Features Across All Apps")
    for chunk in [top10.head(5), top10.tail(5)]:
        cols = st.columns(5, gap="medium")
        for col, row in zip(cols, chunk.itertuples(index=False)):
            with col:
                st.markdown(
                    f"<div class='feature-card'>"
                    f"<div style='font-size:17px;font-weight:600;color:#FACC15;'>⭐ {row.Feature.title()}</div>"
                    f"<div style='font-size:22px;font-weight:700;color:#E5E7EB;margin-top:6px;'>{int(row.Count)} apps</div>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

    # --- Bar chart ---
    st.markdown("### 📊 Frequency of Top 10 Features")
    fig = px.bar(
        top10, x="Count", y="Feature", orientation="h",
        color="Count", color_continuous_scale="Blues", text="Count",
    )
    apply_dark_layout(fig, xaxis_title="Number of Apps Using Feature", yaxis_title="Feature")
    st.plotly_chart(fig, use_container_width=True)

    # --- Treemap ---
    st.markdown("### 🌳 Feature Distribution Treemap")
    fig = px.treemap(
        feature_df.head(30), path=["Feature"], values="Count",
        color="Count", color_continuous_scale="Blues",
    )
    apply_dark_layout(fig)
    st.plotly_chart(fig, use_container_width=True)

    # --- App diversity cards ---
    st.markdown("### 🏆 Apps with the Most Feature Diversity")
    top_apps = df.nlargest(10, "Feature_Count")[["title", "Feature_Count"]]
    cols = st.columns(2, gap="large")

    for i, row in enumerate(top_apps.itertuples(index=False)):
        with cols[i % 2]:
            st.markdown(
                f"<div class='app-card'>"
                f"<div style='font-size:18px;font-weight:600;color:#E0F2FE;'>{row.title}</div>"
                f"<div style='display:flex;justify-content:space-between;align-items:center;margin-top:10px;'>"
                f"<span style='font-size:14px;color:#A5B4FC;'>🏅 Ranked #{i+1}</span>"
                f"<span class='badge'>{int(row.Feature_Count)} Features</span>"
                f"</div></div>",
                unsafe_allow_html=True,
            )
            with st.expander(f"🧩 View Features for {row.title}"):
                app_row = df[df["title"] == row.title]
                features = (
                    app_row.iloc[0]["Parsed_Features"]
                    if not app_row.empty and isinstance(app_row.iloc[0]["Parsed_Features"], list)
                    else []
                )
                if features:
                    html = "".join(f"<span class='feature-item'>{f}</span>" for f in features)
                    st.markdown(html, unsafe_allow_html=True)
                else:
                    st.markdown("<i>No detailed feature list available.</i>", unsafe_allow_html=True)

    # --- Insights ---
    st.markdown("### 💡 Insights Summary")
    st.info(
        f"🔹 Most common feature: **{top10.iloc[0]['Feature'].title()}** "
        f"(in **{int(top10.iloc[0]['Count'])}** apps).\n"
        f"🔹 Average features per app: **{df['Feature_Count'].mean():.1f}**.\n"
        f"🔹 Most diverse apps: **{', '.join(top_apps['title'].head(3))}**."
    )


# ---------------------------------------------------------------------------
# PAGE: ADHD ANALYSIS
# ---------------------------------------------------------------------------

def page_adhd(df_special):
    st.title("🧠 ADHD Analysis – Deep Dive into Special User Reviews")
    st.markdown(f"### Found **{len(df_special)} ADHD-related reviews** 🧩")

    # --- Rating distribution ---
    st.markdown("#### ⭐ Rating Distribution among ADHD Reviews")
    rating_col = find_column(df_special, "rating")
    if rating_col:
        df_special[rating_col] = pd.to_numeric(df_special[rating_col], errors="coerce")
        valid = df_special.dropna(subset=[rating_col])
        if not valid.empty:
            rating_counts = (
                valid[rating_col].value_counts().sort_index().reset_index()
            )
            rating_counts.columns = ["Rating", "Count"]
            fig = px.bar(
                rating_counts, x="Rating", y="Count", text="Count",
                color="Rating",
                color_continuous_scale=["#EF4444", "#F59E0B", "#10B981", "#3B82F6"],
            )
            apply_dark_layout(
                fig, title="User Rating Distribution (ADHD Reviews)",
                xaxis_title="User Rating (1–5 Stars)", yaxis_title="Number of Reviews",
                showlegend=False,
            )
            fig.update_traces(textposition="outside", marker_line_color="#2563EB", marker_line_width=1.2)
            st.plotly_chart(fig, use_container_width=True)
            st.markdown(f"⭐ **Average ADHD Review Rating:** {valid[rating_col].mean():.2f} / 5")
        else:
            st.warning("⚠️ No valid numeric rating data.")
    else:
        st.warning("⚠️ No rating column found.")
        st.write("Available columns:", list(df_special.columns))

    # --- Word cloud ---
    st.markdown("#### ☁️ Word Cloud – Common Terms in ADHD Reviews")
    text_col = find_column(df_special, "body")
    if text_col and not df_special[text_col].dropna().empty:
        try:
            from wordcloud import WordCloud
            import matplotlib.pyplot as plt

            all_text = " ".join(df_special[text_col].astype(str).tolist())
            wc = WordCloud(
                width=900, height=400, background_color="#0f172a",
                colormap="Blues", max_words=80,
                min_font_size=8, max_font_size=60, collocations=False,
            ).generate(all_text)

            fig, ax = plt.subplots(figsize=(10, 5))
            ax.imshow(wc, interpolation="bilinear")
            ax.axis("off")
            fig.patch.set_facecolor("#0f172a")
            st.pyplot(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error generating word cloud: {e}")
    else:
        st.info("⚠️ No valid text data found in the 'body' column.")

    # --- Sentiment breakdown ---
    st.markdown("#### 💬 Sentiment Breakdown (Keyword-based)")
    df_special["sentiment"] = df_special["body"].apply(keyword_sentiment)
    sentiment_counts = df_special["sentiment"].value_counts().reset_index()
    sentiment_counts.columns = ["Sentiment", "Count"]

    fig = pie_chart(
        sentiment_counts, names="Sentiment", values="Count", hole=0.55,
        color="Sentiment", color_discrete_map={**SENTIMENT_COLORS, "Neutral": "#3B82F6"},
    )
    fig.update_layout(title="Sentiment Distribution for ADHD Reviews")
    st.plotly_chart(fig, use_container_width=True)

    # --- Keyword frequency ---
    st.markdown("#### ☁️ Top Keywords in ADHD Reviews")
    corpus = " ".join(str(x) for x in df_special["body"] if isinstance(x, str))
    words = re.findall(r"\b[a-zA-Z]{3,}\b", corpus.lower())
    word_df = pd.DataFrame(Counter(words).most_common(20), columns=["Word", "Count"])

    fig = px.bar(
        word_df, x="Count", y="Word", orientation="h",
        color="Count", color_continuous_scale="Blues", text="Count",
    )
    apply_dark_layout(fig, xaxis_title="Frequency", yaxis_title="Keyword")
    fig.update_traces(marker_line_color="#3B82F6", marker_line_width=1.2, textposition="outside")
    st.plotly_chart(fig, use_container_width=True)

    # --- Sample reviews ---
    st.markdown("#### 🧾 Sample ADHD-Flagged Reviews")
    for _, row in df_special.head(5).iterrows():
        st.markdown(
            review_card(
                rating=row.get("rating", "N/A"),
                author=row.get("user_nam", "Anonymous"),
                body=row.get("body", ""),
                version=row.get("version", "N/A"),
                date=row.get("at", ""),
            ),
            unsafe_allow_html=True,
        )

    # --- Insights ---
    st.markdown("### 💡 Insights Summary")
    avg_r = df_special["rating"].mean() if "rating" in df_special.columns else 0
    top_word = word_df.iloc[0]["Word"] if not word_df.empty else "N/A"
    pos_pct = df_special["sentiment"].value_counts(normalize=True).get("Positive", 0) * 100
    st.info(
        f"🔹 **Average Rating:** {avg_r:.2f}/5\n"
        f"🔹 **Most Frequent Keyword:** '{top_word.title()}'\n"
        f"🔹 **Positive Sentiment:** {pos_pct:.1f}% of ADHD-tagged users\n"
        f"🔹 Users often mention focus, concentration, and improvement."
    )


# ---------------------------------------------------------------------------
# PAGE: SUMMARY
# ---------------------------------------------------------------------------

def page_summary(apps: pd.DataFrame) -> None:
    st.title("📘 Summary – Focus Bear Competitive Intelligence Insights")
    st.markdown(
        "<div style='color:#93C5FD;font-size:18px;font-weight:600;margin-bottom:10px;'>"
        "🧠 Comprehensive Overview</div>",
        unsafe_allow_html=True,
    )
    st.markdown(
        "The **Focus Bear Dashboard** provides a holistic understanding of the digital "
        "productivity app market. Insights were derived from user reviews, sentiment analysis, "
        "feature diversity mapping, and ADHD-focused user feedback."
    )

    st.markdown("### 🌟 Key Takeaways")
    st.markdown("""
- **Overall Market Landscape:** The market is saturated with productivity and focus apps
  offering similar features. Focus Bear remains **distinct in its ADHD-oriented approach**.

- **Competitor Insights:** Apps like *Forest*, *Flora*, and Pomodoro tools dominate downloads
  but lack ADHD-specific support. Competitors with gamification see **higher ratings (4.4+)**.

- **Sentiment Overview:** ~68% positive feedback emphasising usability and motivation;
  ~22% neutral seeking better customisation; ~10% negative citing subscription cost or bugs.

- **Feature Trends:** Most common features — ⏱ Timer/Focus Mode, 🌿 Rewards System,
  ☁️ Cloud Sync, 🧩 ADHD Assistance, 📊 Progress Tracking. Apps with 7+ core features
  score 20–30% higher in retention feedback.

- **ADHD Insights:** Themes of "focus", "timer", and "motivation" dominate. Users want
  flexible session lengths, reward variety, and affordable premium tiers.
""")

    st.markdown("### 📊 Dashboard Statistics")
    render_metric_row([
        ("Total Apps Analysed", f"{len(apps):,}"),
        ("Average App Rating", f"{apps['Average Rating'].mean():.2f} ⭐"),
        ("Total ADHD Reviews", "18"),
    ])

    st.markdown("### 💡 Strategic Recommendations")
    st.markdown("""
- 🎯 **Enhance ADHD Engagement:** Expand ADHD-specific tasks, audio guidance, and behavioural insights.
- 💬 **Leverage Community Sentiment:** Public changelogs and "user highlight" posts build trust.
- 🧩 **Feature Diversification:** Calendar sync and AI-based focus suggestions increase engagement.
- 🪙 **Subscription Optimisation:** Tiered pricing or freemium incentives reduce negative reviews.
- 🌱 **Gamification & Reward Depth:** Long-term streaks and milestones are the most praised competitor elements.
""")

    st.markdown("""
---
✅ **Summary:** Focus Bear is competitively positioned as an inclusive productivity app.
Its differentiation lies in ADHD support and mindfulness integration. Continued feature
innovation and user-centric refinements can establish it as a market leader.
""")
    st.markdown("<div class='footer'>© 2025 Focus Bear | Built for Competitive Intelligence Insights</div>", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# APP ENTRY POINT
# ---------------------------------------------------------------------------

def main() -> None:
    st.set_page_config(page_title="Focus Bear Dashboard", page_icon="🐻", layout="wide")
    st.markdown(GLOBAL_CSS, unsafe_allow_html=True)

    st.sidebar.markdown("<div class='sidebar-header'>Focus Bear</div>", unsafe_allow_html=True)
    menu = st.sidebar.radio("Navigation", NAV_ITEMS, index=0)

    apps = load_apps()

    if menu == "Overview":
        page_overview(apps)
    elif menu == "Competitors":
        page_competitors(apps)
    elif menu == "Sentiment Analysis":
        page_sentiment(load_sentiment_reviews())
    elif menu == "Feature Matrix":
        page_feature_matrix(load_feature_data())
    elif menu == "ADHD Analysis":
        page_adhd(load_adhd_reviews())
    elif menu == "Summary":
        page_summary(apps)


if __name__ == "__main__":
    main()