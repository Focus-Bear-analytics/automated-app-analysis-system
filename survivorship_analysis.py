"""
survivorship_analysis.py
Streamlit page — Survivorship & Longitudinal Analysis
Reads three separate review CSVs (Play, iOS, Chrome) collected ~2019.
Maps apps to Lyngs (2022) study registry, enriches with survival status (2026),
mines feature keywords, and visualises:
Tab 1 – App Status Table
Tab 2 – Feature Survival Charts
Tab 3 – Rating Trends Over Time
Tab 4 – Review Evidence (feature mentions by survival status)
Tab 5 – Methodology
"""

import os
import re
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ─────────────────────────────────────────────────────────────────────────────
# 1. APP REGISTRY (Lyngs 2022 study apps + 2026 survival status)
# ─────────────────────────────────────────────────────────────────────────────
APP_REGISTRY = {
    # Play Store app_ids
    "cc.forestapp": {"name": "Forest", "category": "Timer/Gamification", "status": "Active", "platforms": ["play", "ios"]},
    "com.urbandroid.sleep": {"name": "Sleep as Android", "category": "Tracking/Analytics", "status": "Active", "platforms": ["play"]},
    "com.rescuetime.android": {"name": "RescueTime", "category": "Tracking/Analytics", "status": "Active", "platforms": ["play", "ios", "chrome"]},
    "com.appfinca.flora.ios": {"name": "Flora", "category": "Timer/Gamification", "status": "Active", "platforms": ["play", "ios"]},
    "com.offtime.app": {"name": "OFFTIME", "category": "Block/Removal", "status": "Discontinued", "platforms": ["play", "ios"]},
    "mobi.mobirix.rewardchart": {"name": "AppDetox", "category": "Block/Removal", "status": "Discontinued", "platforms": ["play"]},
    "com.appdetox.appdetox": {"name": "AppDetox", "category": "Block/Removal", "status": "Discontinued", "platforms": ["play"]},
    "com.zenlabs.focusplant": {"name": "Focus Plant", "category": "Timer/Gamification", "status": "Active", "platforms": ["play", "ios"]},
    "com.kevinholesh.Moment": {"name": "Moment", "category": "Tracking/Analytics", "status": "Discontinued", "platforms": ["ios"]},
    "com.smashingboxes.focuskeeper": {"name": "Focus Keeper", "category": "Timer/Pomodoro", "status": "Active", "platforms": ["ios"]},
    "com.focusmatrix.focusmatrixapp": {"name": "Focus Matrix", "category": "Scheduling/Planning", "status": "Stale", "platforms": ["ios"]},
    "com.usepanda.panda": {"name": "Panda", "category": "Nudge/Reflection", "status": "Discontinued", "platforms": ["chrome"]},
    "com.habitrpg.android.habitica": {"name": "Habitica", "category": "Timer/Gamification", "status": "Active", "platforms": ["play", "ios"]},
    "com.urbandroid.lux": {"name": "Twilight", "category": "Block/Removal", "status": "Active", "platforms": ["play"]},
    "com.mobileware.timezoneapp": {"name": "Timezone Pro", "category": "Scheduling/Planning", "status": "Stale", "platforms": ["play"]},
    "org.pricelessapps.focusnow": {"name": "Focus Now", "category": "Timer/Pomodoro", "status": "Discontinued", "platforms": ["play"]},
    "com.samanasoftwares.focustimer": {"name": "Focus Timer", "category": "Timer/Pomodoro", "status": "Active", "platforms": ["play", "ios"]},
    "com.focusbull.android": {"name": "Focus Bull", "category": "Timer/Pomodoro", "status": "Discontinued", "platforms": ["play"]},
    "com.calm.android": {"name": "Calm", "category": "Nudge/Reflection", "status": "Active", "platforms": ["play", "ios"]},
    "com.headspace.android": {"name": "Headspace", "category": "Nudge/Reflection", "status": "Active", "platforms": ["play", "ios"]},

    # iOS-only
    "pabloweb.net.SelfControl": {"name": "SelfControl", "category": "Block/Removal", "status": "Active", "platforms": ["ios"]},
    "com.imobiapp.screentimer": {"name": "Screen Timer", "category": "Tracking/Analytics", "status": "Discontinued", "platforms": ["ios"]},
    "com.jordan-carney.Liberate": {"name": "Liberate", "category": "Block/Removal", "status": "Discontinued", "platforms": ["ios"]},
    "ca.genoe.Refrain": {"name": "Refrain", "category": "Block/Removal", "status": "Discontinued", "platforms": ["ios"]},
    "com.bellostudios.hooked": {"name": "Hooked", "category": "Tracking/Analytics", "status": "Discontinued", "platforms": ["ios"]},
    "com.getfeedless.feedless": {"name": "Feedless", "category": "Block/Removal", "status": "Discontinued", "platforms": ["ios"]},
    "vn.wehelp.SelfControlLite": {"name": "SelfControl Lite", "category": "Block/Removal", "status": "Discontinued", "platforms": ["ios"]},
    "club.donutdog.ios": {"name": "Donut Dog", "category": "Timer/Gamification", "status": "Discontinued", "platforms": ["ios"]},
    "com.riko.suyasaso": {"name": "Suyasaso", "category": "Timer/Pomodoro", "status": "Discontinued", "platforms": ["ios"]},
    "com.blacklistapp.Blacklist": {"name": "Blacklist", "category": "Block/Removal", "status": "Stale", "platforms": ["ios"]},
    "com.erichuju.pomodoro": {"name": "Pomodoro Timer", "category": "Timer/Pomodoro", "status": "Active", "platforms": ["ios"]},
    "com.getcluster.Compose": {"name": "Compose", "category": "Nudge/Reflection", "status": "Discontinued", "platforms": ["ios"]},
}

CHROME_TITLE_REGISTRY = {
    "StayFocusd": {"name": "StayFocusd", "category": "Block/Removal", "status": "Active", "platforms": ["chrome"]},
    "RescueTime": {"name": "RescueTime", "category": "Tracking/Analytics", "status": "Active", "platforms": ["play", "ios", "chrome"]},
    "HabitLab": {"name": "HabitLab", "category": "Nudge/Reflection", "status": "Discontinued", "platforms": ["chrome"]},
    "Leechblock NG": {"name": "LeechBlock NG", "category": "Block/Removal", "status": "Active", "platforms": ["chrome"]},
    "LeechBlock": {"name": "LeechBlock NG", "category": "Block/Removal", "status": "Active", "platforms": ["chrome"]},
    "Mercury Reader": {"name": "Mercury Reader", "category": "Nudge/Reflection", "status": "Active", "platforms": ["chrome"]},
    "Nanny for Google Chrome": {"name": "Nanny", "category": "Block/Removal", "status": "Active", "platforms": ["chrome"]},
    "WasteNoTime": {"name": "WasteNoTime", "category": "Block/Removal", "status": "Stale", "platforms": ["chrome"]},
    "Freedom": {"name": "Freedom", "category": "Block/Removal", "status": "Active", "platforms": ["play", "ios", "chrome"]},
    "Focusmate": {"name": "Focusmate", "category": "Nudge/Reflection", "status": "Active", "platforms": ["chrome"]},
    "Intent.": {"name": "Intent", "category": "Nudge/Reflection", "status": "Discontinued", "platforms": ["chrome"]},
    "Productivity Owl": {"name": "Productivity Owl", "category": "Block/Removal", "status": "Discontinued", "platforms": ["chrome"]},
    "Forest": {"name": "Forest", "category": "Timer/Gamification", "status": "Active", "platforms": ["play", "ios", "chrome"]},
}

# ─────────────────────────────────────────────────────────────────────────────
# 2. FEATURE KEYWORD PATTERNS
# ─────────────────────────────────────────────────────────────────────────────
FEATURE_PATTERNS = {
    "feat_block": r"\b(block|blacklist|restrict|ban|prevent|lock)\b",
    "feat_timer": r"\b(timer|pomodoro|countdown|time limit|session|interval)\b",
    "feat_track": r"\b(track|usage|statistics|stats|monitor|screen time|dashboard|report|analytics)\b",
    "feat_reward": r"\b(reward|gamif|points|coins|tree|plant|badge|achievement|streak|earn)\b",
    "feat_nudge": r"\b(nudge|remind|prompt|aware|reflect|mindful|conscious|insight|gentle)\b",
    "feat_schedule": r"\b(schedul|calendar|routine|habit|whitelist|allow|permit)\b",
    "feat_adhd": r"\b(adhd|focus|concentrat|attention|distract|productive|procrastinat)\b",
    "feat_social": r"\b(social media|facebook|instagram|twitter|reddit|youtube|tiktok)\b",
}

STATUS_ORDER = ["Active", "Stale", "Discontinued"]
STATUS_COLORS = {"Active": "#2ecc71", "Stale": "#f39c12", "Discontinued": "#e74c3c"}
CATEGORY_COLORS = px.colors.qualitative.Set2

# ─────────────────────────────────────────────────────────────────────────────
# 3. DATA LOADERS
# ─────────────────────────────────────────────────────────────────────────────
def _find(filename: str) -> Path | None:
    candidates = [
        Path(filename),
        Path("data") / filename,
        Path("../data") / filename,
        Path(__file__).parent / filename,
        Path(__file__).parent / "data" / filename,
    ]
    for p in candidates:
        if p.exists():
            return p
    return None


@st.cache_data(show_spinner=False)
def load_reviews() -> pd.DataFrame:
    frames = []

    play_path = _find("2019_03_19-play_reviews_unique.csv")
    if play_path:
        df = pd.read_csv(play_path, on_bad_lines="skip")
        df = df.rename(columns={"app_id": "tool_id", "text": "review_text"})
        df["store"] = "play"
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
        frames.append(df[["tool_id", "user_name", "date", "rating", "review_text", "store"]])

    ios_path = _find("2019-03-21_apple_reviews_unique.csv")
    if ios_path:
        df = pd.read_csv(ios_path, on_bad_lines="skip")
        df = df.rename(columns={"app_id": "tool_id"})
        df["store"] = "ios"
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
        frames.append(df[["tool_id", "user_name", "date", "rating", "review_text", "store"]])

    cws_path = _find("2019-02-08-chrome_reviews_unique.csv")
    if cws_path:
        df = pd.read_csv(cws_path, on_bad_lines="skip")
        if "type" in df.columns:
            df = df[df["type"] == "review"].copy()
        df = df.rename(columns={"extension_title": "tool_id", "date_modified": "date", "text": "review_text"})
        df["store"] = "chrome"
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
        frames.append(df[["tool_id", "user_name", "date", "rating", "review_text", "store"]])

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.dropna(subset=["rating"])
    combined["review_text"] = combined["review_text"].fillna("").astype(str)
    return combined


def enrich(df: pd.DataFrame) -> pd.DataFrame:
    def lookup(row):
        tid = str(row["tool_id"]).strip()
        if row["store"] in ("play", "ios"):
            rec = APP_REGISTRY.get(tid)
        else:
            rec = CHROME_TITLE_REGISTRY.get(tid)
            if rec is None:
                for k, v in CHROME_TITLE_REGISTRY.items():
                    if k.lower() in tid.lower():
                        rec = v
                        break

        if rec:
            return pd.Series([rec["name"], rec["status"], rec["category"]])
        return pd.Series([tid, "Unknown", "Unknown"])

    df[["app_name", "status", "category"]] = df.apply(lookup, axis=1)

    for feat, pattern in FEATURE_PATTERNS.items():
        df[feat] = df["review_text"].str.lower().str.contains(pattern, regex=True, na=False).astype(int)

    df["year"] = df["date"].dt.year
    df["quarter"] = df["date"].dt.to_period("Q").astype(str)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 4. PAGE RENDER
# ─────────────────────────────────────────────────────────────────────────────
def survivorship_page():
    st.title("🌱 Survivorship & Longitudinal Analysis")
    st.caption(
        "Apps from Lyngs (2022) study • reviews collected 2019 • "
        "survival status verified May 2026"
    )

    with st.spinner("Loading reviews…"):
        raw = load_reviews()

    if raw.empty:
        st.error(
            "No review files found. Place these three CSVs in the same folder as app.py "
            "(or a `data/` subfolder):\n\n"
            "- `2019_03_19-play_reviews_unique.csv`\n"
            "- `2019-03-21_apple_reviews_unique.csv`\n"
            "- `2019-02-08-chrome_reviews_unique.csv`"
        )
        return

    df = enrich(raw.copy())

    with st.sidebar:
        st.header("Filters")
        sel_status = st.multiselect("Survival status", STATUS_ORDER, default=STATUS_ORDER)
        all_cats = sorted(df["category"].dropna().unique())
        sel_cats = st.multiselect("Category", all_cats, default=all_cats)
        sel_stores = st.multiselect("Store", ["play", "ios", "chrome"], default=["play", "ios", "chrome"])

        valid_years = df["year"].dropna()
        if valid_years.empty:
            year_min, year_max = 2019, 2019
        else:
            year_min, year_max = int(valid_years.min()), int(valid_years.max())

        year_range = st.slider("Year range", year_min, year_max, (year_min, year_max))

    mask = (
        df["status"].isin(sel_status)
        & df["category"].isin(sel_cats)
        & df["store"].isin(sel_stores)
        & df["year"].between(*year_range)
    )
    dff = df[mask].copy()

    known = df[df["status"] != "Unknown"]
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total reviews", f"{len(df):,}")
    c2.metric("Unique apps", df["app_name"].nunique())
    c3.metric("Active app reviews", f"{(known['status'] == 'Active').sum():,}")
    c4.metric("Discontinued app reviews", f"{(known['status'] == 'Discontinued').sum():,}")
    c5.metric("Stores", df["store"].nunique())

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📋 App Status",
        "🔬 Feature Survival",
        "📈 Rating Trends",
        "💬 Review Evidence",
        "ℹ️ Methodology",
    ])

    with tab1:
        st.subheader("App Status Summary")

        summary = (
            dff[dff["status"] != "Unknown"]
            .groupby(["app_name", "status", "category", "store"])
            .agg(
                reviews=("rating", "count"),
                avg_rating=("rating", "mean"),
                first_review=("date", "min"),
                last_review=("date", "max"),
            )
            .reset_index()
        )

        summary = (
            summary.groupby(["app_name", "status", "category"])
            .agg(
                reviews=("reviews", "sum"),
                avg_rating=("avg_rating", "mean"),
                first_review=("first_review", "min"),
                last_review=("last_review", "max"),
            )
            .reset_index()
        )

        if not summary.empty:
            summary["avg_rating"] = summary["avg_rating"].round(2)
            summary["first_review"] = summary["first_review"].dt.strftime("%Y-%m-%d")
            summary["last_review"] = summary["last_review"].dt.strftime("%Y-%m-%d")
            summary = summary.sort_values(
                "status",
                key=lambda s: s.map({"Active": 0, "Stale": 1, "Discontinued": 2, "Unknown": 3}),
            )

        def colour_status(val):
            c = {"Active": "#d4edda", "Stale": "#fff3cd", "Discontinued": "#f8d7da"}.get(val, "")
            return f"background-color: {c}" if c else ""

        if not summary.empty:
            styled = summary.style.applymap(colour_status, subset=["status"])
            st.dataframe(styled, use_container_width=True, height=480)
        else:
            st.info("No app summary available for the selected filters.")

        pie_data = (
            dff[dff["status"] != "Unknown"]
            .groupby("status")["rating"]
            .count()
            .reset_index()
            .rename(columns={"rating": "reviews"})
        )

        if not pie_data.empty:
            fig_pie = px.pie(
                pie_data,
                names="status",
                values="reviews",
                color="status",
                color_discrete_map=STATUS_COLORS,
                title="Review share by survival status",
            )
            st.plotly_chart(fig_pie, use_container_width=True)

        csv = summary.to_csv(index=False).encode()
        st.download_button("⬇ Download table as CSV", csv, "survivorship_app_summary.csv", "text/csv")

    with tab2:
        st.subheader("Feature Mention Rates: Surviving vs Discontinued")
        st.caption(
            "% of reviews mentioning each feature keyword bucket. "
            "Higher bars = feature more prominent in that group's reviews."
        )

        known_df = dff[dff["status"].isin(["Active", "Discontinued"])].copy()
        feat_cols = list(FEATURE_PATTERNS.keys())
        feat_labels = [f.replace("feat_", "").title() for f in feat_cols]

        feat_by_status = (
            known_df.groupby("status")[feat_cols]
            .mean()
            .mul(100)
            .round(1)
            .T
            .rename_axis("feature")
            .reset_index()
        )

        if not feat_by_status.empty:
            feat_by_status["feature"] = feat_labels

            fig_feat = go.Figure()
            for status in ["Active", "Discontinued"]:
                if status in feat_by_status.columns:
                    fig_feat.add_bar(
                        x=feat_by_status["feature"],
                        y=feat_by_status[status],
                        name=status,
                        marker_color=STATUS_COLORS[status],
                    )

            fig_feat.update_layout(
                barmode="group",
                xaxis_title="Feature bucket",
                yaxis_title="% of reviews mentioning",
                legend_title="Status",
                height=420,
            )
            st.plotly_chart(fig_feat, use_container_width=True)

            if "Active" in feat_by_status.columns and "Discontinued" in feat_by_status.columns:
                feat_by_status["delta"] = feat_by_status["Active"] - feat_by_status["Discontinued"]
                feat_by_status = feat_by_status.sort_values("delta", ascending=True)

                fig_delta = px.bar(
                    feat_by_status,
                    x="delta",
                    y="feature",
                    orientation="h",
                    color="delta",
                    color_continuous_scale=["#e74c3c", "#ffffff", "#2ecc71"],
                    color_continuous_midpoint=0,
                    title="Δ Feature mention rate (Active − Discontinued)",
                    labels={"delta": "Δ %", "feature": "Feature"},
                )
                fig_delta.update_layout(height=380, showlegend=False)
                st.plotly_chart(fig_delta, use_container_width=True)
        else:
            st.info("No feature survival comparison available for the selected filters.")

        st.subheader("Feature mentions by category")
        cat_feat = (
            dff[dff["status"] != "Unknown"]
            .groupby("category")[feat_cols]
            .mean()
            .mul(100)
            .round(1)
        )

        if not cat_feat.empty:
            cat_feat.columns = feat_labels
            fig_heat = px.imshow(
                cat_feat,
                color_continuous_scale="Greens",
                title="Feature mention heatmap by app category (%)",
                aspect="auto",
            )
            st.plotly_chart(fig_heat, use_container_width=True)

    with tab3:
        st.subheader("Average Rating Over Time by Survival Status")

        trend = (
            dff[dff["status"].isin(["Active", "Discontinued"])]
            .groupby(["quarter", "status"])["rating"]
            .agg(["mean", "count"])
            .reset_index()
            .rename(columns={"mean": "avg_rating", "count": "n_reviews"})
        )

        if not trend.empty:
            trend = trend[trend["n_reviews"] >= 3]

        if not trend.empty:
            fig_trend = px.line(
                trend,
                x="quarter",
                y="avg_rating",
                color="status",
                color_discrete_map=STATUS_COLORS,
                markers=True,
                title="Avg rating per quarter — Active vs Discontinued apps",
                labels={"quarter": "Quarter", "avg_rating": "Avg Rating"},
            )
            fig_trend.update_layout(height=420)
            st.plotly_chart(fig_trend, use_container_width=True)
        else:
            st.info("No rating trend data available for the selected filters.")

        st.subheader("Rating Distribution by Survival Status")
        box_df = dff[dff["status"].isin(STATUS_ORDER)]
        if not box_df.empty:
            fig_box = px.box(
                box_df,
                x="status",
                y="rating",
                color="status",
                color_discrete_map=STATUS_COLORS,
                category_orders={"status": STATUS_ORDER},
                title="Rating distribution — Active / Stale / Discontinued",
            )
            st.plotly_chart(fig_box, use_container_width=True)

        st.subheader("Per-app rating trend (top apps by volume)")
        top_apps = (
            dff[dff["status"] != "Unknown"]
            .groupby("app_name")["rating"]
            .count()
            .nlargest(15)
            .index.tolist()
        )

        app_q = (
            dff[dff["app_name"].isin(top_apps)]
            .groupby(["app_name", "quarter", "status"])["rating"]
            .mean()
            .reset_index()
        )

        if not app_q.empty:
            fig_apps = px.line(
                app_q,
                x="quarter",
                y="rating",
                color="app_name",
                line_dash="status",
                title="Quarterly avg rating — top 15 apps",
                labels={"rating": "Avg Rating", "quarter": "Quarter"},
            )
            fig_apps.update_layout(height=480, legend=dict(orientation="v", font_size=10))
            st.plotly_chart(fig_apps, use_container_width=True)

    with tab4:
        st.subheader("Explore Reviews by App & Feature")

        col_a, col_b = st.columns(2)
        sel_app = col_a.selectbox("App", ["(All)"] + sorted(dff["app_name"].dropna().unique()))
        sel_feat = col_b.selectbox(
            "Feature bucket",
            ["(Any)"] + [f.replace("feat_", "").title() for f in FEATURE_PATTERNS],
        )

        rev_df = dff.copy()
        if sel_app != "(All)":
            rev_df = rev_df[rev_df["app_name"] == sel_app]
        if sel_feat != "(Any)":
            col_key = "feat_" + sel_feat.lower()
            rev_df = rev_df[rev_df[col_key] == 1]

        rev_df = rev_df.sort_values("date", ascending=False)

        st.write(f"Showing **{len(rev_df):,}** reviews")
        if not rev_df.empty:
            st.dataframe(
                rev_df[["date", "app_name", "status", "store", "rating", "review_text"]]
                .rename(columns={"review_text": "review"})
                .head(500),
                use_container_width=True,
                height=440,
            )

        st.subheader("Top feature keywords by status")
        feat_cols = list(FEATURE_PATTERNS.keys())
        feat_labels = [f.replace("feat_", "").title() for f in feat_cols]

        for status in ["Active", "Discontinued"]:
            sub = dff[dff["status"] == status]
            if sub.empty:
                continue
            rates = sub[feat_cols].mean().mul(100).round(1)
            rates.index = feat_labels
            top3 = rates.nlargest(3)
            st.markdown(
                f"**{status}** — top features: "
                + ", ".join(f"{k} ({v:.0f}%)" for k, v in top3.items())
            )

        csv2 = (
            dff[["date", "app_name", "status", "category", "store", "rating", "review_text"] + feat_cols]
            .to_csv(index=False)
            .encode()
        )
        st.download_button(
            "⬇ Download enriched reviews CSV",
            csv2,
            "survivorship_reviews_enriched.csv",
            "text/csv",
        )

    with tab5:
        st.subheader("Methodology")
        st.markdown("""
### Data Sources
| File | Store | Period |
|------|-------|--------|
| `2019_03_19-play_reviews_unique.csv` | Google Play | up to March 2019 |
| `2019-03-21_apple_reviews_unique.csv` | Apple App Store | up to March 2019 |
| `2019-02-08-chrome_reviews_unique.csv` | Chrome Web Store | up to February 2019 |

### App Registry
Apps are mapped from the **Lyngs (2022)** catalogue of self-regulation tools.
Each app is assigned:
- **Status** (Active / Stale / Discontinued) — manually verified May 2026
- **Category** — Timer/Pomodoro, Block/Removal, Tracking/Analytics, Timer/Gamification, Nudge/Reflection, Scheduling/Planning

### Feature Keyword Mining
Each review is scanned with 8 regex pattern buckets:

| Bucket | Keywords (sample) |
|--------|------------------|
| Block | block, blacklist, restrict, lock |
| Timer | timer, pomodoro, countdown, session |
| Track | track, usage, stats, screen time |
| Reward | reward, coins, tree, badge, streak |
| Nudge | nudge, remind, mindful, reflect |
| Schedule | schedule, whitelist, routine, habit |
| ADHD | adhd, focus, distract, productive |
| Social | facebook, instagram, twitter, reddit |

### Survivorship Comparison
- **Feature mention rate** = fraction of an app-group's reviews mentioning a feature bucket
- **Δ** = Active rate − Discontinued rate; positive = feature more associated with surviving apps
- **Rating trend** = quarterly mean rating, filtered to quarters with ≥ 3 reviews

### Limitations
- Reviews are a 2019 **point-in-time snapshot** — survivorship is measured 7 years later
- No NLP negation (`"no timer"` still scores as a timer mention)
- Chrome apps are matched by title (no stable extension ID in the CSV)
- Unknown apps = apps in the review files not found in the Lyngs registry
""")

        st.subheader("Files expected")
        for f in [
            "2019_03_19-play_reviews_unique.csv",
            "2019-03-21_apple_reviews_unique.csv",
            "2019-02-08-chrome_reviews_unique.csv",
        ]:
            p = _find(f)
            icon = "✅" if p else "❌"
            st.write(f"{icon} `{f}`" + (f" → `{p}`" if p else " (not found)"))


def render_survivorship_page():
    survivorship_page()


if __name__ == "__main__":
    st.set_page_config(page_title="Survivorship Analysis", layout="wide")
    survivorship_page()