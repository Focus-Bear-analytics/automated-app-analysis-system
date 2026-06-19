# etl/clean_apps.py
import argparse
import re
from pathlib import Path

import pandas as pd
import yaml
from dateutil import parser as dparser

DISCOVERY_CFG = Path("scrapers/discovery.yml")


def load_discovery_filters() -> dict:
    """
    Load filter terms from scrapers/discovery.yml.

    This allows clean_apps.py to use:
    - weak_scope_terms
    - required_strong_terms
    - include_terms
    - exclude_terms
    from the same config used by the scraper pipeline.
    """
    if not DISCOVERY_CFG.exists():
        return {}

    try:
        cfg = yaml.safe_load(DISCOVERY_CFG.read_text(encoding="utf-8")) or {}
        return cfg.get("filters", {}) or {}
    except Exception:
        return {}


EXCLUDE_TITLES = {
    "grok", "perplexity", "perplexity - ask anything", "microsoft office",
    "microsoft 365", "google docs", "google sheets", "google slides",
    "google drive", "gmail", "chatgpt", "claude", "gemini", "copilot",
}

FORCE_INCLUDE_TITLES = {
    "todo list - focus tasks",
    "mindshield - digital wellbeing tool",
    "circle: stay focused",
    "dwt - procrastination tracker",
    "blockit | stop phone addiction",
    "bosla - time boxing, 2nd brain",
    "study planner: task splitter",
    "timebox planner",
    "visual schedule: daily planner",
    "adhd-friendly todo list",
    "byebyedistraction",
    "addiction blocker",
    "beyond - digital wellbeing",
    "focused todo",
    "productivitypro - smart time tracker",
}

EXCLUDE_DEVELOPERS = {
    "x.ai", "xai", "perplexity ai", "perplexityai", "microsoft corporation",
    "openai", "anthropic", "anthropic pbc", "google llc",
}

EXCLUDE_CATEGORIES = {
    "games", "game", "entertainment", "photo & video", "music",
    "social networking", "shopping", "news", "weather", "finance", "business",
    "travel", "food & drink", "sports", "utilities", "tools", "personalization",
    "communication", "dating", "navigation", "reference",
}

POSITIVE_SCOPE_HINTS = [
    "focus", "focused", "productivity", "productive", "pomodoro", "timer",
    "study", "concentration", "deep work", "habit", "routine", "goal",
    "task", "todo", "adhd", "planner", "reminder", "meditation", "mindfulness",
    "distraction", "block", "blocker", "website blocker", "site blocker",
    "app blocker", "focus mode", "screen time", "digital wellbeing", "parental control",
]

STRONG_NON_COMPETITOR_TERMS = [
    "chatbot", "chat bot", "ai assistant", "ai chatbot", "office suite",
    "document editor", "spreadsheet", "presentation", "word processor",
    "pdf editor", "search engine", "search assistant", "general ai",
    "file manager", "battery saver", "qr scanner", "photo editor", "video editor",
    "music player", "wallpaper", "theme", "ringtone", "launcher", "keyboard",
    "icon", "vpn", "antivirus", "camera", "photo", "video", "game",
    "gallery", "widget", "movie", "streaming", "shopping",
    "dating", "news", "weather", "calculator", "cleaner", "browser",
]

# Fallback weak terms.
# These words are too generic to prove an app is a true focus competitor.
DEFAULT_WEAK_SCOPE_TERMS = [
    "timer",
    "planner",
    "habit",
    "task",
    "todo",
    "reminder",
    "goal",
    "schedule",
    "calendar",
    "journal",
    "tracker",
    "wellness",
    "mind",
    "motivation",
]

# Fallback required strong terms.
# If an app has only weak terms, it must also contain at least one of these.
DEFAULT_REQUIRED_STRONG_TERMS = [
    "focus",
    "focused",
    "productivity",
    "productive",
    "pomodoro",
    "adhd",
    "executive function",
    "concentration",
    "deep work",
    "distraction",
    "blocker",
    "website blocker",
    "site blocker",
    "app blocker",
    "block apps",
    "screen time",
    "digital wellbeing",
    "digital wellness",
    "digital detox",
    "routine",
    "time blocking",
    "time management",
    "study focus",
    "focus mode",
]


def to_iso_date(val):
    if val is None or (isinstance(val, float) and pd.isna(val)) or (isinstance(val, str) and not val.strip()):
        return pd.NA
    try:
        return dparser.parse(str(val)).date().isoformat()
    except Exception:
        return pd.NA


def norm_cols(df):
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower().str.replace(r"[^a-z0-9]+", "_", regex=True)
    for c in df.select_dtypes(include=["object"]).columns:
        df[c] = df[c].astype(str).str.strip().replace({"": pd.NA, "nan": pd.NA})
    return df


def map_store(s):
    if not isinstance(s, str):
        return pd.NA
    t = s.strip().lower()
    if t in {"playstore", "googleplay", "google_play", "android"}:
        return "PlayStore"
    if t in {"appstore", "ios", "apple_app_store", "apple"}:
        return "AppStore"
    if t in {"chromews", "chrome_web_store", "chromewebstore", "cws"}:
        return "ChromeWS"
    return s


def norm_text(value) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip().lower()


def contains_word_or_phrase(text: str, term: str) -> bool:
    text = norm_text(text)
    term = norm_text(term)
    if not term:
        return False
    if " " in term:
        return term in text
    # whole-word style matching prevents game from matching management
    return re.search(r"(?<![a-z0-9])" + re.escape(term) + r"(?![a-z0-9])", text) is not None


def row_text(row) -> str:
    return " ".join([
        norm_text(row.get("title")),
        norm_text(row.get("description")),
        norm_text(row.get("category")),
        norm_text(row.get("developer")),
    ])


def is_explicitly_excluded(row) -> bool:
    title = norm_text(row.get("title"))
    developer = norm_text(row.get("developer"))

    for bad_title in EXCLUDE_TITLES:
        if title == bad_title or bad_title in title:
            return True

    for bad_dev in EXCLUDE_DEVELOPERS:
        if developer == bad_dev or bad_dev in developer:
            return True

    return False

def is_force_included(row) -> bool:
    """
    Keep manually verified relevant apps even if they fail popularity
    thresholds or weak generic scope checks.
    """
    title = norm_text(row.get("title"))

    for good_title in FORCE_INCLUDE_TITLES:
        if title == good_title or good_title in title:
            return True

    return False


def is_excluded_category(row) -> bool:
    category = norm_text(row.get("category"))
    return category in EXCLUDE_CATEGORIES


def has_scope_signal(row) -> bool:
    text = row_text(row)
    return any(contains_word_or_phrase(text, term) for term in POSITIVE_SCOPE_HINTS)

def passes_weak_scope_rule(row, filters: dict) -> bool:
    """
    Prevent apps from being kept only because they contain generic words
    such as timer, planner, habit, task, journal, or tracker.

    Rule:
    - If weak terms are present, at least one required strong term must also be present.
    - If no weak terms are present, this rule does not block the app.
    """
    text = row_text(row)

    weak_terms = filters.get("weak_scope_terms") or DEFAULT_WEAK_SCOPE_TERMS
    required_strong_terms = filters.get("required_strong_terms") or DEFAULT_REQUIRED_STRONG_TERMS

    has_weak_term = any(
        contains_word_or_phrase(text, term)
        for term in weak_terms
    )

    has_required_strong_term = any(
        contains_word_or_phrase(text, term)
        for term in required_strong_terms
    )

    # If the app has weak terms but no strong focus/productivity/ADHD signal,
    # reject it.
    if has_weak_term and not has_required_strong_term:
        return False

    return True


def has_strong_non_competitor_signal(row) -> bool:
    text = row_text(row)
    return any(contains_word_or_phrase(text, term) for term in STRONG_NON_COMPETITOR_TERMS)


def main(in_csv, out_keep, out_drop, min_rating_count, min_play_installs, min_cws_users, min_relevance):
    filters = load_discovery_filters()

    df = pd.read_csv(in_csv, low_memory=False)
    df = norm_cols(df)

    if "store" in df.columns:
        df["store"] = df["store"].apply(map_store)

    for c in ("rating_count", "installs_or_users", "relevance_score"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    for c in ("release_date", "last_update", "scraped_at"):
        if c in df.columns:
            df[c] = df[c].apply(to_iso_date)

    if "app_key" not in df.columns:
        if {"store", "id"}.issubset(df.columns):
            df["app_key"] = df["store"].astype(str) + "::" + df["id"].astype(str)
        else:
            df["app_key"] = df.index.astype(str)

    # Dedupe by app_key:
    # keep latest scraped_at, then highest rating_count.
    if "scraped_at" in df.columns:
        df["_t"] = pd.to_datetime(df["scraped_at"], errors="coerce")
    else:
        df["_t"] = pd.NaT

    df["_rc"] = pd.to_numeric(df.get("rating_count"), errors="coerce")

    df = (
        df.sort_values(["_t", "_rc"], ascending=[True, True])
        .drop_duplicates(subset=["app_key"], keep="last")
        .drop(columns=["_t", "_rc"])
    )

    # Minimal viability:
    # title or description must exist; avoid junk 1-character titles.
    have_text = df.get("title").notna() | df.get("description").notna()
    junk = df.get("title", pd.Series([""] * len(df), index=df.index)).astype(str).str.len() < 2
    need_ok = have_text & ~junk

    rc = pd.to_numeric(df.get("rating_count"), errors="coerce").fillna(0)
    ins = pd.to_numeric(df.get("installs_or_users"), errors="coerce").fillna(0)
    store = df.get("store").fillna("")

    ok_signal = (
        (rc >= min_rating_count)
        | ((store.eq("PlayStore")) & (ins >= min_play_installs))
        | ((store.eq("ChromeWS")) & (ins >= min_cws_users))
    )

    rel = pd.to_numeric(df.get("relevance_score"), errors="coerce").fillna(0)
    ok_relevance = rel >= min_relevance

    explicit_exclude = df.apply(is_explicitly_excluded, axis=1)
    force_include = df.apply(is_force_included, axis=1)
    category_exclude = df.apply(is_excluded_category, axis=1)
    scope_signal = df.apply(has_scope_signal, axis=1)
    weak_scope_ok = df.apply(lambda row: passes_weak_scope_rule(row, filters), axis=1)
    non_competitor_signal = df.apply(has_strong_non_competitor_signal, axis=1)

    ok_scope = (
    ~explicit_exclude
    & ~category_exclude
    & scope_signal
    & weak_scope_ok
    & ~non_competitor_signal
)

# Force-included apps are manually verified as relevant.
# They are allowed to bypass weak-scope and popularity problems,
# but they should still NOT be explicitly excluded.
    ok_scope_with_force_include = (
    ok_scope
    | (
        force_include
        & ~explicit_exclude
        & ~category_exclude
        & ~non_competitor_signal
    )
)
    
    
    


    # -------------------------------------------------------------------
    # High relevance override
    # -------------------------------------------------------------------
    # Problem:
    # Some real focus / ADHD / Pomodoro / app-blocking tools are small or new.
    # They may have low ratings, installs, or Chrome Web Store users, but they
    # should still be kept if their relevance score is very strong.
    #
    # Logic:
    # Keep the app if it is:
    # - valid,
    # - relevant,
    # - in scope,
    # - and either popular enough OR highly relevant.
    # -------------------------------------------------------------------

    high_relevance_focus_app = (
    (rel >= 0.85)
    & ok_scope_with_force_include
)

    medium_relevance_chrome_extension = (
    store.eq("ChromeWS")
    & (rel >= 0.70)
    & ok_scope_with_force_include
)

    keep_mask = (
    need_ok
    & (
        ok_relevance
        | force_include
    )
    & ok_scope_with_force_include
    & (
        ok_signal
        | high_relevance_focus_app
        | medium_relevance_chrome_extension
        | force_include
    )
)

    kept = df[keep_mask].copy()
    dropped = df[~keep_mask].copy()
    kept["kept_by_force_include"] = force_include[keep_mask].values

    # Optional debugging columns.
    # These help you inspect which apps were retained due to the override.
    # You can remove these later if you do not want them in apps_clean.csv.
    kept["kept_by_high_relevance_override"] = high_relevance_focus_app[keep_mask].values
    kept["kept_by_chrome_relevance_override"] = medium_relevance_chrome_extension[keep_mask].values

    reasons = []
    for i in dropped.index:
        why = []

        if not need_ok.loc[i]:
            why.append("missing_title_and_description")

        if (
            not ok_signal.loc[i]
            and not high_relevance_focus_app.loc[i]
            and not medium_relevance_chrome_extension.loc[i]
        ):
            why.append("below_popularity_thresholds")

        if not ok_relevance.loc[i]:
            why.append("low_relevance")

        if explicit_exclude.loc[i]:
            why.append("explicit_exclude_list")

        if category_exclude.loc[i]:
            why.append("excluded_category")

        if not scope_signal.loc[i]:
            why.append("not_focus_adhd_productivity_competitor")

        if not weak_scope_ok.loc[i]:
            why.append("weak_scope_without_required_strong_signal")

        if non_competitor_signal.loc[i]:
            why.append("strong_non_competitor_signal")

        if force_include.loc[i]:
            why.append("force_include_requested_but_blocked_by_exclusion_or_invalid_data")

        reasons.append(",".join(why) or "unknown")

    dropped["drop_reason"] = reasons

    Path(out_keep).parent.mkdir(parents=True, exist_ok=True)
    kept.to_csv(out_keep, index=False)
    dropped.to_csv(out_drop, index=False)

    print(f"[clean_apps] kept={len(kept)} dropped={len(dropped)} -> {out_keep} / {out_drop}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()

    ap.add_argument("--in", dest="in_csv", default="data/curated/apps_all.csv")
    ap.add_argument("--out", dest="out_keep", default="data/curated/apps_clean.csv")
    ap.add_argument("--out-dropped", dest="out_drop", default="data/curated/apps_dropped.csv")

    ap.add_argument("--min-rating-count", type=int, default=10)
    ap.add_argument("--min-play-installs", type=int, default=50000)
    ap.add_argument("--min-cws-users", type=int, default=10000)
    ap.add_argument("--min-relevance", type=float, default=0.15)

    args = ap.parse_args()

    main(
        args.in_csv,
        args.out_keep,
        args.out_drop,
        args.min_rating_count,
        args.min_play_installs,
        args.min_cws_users,
        args.min_relevance,
    )