# llm/feature_flags.py
import argparse
import re
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd


# ==============================
# Feature patterns (no capture groups)
# ==============================
# One pattern list per feature; we match these against:
# - app title
# - description
# - website_text
# - review title/body (lowercased)
#
# NOTE: use (?: ... ) for non-capturing groups and include \b word boundaries.
FEATURE_PATTERNS: Dict[str, List[str]] = {
    # ---- original set ----
    "blocking": [
        r"\b(?:block|blocking|blocker|blacklist|whitelist)\b",
        r"\b(?:app|site|website)\s*block(?:er|ing)?\b",
        r"\b(?:site|website)\s*filter(?:ing|s)?\b",
        r"\bnuclear\s*option\b",
        r"\bdistraction\s*block(?:er|ing)?\b",
        r"\bporn\s*block(?:er|ing)?\b",
        r"\bsocial(?:\s*media)?\s*block(?:er|ing)?\b",
        r"\bshorts\s*block(?:er|ing)?\b",
        r"\breels?\s*block(?:er|ing)?\b",
    ],
    "timer": [
        r"\btimers?\b",
        r"\bcount\s*down\b",
        r"\bcount\s*up\b",
        r"\btime\s*tracking\b",
        r"\bwork\s*sessions?\b",
        r"\bbreak(?:\s*(?:timer|mode))?\b",
        r"\bvisual\s*timer\b",
    ],
    "pomodoro": [
        r"\bpomodoro\b",
        r"\bpomo\b",
        r"\b25\s*min(?:ute)?\b",
        r"\b(?:4|four)\s*cycles?\b",
        r"\bshort\s*break\b",
        r"\blong\s*break\b",
    ],
    "habit_tracking": [
        r"\bhabits?\b",
        r"\bstreaks?\b",
        r"\bdaily\s*goals?\b",
        r"\broutines?\b",
        r"\bcheck\s*ins?\b",
        r"\bbuild\s*habits?\b",
        r"\bchain(s)?\b",
    ],
    "focus_mode": [
        r"\bfocus\s*mode\b",
        r"\bdeep\s*work\b",
        r"\bdistraction\s*free\b",
        r"\bconcentrat(?:e|ion)\b",
        r"\bflow\s*state\b",
        r"\bdo\s*not\s*disturb\b",
    ],
    "screen_time": [
        r"\bscreen\s*time\b",
        r"\busage\s*stats?\b",
        r"\bdigital\s*wellbeing\b",
        r"\bapp\s*usage\b",
        r"\bdowntime\b",
        r"\busage\s*limit(s)?\b",
    ],
    "parental_control": [
        r"\bparental\s*controls?\b",
        r"\bchild\s*lock\b",
        r"\bkid(?:s|z)\b",
        r"\bfamily\s*link\b",
        r"\bblock\s*(?:game|app)s?\s*for\s*(?:kids?|child)\b",
        r"\bremote\s*lock\b",
        r"\bapprove\s*downloads?\b",
        r"\bcontent\s*filter\b",
    ],
    "adhd_support": [
        r"\badhd\b",
        r"\badd\b",
        r"\bneuro\s*divergent\b",
        r"\bneurodivergent\b",
        r"\bneuro\s*diversit(?:y|ies)\b",
        r"\bautism\b",
        r"\basd\b",
        r"\b(?:adhd|asd)\s*(?:friendly|support|tool|tools)\b",
    ],
    "reminders": [
        r"\breminders?\b",
        r"\b(?:notify|notifications?)\b",
        r"\bnudges?\b",
        r"\bsnooze\b",
        r"\balarms?\b",
        r"\bpush\s*notifications?\b",
    ],
    "analytics": [
        r"\banalytics?\b",
        r"\bstat(?:s|istics)\b",
        r"\breports?\b",
        r"\binsights?\b",
        r"\bcharts?\b",
        r"\bdashboards?\b",
        r"\btrends?\b",
        r"\bweekly\s*report\b",
        r"\btime\s*breakdown\b",
        r"\bprogress\b",
    ],

    # ---- new set (10 more) ----
    "gamification": [
        r"\bgamif(?:ied|ication)\b",
        r"\bquests?\b",
        r"\bstreaks?\b",
        r"\bbadges?\b",
        r"\bpoints?\b",
        r"\blevels?\b",
    ],
    "rewards": [
        r"\brewards?\b",
        r"\bearn\s*(?:coins|points)\b",
        r"\ballowance\b",
        r"\bunlock\b",
    ],
    "calendar_integration": [
        r"\bcalendar\b",
        r"\bgoogle\s*calendar\b",
        r"\b(?:i|iCal|ical)\b",
        r"\boutlook\b",
        r"\bblock\s*calendar\s*time\b",
        r"\bcalendar\s*integration\b",
    ],
    "todo_integration": [
        r"\bto[- ]?do\b",
        r"\btasks?\b",
        r"\blists?\b",
        r"\b(todoist|notion|google\s*tasks?|microsoft\s*to\s*do)\b",
        r"\bintegration(s)?\b.*\b(todo|task|notion|todoist)\b",
    ],
    "browser_extension": [
        r"\b(?:chrome|firefox|safari)\s*(?:extension|add-?on)\b",
        r"\bbrowser\s*(?:extension|add-?on)\b",
        r"\bextension\b",
        r"\badd-?on\b",
    ],
    "web_filtering": [
        r"\bcontent\s*filter(?:ing)?\b",
        r"\bweb\s*filter(?:ing)?\b",
        r"\bporn(?:ography)?\s*filter\b",
        r"\bgambling\s*filter\b",
    ],
    "whitelist_blacklist": [
        r"\ballowlist\b",
        r"\bdenylist\b",
        r"\bwhitelist\b",
        r"\bblacklist\b",
    ],
    "scheduling": [
        r"\bschedules?\b",
        r"\bscheduled\b",
        r"\bbedtime\b",
        r"\bdowntime\b",
        r"\bcurfew\b",
        r"\broutines?\b",
        r"\bapp\s*limit(s)?\b",
    ],
    "cross_device_sync": [
        r"\bsync(?:hroni[sz]e|hronization)?\b",
        r"\bcross-?device\b",
        r"\bcloud\s*sync\b",
        r"\bmulti-?platform\b",
    ],
    "focus_music_noise": [
        r"\bfocus\s*music\b",
        r"\bbackground\s*sound(?:s)?\b",
        r"\b(?:white|brown|pink)\s*noise\b",
        r"\bambient\s*sound(?:s)?\b",
    ],
}


def _compile(patterns: List[str]) -> List[re.Pattern]:
    return [re.compile(p, re.I) for p in patterns]


COMPILED_PATTERNS: Dict[str, List[re.Pattern]] = {
    k: _compile(v) for k, v in FEATURE_PATTERNS.items()
}


# ==============================
# Loading helpers
# ==============================

def _load_apps(apps_csv: Path) -> pd.DataFrame:
    df = pd.read_csv(apps_csv)
    need = {"app_key", "title"}
    if not need.issubset(df.columns):
        raise SystemExit(f"{apps_csv} must include columns: {sorted(need)}")
    for c in ["store", "description", "category", "developer"]:
        if c not in df.columns:
            df[c] = ""
    return df[["app_key", "store", "title", "description", "category", "developer"]].copy()


def _load_web(web_csv: Path | None) -> pd.DataFrame:
    if not web_csv or not Path(web_csv).exists():
        return pd.DataFrame({"app_key": [], "website_text": []})
    w = pd.read_csv(web_csv)
    txt_col = None
    if "website_text" in w.columns:
        txt_col = "website_text"
    else:
        # try fuzzy
        for c in w.columns:
            if str(c).lower().startswith("website_text"):
                txt_col = c
                break
    if not txt_col or "app_key" not in w.columns:
        raise SystemExit(f"{web_csv} must contain app_key and website_text")
    w = w[["app_key", txt_col]].rename(columns={txt_col: "website_text"})
    w["website_text"] = w["website_text"].fillna("").astype(str)
    return w


def _load_reviews(rev_csv: Path | None) -> pd.DataFrame:
    if not rev_csv or not Path(rev_csv).exists():
        return pd.DataFrame({"app_key": [], "body": [], "title": []})
    cols = pd.read_csv(rev_csv, nrows=0).columns.tolist()
    pick = ["app_key"]
    if "body" in cols:
        pick.append("body")
    if "title" in cols:
        pick.append("title")
    r = pd.read_csv(rev_csv, usecols=pick)
    for c in ("body", "title"):
        if c not in r.columns:
            r[c] = ""
    r["body_low"] = r["body"].fillna("").astype(str).str.lower()
    r["title_low"] = r["title"].fillna("").astype(str).str.lower()
    return r[["app_key", "body_low", "title_low"]]


# ==============================
# Matching + scoring
# ==============================

def _any_match(text: str, patterns: List[re.Pattern]) -> bool:
    if not isinstance(text, str) or not text:
        return False
    t = text.lower()
    for p in patterns:
        if p.search(t):
            return True
    return False


def _signal_sources(row: pd.Series, patterns: List[re.Pattern]) -> Tuple[bool, List[str]]:
    """
    Return (flagged, sources) where sources ∈ {title, description, website, metadata}
    """
    sources: List[str] = []
    if _any_match(row.get("title", ""), patterns):
        sources.append("title")
    if _any_match(row.get("description", ""), patterns):
        sources.append("description")
    if _any_match(row.get("website_text", ""), patterns):
        sources.append("website")
    meta = f"{row.get('category','')} {row.get('developer','')}"
    if _any_match(meta, patterns):
        sources.append("metadata")
    return (len(sources) > 0, sources)


def _count_hits(series: pd.Series, patterns: List[re.Pattern]) -> int:
    """
    Count how many review rows mention the feature at least once.
    Avoids pandas .str.contains() capture-group warnings by using regex.search.
    """
    def any_hit(text: str) -> bool:
        if not isinstance(text, str):
            return False
        for p in patterns:
            if p.search(text):
                return True
        return False
    return int(series.apply(any_hit).sum())


def _confidence_from_sources(sources: List[str], review_hits: int) -> float:
    """
    Simple heuristic:
      - ≥2 sources -> 0.80
      - 1 source + ≥10 review hits -> 0.70
      - 1 source -> 0.60
      - 0 source + ≥25 review hits -> 0.55 (reviews-only signal)
      - else -> 0.00 (not flagged)
    """
    s = len(set(sources))
    if s >= 2:
        return 0.80
    if s == 1 and review_hits >= 10:
        return 0.70
    if s == 1:
        return 0.60
    if s == 0 and review_hits >= 25:
        return 0.55
    return 0.00


# ==============================
# Driver
# ==============================

def detect_feature(
    feature: str,
    apps_csv: Path,
    web_csv: Path | None,
    reviews_csv: Path | None,
    out_csv: Path,
):
    if feature not in COMPILED_PATTERNS:
        raise SystemExit(f"Unknown feature '{feature}'. Valid: {sorted(COMPILED_PATTERNS.keys())}")

    apps = _load_apps(apps_csv)
    web = _load_web(web_csv)
    df = apps.merge(web, on="app_key", how="left")

    reviews = _load_reviews(reviews_csv)
    has_reviews = not reviews.empty
    pats = COMPILED_PATTERNS[feature]

    rows: List[dict] = []
    for t in df.itertuples(index=False):
        row = t._asdict() if hasattr(t, "_asdict") else dict(t._mapping)  # safety
        flagged, sources = _signal_sources(pd.Series(row), pats)

        review_hits = 0
        if has_reviews:
            sub = reviews[reviews["app_key"] == row["app_key"]]
            if not sub.empty:
                series = (sub["body_low"].fillna("") + " " + sub["title_low"].fillna("")).str.strip()
                review_hits = _count_hits(series, pats)

        conf = _confidence_from_sources(sources, review_hits)
        if flagged or conf >= 0.55:
            rows.append({
                "app_key": row.get("app_key"),
                "title": row.get("title"),
                "confidence": round(conf, 2),
                "signals": ",".join(sources) if sources else ("reviews" if review_hits > 0 else ""),
                "review_hits": int(review_hits),
            })

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_df = pd.DataFrame(rows, columns=["app_key", "title", "confidence", "signals", "review_hits"])
    out_df.sort_values(["confidence", "review_hits", "title"], ascending=[False, False, True], inplace=True)

    n_flagged = len(out_df)
    n_total = len(df)
    out_df.to_csv(out_csv, index=False)
    print(f"[features] {feature}: {n_flagged:>2}/{n_total:>3} apps flagged -> {out_csv}")


def detect_all(
    apps_csv: Path,
    web_csv: Path | None,
    reviews_csv: Path | None,
    out_dir: Path,
):
    out_dir.mkdir(parents=True, exist_ok=True)
    for feat in COMPILED_PATTERNS.keys():
        out_csv = out_dir / f"features_{feat}.csv"
        detect_feature(feat, apps_csv, web_csv, reviews_csv, out_csv)


# ==============================
# CLI
# ==============================

def main():
    ap = argparse.ArgumentParser(description="Feature flags from app metadata, website text, and reviews.")
    ap.add_argument("--feature", default="all",
                    help="Feature key (one of: {}) or 'all'.".format(", ".join(sorted(COMPILED_PATTERNS.keys()))))
    ap.add_argument("--apps", required=True, help="CSV of apps (e.g., data/curated/apps_clean.csv)")
    ap.add_argument("--web", default=None, help="CSV of website text (e.g., data/curated/websites.csv)")
    ap.add_argument("--reviews", default=None, help="CSV of reviews (e.g., data/curated/reviews_with_sentiment.csv)")
    ap.add_argument("--out", default=None, help="Output CSV (when --feature != all)")
    ap.add_argument("--out-dir", default=None, help="Output directory (when --feature == all)")

    args = ap.parse_args()
    apps_csv = Path(args.apps)
    web_csv = Path(args.web) if args.web else None
    reviews_csv = Path(args.reviews) if args.reviews else None

    if args.feature == "all":
        if not args.out_dir:
            raise SystemExit("--out-dir is required when --feature all")
        detect_all(apps_csv, web_csv, reviews_csv, Path(args.out_dir))
    else:
        if not args.out:
            raise SystemExit("--out is required when --feature is a single feature")
        detect_feature(args.feature, apps_csv, web_csv, reviews_csv, Path(args.out))


if __name__ == "__main__":
    main()
