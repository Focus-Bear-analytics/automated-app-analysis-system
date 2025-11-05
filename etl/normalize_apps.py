# etl/normalize_apps.py
import math, datetime as _dt, re, copy
import argparse
import json
from pathlib import Path

import pandas as pd
from dateutil import parser as dparser

# --- pricing helpers ---
PRICE_RX = re.compile(r'[$£€]\s?(\d+(?:\.\d{1,2})?)')

def parse_play_range(text: str):
    """Extract min/max numbers from strings like '$0.99 - $21.99 per item'."""
    if not isinstance(text, str):
        return float("nan"), float("nan")
    vals = [float(m.group(1)) for m in PRICE_RX.finditer(text)]
    if not vals:
        return float("nan"), float("nan")
    return min(vals), max(vals)

# ---------- generic helpers ----------
def to_iso_date(val):
    if val is None or (isinstance(val, float) and pd.isna(val)) or (isinstance(val, str) and val.strip() == ""):
        return pd.NA
    try:
        return dparser.parse(str(val)).date().isoformat()
    except Exception:
        return pd.NA

def parse_int_from_human(s):
    """Turn '50,000+' or '700,000 users' into 50000 / 700000 (int) if possible."""
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return pd.NA
    s = str(s).lower().replace("users", "").replace("+", "").replace(",", " ").strip()
    m = re.search(r"(\d[\d\s]*)", s)
    if not m:
        return pd.NA
    digits = m.group(1).replace(" ", "")
    try:
        return int(digits)
    except Exception:
        return pd.NA

def first_existing_series(df: pd.DataFrame, candidates: list[str], default=pd.NA):
    for c in candidates:
        if c in df.columns:
            return df[c]
    return pd.Series([default] * len(df))

def first_nonempty_series(df: pd.DataFrame, candidates: list[str], default=pd.NA):
    for c in candidates:
        if c in df.columns and df[c].notna().any():
            return df[c]
    return pd.Series([default] * len(df))

def load_any(path: Path) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Input file not found: {p}")

    if p.suffix.lower() == ".jsonl":
        rows = []
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
        return pd.DataFrame(rows)
    elif p.suffix.lower() == ".json":
        obj = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(obj, list):
            return pd.DataFrame(obj)
        elif isinstance(obj, dict):
            return pd.DataFrame([obj])
        else:
            raise ValueError("Unsupported JSON structure (expected list or dict).")
    else:
        if p.suffix.lower() in {".xlsx", ".xls"}:
            try:
                return pd.read_excel(p)
            except Exception as e:
                raise RuntimeError(
                    f"Failed to read Excel file {p}. "
                    f"Install openpyxl (pip install openpyxl). Original error: {e}"
                )
        return pd.read_csv(p)

# ---------- relevance (legacy helper retained for backward-compat) ----------
def _to_float_safe(x, default=0.0) -> float:
    try:
        if x is None or pd.isna(x):
            return default
    except Exception:
        pass
    try:
        return float(x)
    except Exception:
        return default

def compute_relevance(row, include_terms, exclude_terms) -> float:
    """
    Lightweight relevance in [0,1]:
    - +1 per include-term occurrence
    - -2 per exclude-term occurrence
    - small boosts for popularity (ratings / installs)
    """
    fields = [row.get("title"), row.get("description"), row.get("category"), row.get("developer")]
    text = " ".join([str(x) for x in fields if isinstance(x, str)]).lower()

    inc_hits = sum(text.count(t) for t in include_terms)
    exc_hits = sum(text.count(t) for t in exclude_terms)

    raw = inc_hits - 2 * exc_hits
    base = max(0.0, float(raw))
    denom = max(5.0, float(len(include_terms)))  # keep scale reasonable
    score = base / denom

    # tiny popularity nudges (safe handling of <NA>)
    rc = _to_float_safe(row.get("rating_count"), 0.0)
    if rc >= 10000:
        score += 0.10
    elif rc >= 1000:
        score += 0.05
    elif 0 < rc < 25:
        score -= 0.10

    inst = _to_float_safe(row.get("installs_or_users"), 0.0)
    if inst >= 10_000_000:
        score += 0.10
    elif inst >= 1_000_000:
        score += 0.05

    return round(max(0.0, min(1.0, score)), 3)

# ---------- relevance v2 (field-weighted, phrase-aware, popularity & recency) ----------
SCORING_CFG = {
    "include_terms": [
        "focus","focused","productivity","productive","pomodoro","timer","countdown",
        "study","study timer","deep work","habit","routine","adhd","mindful","meditation",
        "block","blocker","website blocker","site blocker","distraction","parental","screen time"
    ],
    "include_phrases": [
        "site blocker","website blocker","parental control","screen time","focus timer","study timer","deep work"
    ],
    "exclude_terms": [
        "wallpaper","theme","ringtone","launcher","keyboard","icon","vpn","antivirus",
        "camera","photo","video","music","game","gallery","widget"
    ],
    "exclude_phrases": [],
    "field_weights": {"title": 3.0, "category": 2.0, "description": 1.0, "developer": 0.5},
    "allowed_categories": {
        "PlayStore": {"PRODUCTIVITY","EDUCATION","HEALTH_AND_FITNESS"},
        "ChromeWS":  {"Productivity","Education","Utilities"},
        "AppStore":  {"Productivity","Education","Health & Fitness"},
    },
    "popularity": {"ratings_max_bonus": 0.18, "installs_max_bonus": 0.18},
    "recency": {"fresh_days": 365, "stale_days": 365*3, "fresh_bonus": 0.10, "stale_penalty": 0.10},
}

def _count_word(word, text):  # whole-word matches (avoid "block" -> "blockchain")
    return len(re.findall(rf"\b{re.escape(word)}\b", text))

def _count_phrase(phrase, text):
    return 1 if re.search(rf"\b{re.escape(phrase)}\b", text) else 0

def _to_date(val):
    try:
        return dparser.parse(str(val)).date()
    except Exception:
        return None

def _log_bonus(x, cap):
    try:
        x = float(x or 0)
        if x <= 0: return 0.0
        return cap * min(1.0, math.log10(x)/7.0)  # gentle log scale
    except Exception:
        return 0.0

def compute_relevance_v2(row, cfg=SCORING_CFG) -> float:
    title       = "" if pd.isna(row.get("title")) else str(row.get("title")).lower()
    desc        = "" if pd.isna(row.get("description")) else str(row.get("description")).lower()
    category    = "" if pd.isna(row.get("category")) else str(row.get("category")).lower()
    developer   = "" if pd.isna(row.get("developer")) else str(row.get("developer")).lower()
    store       = str(row.get("store") or "")
    rating_cnt  = _to_float_safe(row.get("rating_count"), 0.0)
    installs    = _to_float_safe(row.get("installs_or_users"), 0.0)
    last_update = row.get("last_update")

    fw = cfg["field_weights"]
    inc_terms, inc_phrases = [t.lower() for t in cfg["include_terms"]], [t.lower() for t in cfg["include_phrases"]]
    exc_terms, exc_phrases = [t.lower() for t in cfg["exclude_terms"]], [t.lower() for t in cfg["exclude_phrases"]]

    def score_field(text, weight, term_mult=1.0, exc_mult=1.0):
        sc = 0.0
        for w in inc_terms: sc += term_mult * _count_word(w, text)
        for p in inc_phrases: sc += 2.0 * _count_phrase(p, text)
        for w in exc_terms: sc -= 1.0 * exc_mult * _count_word(w, text)
        for p in exc_phrases: sc -= 2.0 * exc_mult * _count_phrase(p, text)
        return sc * weight

    text_score = 0.0
    # heavier penalty influence in title/category than description
    text_score += score_field(title,     fw["title"],      term_mult=1.0, exc_mult=2.0)
    text_score += score_field(category,  fw["category"],   term_mult=0.8, exc_mult=1.5)
    text_score += score_field(desc,      fw["description"],term_mult=0.6, exc_mult=0.5)
    text_score += score_field(developer, fw["developer"],  term_mult=0.4, exc_mult=0.5)

    text_part = max(0.0, min(1.0, text_score / 7.0))  # compress to ~0..1

    cat_bonus = 0.0
    allow = cfg["allowed_categories"]
    if store in allow and row.get("category") in allow[store]: cat_bonus = 0.12
    if any(x in category for x in ["wallpaper","ringtones","themes","games"]): cat_bonus -= 0.08

    ratings_bonus  = _log_bonus(rating_cnt, cfg["popularity"]["ratings_max_bonus"])
    installs_bonus = _log_bonus(installs,   cfg["popularity"]["installs_max_bonus"])

    recency_bonus = 0.0
    lu = _to_date(last_update)
    if lu:
        days = (_dt.date.today() - lu).days
        if days <= SCORING_CFG["recency"]["fresh_days"]: recency_bonus += SCORING_CFG["recency"]["fresh_bonus"]
        elif days >= SCORING_CFG["recency"]["stale_days"]: recency_bonus -= SCORING_CFG["recency"]["stale_penalty"]

    score = text_part + cat_bonus + ratings_bonus + installs_bonus + recency_bonus
    return round(max(0.0, min(1.0, score)), 3)

def _augment_scoring_with_cli(cfg: dict, include_terms_cli: list[str], exclude_terms_cli: list[str]) -> dict:
    """Add CLI include/exclude terms to the scorer config (lowercased, deduped)."""
    cfg = copy.deepcopy(cfg)
    if include_terms_cli:
        base = [t.lower() for t in cfg.get("include_terms", [])]
        extra = [t.lower() for t in include_terms_cli if t.strip()]
        cfg["include_terms"] = list(dict.fromkeys(base + extra))
    if exclude_terms_cli:
        base = [t.lower() for t in cfg.get("exclude_terms", [])]
        extra = [t.lower() for t in exclude_terms_cli if t.strip()]
        cfg["exclude_terms"] = list(dict.fromkeys(base + extra))
    return cfg

# ---------- main transform ----------
def main(in_path, out_path, include_terms, exclude_terms):
    df = load_any(Path(in_path))
    n = len(df)
    out = pd.DataFrame(index=range(n))

    # identifiers
    out["store"]   = first_existing_series(df, ["store"], default="PlayStore")
    out["id"]      = first_existing_series(df, ["id", "app_id", "package"])
    out["app_key"] = first_existing_series(df, ["app_key"])

    # display/meta
    out["title"]      = first_existing_series(df, ["title", "name"])
    out["developer"]  = first_existing_series(df, ["developer", "offered_by", "seller"])
    out["category"]   = first_existing_series(df, ["genre", "category"])

    # ratings
    out["rating_avg"]   = first_existing_series(df, ["rating_avg", "ratingValue", "score"])
    out["rating_count"] = first_existing_series(df, ["rating_count", "ratingCount", "ratings"])

    # description
    out["description"] = first_nonempty_series(df, ["description", "description_full", "summary"])

    # links/extras
    out["website_url"] = first_nonempty_series(df, ["website_url", "website", "url"])
    out["store_url"]   = first_existing_series(df, ["store_url"])
    out["icon_url"]    = first_existing_series(df, ["icon_url"])
    out["version"]     = first_existing_series(df, ["version"])

    # pricing (raw) + normalized IAP range
    out["pricing_raw"] = first_existing_series(df, ["pricing_raw"])
    out["iap_min"]     = pd.to_numeric(first_existing_series(df, ["iap_min"]), errors="coerce")
    out["iap_max"]     = pd.to_numeric(first_existing_series(df, ["iap_max"]), errors="coerce")

    # Fill missing iap_min/max from Play-style pricing_raw ranges
    fill_mask = out["iap_min"].isna() & out["iap_max"].isna() & out["pricing_raw"].notna()
    if fill_mask.any():
        parsed = out.loc[fill_mask, "pricing_raw"].apply(parse_play_range)
        mins = pd.to_numeric(parsed.apply(lambda t: t[0]), errors="coerce")
        maxs = pd.to_numeric(parsed.apply(lambda t: t[1]), errors="coerce")
        out.loc[fill_mask, "iap_min"] = mins.values
        out.loc[fill_mask, "iap_max"] = maxs.values

    # installs / users
    out["installs_or_users"] = first_existing_series(df, ["installs_or_users", "installs", "users"]).apply(parse_int_from_human)

    # dates
    out["release_date"] = first_existing_series(df, ["release_date", "released", "releaseDate"]).apply(to_iso_date)
    out["last_update"]  = first_existing_series(df, ["last_update", "updated", "lastUpdated"]).apply(to_iso_date)
    out["scraped_at"]   = first_existing_series(df, ["scraped_at", "scrapedAt"]).apply(to_iso_date)

    # relevance (computed here) — use v2 + merge in CLI terms
    cfg = _augment_scoring_with_cli(SCORING_CFG, include_terms, exclude_terms)
    out["relevance_score"] = out.apply(lambda r: compute_relevance_v2(r, cfg), axis=1)

    # convenience slug id
    def slugify_title(s):
        s = "" if pd.isna(s) else str(s).lower()
        return re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    store_norm = out["store"].astype(str).str.lower().str.replace(" ", "", regex=False)
    out["app_id"] = out["title"].apply(slugify_title) + "_" + store_norm

    # final columns (removed: rating_histogram, currency, iap_count, price_app, content_rating, requires_android)
    cols = [
        "app_key", "store", "id", "app_id",
        "title", "developer", "category",
        "installs_or_users", "rating_avg", "rating_count",
        "iap_min", "iap_max", "pricing_raw",
        "description",
        "website_url", "store_url", "icon_url", "version",
        "release_date", "last_update", "relevance_score", "scraped_at",
    ]
    out = out[cols]

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)
    print(f"[normalize_apps] wrote {len(out)} rows -> {out_path}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True)
    ap.add_argument("--out", dest="out_path", default="data/curated/apps.csv")
    ap.add_argument(
        "--include-terms",
        default="focus,block,timer,pomodoro,productivity,habit,distraction,parental",
        help="Comma-separated relevance include terms (added to built-in list)"
    )
    ap.add_argument(
        "--exclude-terms",
        default="wallpaper,theme,ringtone,launcher,keyboard,icon pack",
        help="Comma-separated relevance exclude terms (added to built-in list)"
    )
    args = ap.parse_args()
    include_terms = [t.strip().lower() for t in args.include_terms.split(",") if t.strip()]
    exclude_terms = [t.strip().lower() for t in args.exclude_terms.split(",") if t.strip()]
    main(args.in_path, args.out_path, include_terms, exclude_terms)
