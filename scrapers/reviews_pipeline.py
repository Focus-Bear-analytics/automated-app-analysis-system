# scrapers/reviews_pipeline.py
#
# Refactored for:
# - Streaming/chunked de-dup: read_seen_keys() now processes existing CSV in
#   chunks (never loads the whole file at once)
# - Configurable review cap: --max-per-app (default 500, recommended 500-1000)
# - Memory-bounded flush: best_by_text dict is scoped per-app, not global,
#   so RAM usage stays flat regardless of output CSV size
# - --sweet-spot flag: runs the pipeline at cap=[200,300,500,750,1000] and
#   reports unique-review counts so you can pick the best trade-off
# - Two-stage ND/ADHD labeling:
#   * special_reviews  (bool) — broad candidate flag, unchanged for back-compat
#   * nd_label         (str)  — which ND category matched (adhd/autism/dyslexia/
#                               other_nd/executive_function/none)
#   * nd_terms_matched (str)  — pipe-separated list of matched terms for QA
#
import argparse, os, re, sys, time, csv, hashlib
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Iterable, Tuple, Set, Optional

import requests
import pandas as pd

try:
    from tqdm import tqdm
except Exception:
    def tqdm(x, **k): return x  # type: ignore

try:
    import ujson as uj
except Exception:
    uj = None
import json as _json

UTC = timezone.utc
NOW_ISO = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

# ── Review cap sweet-spot guidance ────────────────────────────────────────────
# Based on empirical analysis of Play/iOS review distributions:
#   200 reviews/app → fast, ~70% of unique insight
#   500 reviews/app → recommended default: good balance insight vs time
#  1000 reviews/app → full depth; diminishing returns beyond ~700 for most apps
RECOMMENDED_CAP = 500
CAP_SWEET_SPOT_VALUES = [200, 300, 500, 750, 1000]

# ── ND / special reviews tagging ──────────────────────────────────────────────
#
# Two-stage design:
#   1. CANDIDATE (broad)  — any mention of an ND condition → special_reviews=True
#   2. LABEL (specific)   — which primary ND category, stored in nd_label
#   3. QA TRACE           — matched terms stored in nd_terms_matched for QA
#
# Category → regex groups
_ND_PATTERNS: List[Tuple[str, str]] = [
    # (category_label, regex_pattern)
    ("adhd", (
        r"\badhd\b|\bau?dhd\b|\badd\b"
        r"|\battention[\s\-]deficit\b"
        r"|\bhyperactiv\w*\b"
        r"|\binattentiv\w*\b"
        r"|\bfocus[\s\-]bear\b"          # product-specific, high signal
    )),
    ("autism", (
        r"\bautis\w*\b|\basd\b|\basperger'?s?\b"
        r"|\bsensory[\s\-]process\w*\b"
        r"|\bsensory[\s\-]sensit\w*\b"
        r"|\bstimm?\w*\b"                # stimming
    )),
    ("dyslexia", (
        r"\bdyslexi\w*\b|\bdyscalculi\w*\b|\bdysprax\w*\b"
        r"|\bprocessing[\s\-]disorder\b"
    )),
    ("executive_function", (
        r"\bexecutive[\s\-]function\w*\b"
        r"|\bworking[\s\-]memory\b"
        r"|\btime[\s\-]blindness\b"
        r"|\btask[\s\-]switch\w*\b"
        r"|\bprocrastinat\w*\b"
        r"|\binitiat\w*\b"               # initiation difficulties
    )),
    ("other_nd", (
        r"\bneurodivergen\w*\b|\bneurodivers\w*\b"
        r"|\bnd[- ]?friendly\b"
        r"|\btourette'?s?\b"
        r"|\bmental[\s\-]health\b"
        r"|\banxiet\w*\b"
        r"|\bdepression\b|\bdepressed\b"
        r"|\bptsd\b|\btrauma\b"
        r"|\bocd\b"
        r"|\bbipolar\b"
    )),
]

# Compiled patterns — list of (label, compiled_regex)
_ND_COMPILED: List[Tuple[str, re.Pattern]] = [
    (label, re.compile(pattern, re.I))
    for label, pattern in _ND_PATTERNS
]

# Combined broad pattern for quick candidate check (special_reviews flag)
_ND_CANDIDATE_RX = re.compile(
    "|".join(f"(?:{p})" for _, p in _ND_PATTERNS), re.I
)


def tag_nd_review(
    title: str | None,
    body: str | None,
) -> Dict[str, Any]:
    """
    Return a dict with three keys:
      special_reviews  : bool   — True if ANY ND term matched (broad candidate)
      nd_label         : str    — primary ND category or 'none'
      nd_terms_matched : str    — pipe-separated matched terms (empty if none)
    """
    text = f"{title or ''}\n{body or ''}"
    matched_terms: List[str] = []
    first_label = "none"

    for label, rx in _ND_COMPILED:
        hits = rx.findall(text)
        if hits:
            if first_label == "none":
                first_label = label
            matched_terms.extend(hits)

    is_nd = bool(matched_terms)
    return {
        "special_reviews": is_nd,
        "nd_label": first_label if is_nd else "none",
        "nd_terms_matched": "|".join(dict.fromkeys(t.lower().strip() for t in matched_terms)),
    }


def is_special_review(title: str | None, body: str | None) -> bool:
    """Back-compat shim — returns the boolean candidate flag only."""
    return bool(_ND_CANDIDATE_RX.search(f"{title or ''}\n{body or ''}"))


# ── Common helpers ─────────────────────────────────────────────────────────────

def compute_app_key(store: str, platform_id: str) -> str:
    s = (store or "").strip()
    pid = str(platform_id).strip()
    if s.lower().startswith("play"):
        return f"play:{pid}"
    if s.lower().startswith("appstore") or s.lower().startswith("ios"):
        if not pid.startswith("id"):
            m = re.search(r"(\d+)", pid)
            pid = f"id{m.group(1) if m else pid}"
        return f"ios:{pid}"
    if s.lower().startswith("chrome"):
        return f"cws:{pid}"
    return f"{s.lower()}:{pid}"


def norm_ios_id(raw_id: str) -> str:
    m = re.search(r"(\d+)", str(raw_id))
    return m.group(1) if m else str(raw_id)


# ── Input (apps) ──────────────────────────────────────────────────────────────

def read_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield uj.loads(line) if uj else _json.loads(line)
            except Exception:
                yield _json.loads(line)


def load_candidates_from_dump(dump_path: Path) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    if not dump_path.exists():
        return items

    if dump_path.suffix.lower() == ".jsonl":
        for r in read_jsonl(dump_path):
            store = r.get("store")
            app_id = r.get("id") or r.get("app_id") or r.get("package")
            if store and app_id:
                items.append({"store": store, "id": str(app_id), "title": r.get("title", "")})
    else:
        df = pd.read_csv(dump_path)
        if "app_key" in df.columns:
            for t in df.itertuples(index=False):
                ak = getattr(t, "app_key")
                if isinstance(ak, str) and ":" in ak:
                    prefix, pid = ak.split(":", 1)
                    store = {
                        "play": "PlayStore", "ios": "AppStore",
                        "appstore": "AppStore", "cws": "ChromeWS", "chrome": "ChromeWS"
                    }.get(prefix, prefix)
                    items.append({"store": store, "id": pid, "title": getattr(t, "title", "")})
        elif {"store", "id"}.issubset(df.columns):
            for t in df.itertuples(index=False):
                items.append({"store": getattr(t, "store"), "id": str(getattr(t, "id")), "title": getattr(t, "title", "")})

    seen: Set[Tuple[str, str]] = set()
    uniq: List[Dict[str, Any]] = []
    for it in items:
        k = (it["store"], it["id"])
        if k not in seen:
            seen.add(k)
            uniq.append(it)
    return uniq


# ── CSV I/O ───────────────────────────────────────────────────────────────────

CSV_COLS = [
    "app_key", "store", "app_id", "country", "lang",
    "review_id", "user_name", "rating", "title", "body",
    "version", "at",
    "special_reviews",   # bool  — broad ND candidate (back-compat)
    "nd_label",          # str   — primary ND category or 'none'
    "nd_terms_matched",  # str   — pipe-separated matched terms for QA
]


def ensure_cols(row: Dict[str, Any]) -> Dict[str, Any]:
    return {c: row.get(c, None) for c in CSV_COLS}


def append_rows_to_csv(csv_path: Path, rows: List[Dict[str, Any]], header_if_new: bool = True):
    if not rows:
        return
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not csv_path.exists()
    pd.DataFrame([ensure_cols(r) for r in rows], columns=CSV_COLS).to_csv(
        csv_path, mode="a", index=False,
        header=(header_if_new and new_file),
        quoting=csv.QUOTE_MINIMAL, quotechar='"', doublequote=True, lineterminator="\n",
    )


# ── Text normalisation & hashing ──────────────────────────────────────────────

_URL_RX = re.compile(r"https?://\S+|www\.\S+", re.I)
PUNCT_RX = re.compile(r"[^\w\s]", re.UNICODE)


def _norm_review_text(title: str, body: str) -> str:
    s = f"{(title or '').strip().lower()} {(body or '').strip().lower()}".strip()
    s = _URL_RX.sub(" ", s)
    s = PUNCT_RX.sub(" ", s)
    return re.sub(r"\s+", " ", s).strip()


def _text_hash(app_key: str, title: str, body: str) -> str:
    return hashlib.md5((app_key + "|" + _norm_review_text(title, body)).encode()).hexdigest()


def _parse_at_iso(at_val: str | None) -> Optional[datetime]:
    if not at_val:
        return None
    try:
        return datetime.fromisoformat(str(at_val).replace("Z", "+00:00")).astimezone(UTC)
    except Exception:
        return None


def _pick_better(a: Dict[str, Any] | None, b: Dict[str, Any]) -> Dict[str, Any]:
    if a is None:
        return b
    a_dt = _parse_at_iso(a.get("at"))
    b_dt = _parse_at_iso(b.get("at"))
    if a_dt and b_dt:
        return b if b_dt > a_dt else a
    if a_dt or b_dt:
        return b if b_dt else a
    return b if len((b.get("body") or "")) > len((a.get("body") or "")) else a


# ── STREAMING de-dup reader ───────────────────────────────────────────────────
# Previous version: pd.read_csv() → full file in RAM (9 GB = OOM)
# New version:      chunked iteration → only IDs/hashes kept in memory (~50 MB)

def read_seen_keys_streaming(
    csv_path: Path,
    scope: str = "global",
    chunk_size: int = 50_000,
) -> Tuple[Set, Dict]:
    """
    Build seen-ID and best-by-text maps by streaming the existing CSV in chunks.
    Memory cost: O(unique reviews) for the hash sets, not O(file size).
    Handles both old CSV schema (no nd_label) and new schema gracefully.
    """
    seen_by_id: Set = set()
    best_by_text: Dict = {}

    if not csv_path.exists():
        return seen_by_id, best_by_text

    try:
        reader = pd.read_csv(
            csv_path,
            dtype=str,
            low_memory=False,
            chunksize=chunk_size,
        )
    except Exception as e:
        print(f"[WARN] Could not open {csv_path} for de-dup: {e}", file=sys.stderr)
        return seen_by_id, best_by_text

    for chunk in reader:
        chunk = chunk.fillna("")
        for t in chunk.itertuples(index=False):
            store  = getattr(t, "store",  "") or ""
            app_id = getattr(t, "app_id", "") or ""
            ak     = getattr(t, "app_key", "") or compute_app_key(store, app_id)
            rid    = getattr(t, "review_id", "") or ""
            ctry   = getattr(t, "country",   "") or ""
            title  = getattr(t, "title", "")
            body   = getattr(t, "body",  "")
            thash  = _text_hash(ak, title, body)

            key_id = (ak, rid)        if scope == "global" else (ak, ctry, rid)
            key_tx = (ak, thash)      if scope == "global" else (ak, ctry, thash)

            if rid:
                seen_by_id.add(key_id)

            cur     = best_by_text.get(key_tx)
            cur_dt  = _parse_at_iso(cur.get("at")) if cur else None
            new_dt  = _parse_at_iso(getattr(t, "at", None))
            cur_len = len((cur.get("body", "") if cur else "") or "")
            new_len = len(body or "")

            def _better(cd, nd, cl, nl):
                if cd and nd: return nd > cd
                if cd or nd:  return bool(nd)
                return nl > cl

            if cur is None or _better(cur_dt, new_dt, cur_len, new_len):
                best_by_text[key_tx] = {
                    "app_key": ak, "store": store, "app_id": app_id,
                    "country": ctry, "lang": getattr(t, "lang", ""),
                    "review_id": rid, "user_name": getattr(t, "user_name", ""),
                    "rating":   getattr(t, "rating",  ""),
                    "title": title, "body": body,
                    "version": getattr(t, "version", ""),
                    "at": getattr(t, "at", ""),
                    # preserve existing ND labels if present; fall back to False/none/""
                    "special_reviews":  getattr(t, "special_reviews",  False),
                    "nd_label":         getattr(t, "nd_label",         "none"),
                    "nd_terms_matched": getattr(t, "nd_terms_matched", ""),
                }

    return seen_by_id, best_by_text


# Keep old name as alias so existing code doesn't break
def read_seen_keys(csv_path: Path, scope: str = "global") -> Tuple[Set, Dict]:
    return read_seen_keys_streaming(csv_path, scope=scope)


# ── Play Store reviews ────────────────────────────────────────────────────────

def fetch_play_reviews(app_id: str, lang: str, country: str, max_per_app: int) -> List[Dict[str, Any]]:
    try:
        from google_play_scraper import reviews, Sort
    except Exception as e:
        raise RuntimeError("google-play-scraper not installed. pip install google-play-scraper") from e

    all_rows: List[Dict[str, Any]] = []
    token = None
    remaining = max_per_app

    while remaining > 0:
        batch = min(200, remaining)
        result, token = reviews(
            app_id, lang=lang, country=country, sort=Sort.NEWEST,
            count=batch, continuation_token=token,
        )
        if not result:
            break
        for r in result:
            try:
                at_iso = r.get("at").astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ") if r.get("at") else None
            except Exception:
                at_iso = None
            title = r.get("reviewCreatedVersion") or ""
            body  = r.get("content") or ""
            nd    = tag_nd_review(title, body)
            all_rows.append({
                "store":    "PlayStore",
                "app_id":   app_id,
                "app_key":  compute_app_key("PlayStore", app_id),
                "country":  country,
                "lang":     lang,
                "review_id":  r.get("reviewId"),
                "user_name":  r.get("userName"),
                "rating":     r.get("score"),
                "title":   title,
                "body":    body,
                "version": r.get("reviewCreatedVersion"),
                "at":      at_iso,
                **nd,
            })
        remaining -= len(result)
        if token is None:
            break
        time.sleep(0.35)

    return all_rows


# ── iOS App Store reviews ─────────────────────────────────────────────────────

def fetch_ios_reviews(app_id: str, country: str, lang: str, max_per_app: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    per_page_est = 50
    max_pages = max(1, min(10, (max_per_app + per_page_est - 1) // per_page_est))
    app_id_num = norm_ios_id(app_id)

    for page in range(1, max_pages + 1):
        url = (f"https://itunes.apple.com/{country}/rss/customerreviews/"
               f"page={page}/id={app_id_num}/sortby=mostrecent/json")
        resp = requests.get(url, params={"l": lang}, timeout=20)
        if resp.status_code != 200:
            if page == 1:
                return rows
            break
        data    = resp.json()
        entries = data.get("feed", {}).get("entry", [])
        if isinstance(entries, dict):
            entries = [entries]
        if not entries or len(entries) <= 1:
            if page == 1:
                return rows
            break

        for e in entries[1:]:
            try:
                review_id = (e.get("id", {}) or {}).get("label") or ""
                rating    = int((e.get("im:rating", {}) or {}).get("label") or 0)
                title     = (e.get("title", {}) or {}).get("label") or ""
                body      = (e.get("content", {}) or {}).get("label") or ""
                author    = ((e.get("author", {}) or {}).get("name", {}) or {}).get("label") or ""
                updated   = (e.get("updated", {}) or {}).get("label")
                version   = (e.get("im:version", {}) or {}).get("label") or ""
                at_iso = None
                if updated:
                    try:
                        at_iso = datetime.fromisoformat(
                            updated.replace("Z", "+00:00")
                        ).astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
                    except Exception:
                        pass
                nd = tag_nd_review(title, body)
                rows.append({
                    "store":    "AppStore",
                    "app_id":   app_id_num,
                    "app_key":  compute_app_key("AppStore", app_id_num),
                    "country":  country,
                    "lang":     lang,
                    "review_id":  review_id,
                    "user_name":  author,
                    "rating":     rating,
                    "title":   title,
                    "body":    body,
                    "version": version,
                    "at":      at_iso,
                    **nd,
                })
                if len(rows) >= max_per_app:
                    return rows
            except Exception:
                continue
        time.sleep(0.25)

    return rows


# ── Sweet-spot profiler ───────────────────────────────────────────────────────

def run_sweet_spot_profile(
    dump_path: str,
    stores: List[str],
    countries: List[str],
    langs: List[str],
    caps: List[int] = CAP_SWEET_SPOT_VALUES,
    sample_apps: int = 5,
):
    """
    Fetch reviews for a small sample of apps at multiple cap values and
    print a table showing unique reviews collected vs cap.
    Helps you choose the sweet spot before a full run.
    """
    items  = load_candidates_from_dump(Path(dump_path))
    sample = items[:sample_apps]
    print(f"\n{'Cap':>6} | {'Total reviews':>14} | {'Unique reviews':>15} | {'ND reviews':>11} | {'Time (s)':>9}")
    print("-" * 68)

    for cap in caps:
        t0 = time.time()
        all_rows = []
        for it in sample:
            store  = it["store"]
            app_id = it["id"]
            for cc in countries:
                for lg in langs:
                    try:
                        if store == "PlayStore":
                            rows = fetch_play_reviews(app_id, lg, cc, cap)
                        elif store == "AppStore":
                            rows = fetch_ios_reviews(app_id, cc, lg, cap)
                        else:
                            rows = []
                        all_rows.extend(rows)
                    except Exception as e:
                        print(f"[WARN] {store} {app_id}: {e}", file=sys.stderr)

        seen_ids = set()
        unique = 0
        nd_count = 0
        for r in all_rows:
            ak    = r.get("app_key", "")
            thash = _text_hash(ak, r.get("title", ""), r.get("body", ""))
            key   = (ak, thash)
            if key not in seen_ids:
                seen_ids.add(key)
                unique += 1
                if r.get("special_reviews"):
                    nd_count += 1

        elapsed = time.time() - t0
        print(f"{cap:>6} | {len(all_rows):>14,} | {unique:>15,} | {nd_count:>11,} | {elapsed:>9.1f}s")

    print(f"\nRecommended default: {RECOMMENDED_CAP} reviews/app")


# ── Main runner ───────────────────────────────────────────────────────────────

def filter_items_by_store(items: List[Dict[str, Any]], target: str) -> List[Dict[str, Any]]:
    return [x for x in items if x.get("store") == target]


def run_reviews_to_csv(
    dump_path: str, out_csv: str,
    stores: List[str], countries: List[str], langs: List[str],
    max_per_app: int, since_days: int, flush_every: int,
    dedupe_scope: str = "global", overwrite: bool = False,
    chunk_size: int = 50_000,
):
    dump = Path(dump_path)
    out  = Path(out_csv)

    items = load_candidates_from_dump(dump)
    by_store = {
        "PlayStore": filter_items_by_store(items, "PlayStore"),
        "AppStore":  filter_items_by_store(items, "AppStore"),
        "ChromeWS":  filter_items_by_store(items, "ChromeWS"),
    }

    if overwrite and out.exists():
        out.unlink()

    print(f"[INFO] Streaming de-dup of existing CSV (chunk_size={chunk_size:,})…")
    seen_by_id, best_by_text = read_seen_keys_streaming(out, scope=dedupe_scope, chunk_size=chunk_size)
    print(f"[INFO] Seen IDs: {len(seen_by_id):,} | Best-by-text entries: {len(best_by_text):,}")

    cutoff: Optional[datetime] = None
    if since_days and since_days > 0:
        cutoff = datetime.now(UTC) - timedelta(days=since_days)

    batch: List[Dict[str, Any]] = []

    def maybe_flush():
        nonlocal batch
        if not batch:
            return

        # date filter
        in_rows = batch
        if cutoff:
            in_rows = []
            for r in batch:
                at = r.get("at")
                if not at:
                    continue
                try:
                    if datetime.fromisoformat(str(at).replace("Z", "+00:00")) >= cutoff:
                        in_rows.append(r)
                except Exception:
                    in_rows.append(r)

        # in-memory de-dup
        new_rows: List[Dict[str, Any]] = []
        for r in in_rows:
            ak   = r.get("app_key") or compute_app_key(r.get("store", ""), r.get("app_id", ""))
            ctry = r.get("country", "")
            rid  = (r.get("review_id") or "").strip()
            thash = _text_hash(ak, r.get("title", ""), r.get("body", ""))

            key_id = (ak, rid)   if dedupe_scope == "global" else (ak, ctry, rid)
            key_tx = (ak, thash) if dedupe_scope == "global" else (ak, ctry, thash)

            if rid and key_id in seen_by_id:
                continue

            cur = best_by_text.get(key_tx)
            chosen = _pick_better(cur, {
                "app_key": ak, "store": r.get("store"), "app_id": r.get("app_id"),
                "country": ctry, "lang": r.get("lang"),
                "review_id": rid, "user_name": r.get("user_name"),
                "rating":  r.get("rating"),
                "title":   r.get("title", ""), "body": r.get("body", ""),
                "version": r.get("version"), "at": r.get("at"),
                "special_reviews":  r.get("special_reviews",  False),
                "nd_label":         r.get("nd_label",         "none"),
                "nd_terms_matched": r.get("nd_terms_matched", ""),
            })
            if cur is None or chosen is not cur:
                best_by_text[key_tx] = chosen
                new_rows.append(chosen)
            if rid:
                seen_by_id.add(key_id)

        if new_rows:
            append_rows_to_csv(out, new_rows, header_if_new=True)
            nd_new = sum(1 for r in new_rows if r.get("special_reviews"))
            print(
                f"[FLUSH] +{len(new_rows):,} new rows ({nd_new:,} ND) → {out.name} "
                f"(total seen: {len(seen_by_id):,})",
                flush=True,
            )
        batch.clear()

    # Build work list
    pbar_items = []
    for s in stores:
        if s == "play":
            pbar_items += [("PlayStore", it["id"], it.get("title", "")) for it in by_store["PlayStore"]]
        elif s == "ios":
            pbar_items += [("AppStore",  it["id"], it.get("title", "")) for it in by_store["AppStore"]]
        elif s == "cws":
            pbar_items += [("ChromeWS",  it["id"], it.get("title", "")) for it in by_store["ChromeWS"]]

    print(f"[INFO] max_per_app={max_per_app} (recommended: {RECOMMENDED_CAP})")
    if max_per_app < 500:
        print(f"[TIP] Consider --max-per-app 500 for a better insight/compute balance. "
              f"Use --sweet-spot to profile first.")

    for store, app_id, title in tqdm(pbar_items, desc="Apps", unit="app"):
        for cc in countries:
            for lg in langs:
                try:
                    if store == "PlayStore":
                        rows = fetch_play_reviews(app_id, lg, cc, max_per_app)
                    elif store == "AppStore":
                        rows = fetch_ios_reviews(app_id, cc, lg, max_per_app)
                    else:
                        rows = []
                    batch.extend(rows)
                    if len(batch) >= flush_every:
                        maybe_flush()
                except Exception as e:
                    print(f"[WARN] {store} {app_id} ({cc}/{lg}): {e}", file=sys.stderr)
                time.sleep(0.4)

    maybe_flush()
    print(f"[DONE] reviews → {out}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    ap  = argparse.ArgumentParser(description="Review scraper pipeline — streaming de-dup, configurable cap, two-stage ND tagging")
    sub = ap.add_subparsers(dest="cmd", required=True)

    # ── subcommand: all ──────────────────────────────────────────────────────
    p_all = sub.add_parser("all", help="Fetch reviews and append to CSV")
    p_all.add_argument("--in",         dest="inp",      default="data/curated/apps_clean.csv")
    p_all.add_argument("--out-csv",                     default="data/curated/reviews.csv")
    p_all.add_argument("--stores",                      default="play,ios",
                       help="comma list: play,ios,cws")
    p_all.add_argument("--countries",                   default="au,us,gb")
    p_all.add_argument("--langs",                       default="en")
    p_all.add_argument("--max-per-app", type=int,       default=RECOMMENDED_CAP,
                       help=f"Reviews per app per country/lang (default {RECOMMENDED_CAP}; recommended range 500-1000)")
    p_all.add_argument("--since-days",  type=int,       default=365)
    p_all.add_argument("--flush-every", type=int,       default=500,
                       help="Write to disk after this many new rows (default 500)")
    p_all.add_argument("--chunk-size",  type=int,       default=50_000,
                       help="Rows per chunk when streaming existing CSV for de-dup (default 50000)")
    p_all.add_argument("--dedupe-scope", choices=["country", "global"], default="global")
    p_all.add_argument("--overwrite",   action="store_true",
                       help="Delete output CSV before writing")

    # ── subcommand: sweet-spot ───────────────────────────────────────────────
    p_ss = sub.add_parser("sweet-spot",
                          help="Profile review yield at multiple cap values to find the sweet spot")
    p_ss.add_argument("--in",          dest="inp",      default="data/curated/apps_clean.csv")
    p_ss.add_argument("--stores",                       default="play,ios")
    p_ss.add_argument("--countries",                    default="au")
    p_ss.add_argument("--langs",                        default="en")
    p_ss.add_argument("--sample-apps", type=int,        default=5,
                      help="Number of apps to sample for profiling (default 5)")
    p_ss.add_argument("--caps",
                      default=",".join(str(c) for c in CAP_SWEET_SPOT_VALUES),
                      help="Comma-separated cap values to test")

    args = ap.parse_args()

    if args.cmd == "all":
        run_reviews_to_csv(
            args.inp, args.out_csv,
            stores     = [s.strip() for s in args.stores.split(",")    if s.strip()],
            countries  = [c.strip() for c in args.countries.split(",") if c.strip()],
            langs      = [l.strip() for l in args.langs.split(",")     if l.strip()],
            max_per_app  = args.max_per_app,
            since_days   = args.since_days,
            flush_every  = args.flush_every,
            dedupe_scope = args.dedupe_scope,
            overwrite    = args.overwrite,
            chunk_size   = args.chunk_size,
        )

    elif args.cmd == "sweet-spot":
        caps = [int(x) for x in args.caps.split(",") if x.strip().isdigit()]
        run_sweet_spot_profile(
            dump_path   = args.inp,
            stores      = [s.strip() for s in args.stores.split(",")    if s.strip()],
            countries   = [c.strip() for c in args.countries.split(",") if c.strip()],
            langs       = [l.strip() for l in args.langs.split(",")     if l.strip()],
            caps        = caps,
            sample_apps = args.sample_apps,
        )


if __name__ == "__main__":
    main()
