# scrapers/reviews_pipeline.py
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

# ------------------ ND / special reviews tagging ------------------
ND_TERMS = (
    r"\badhd\b|\bau?dhd\b|\badd\b|"
    r"\bneurodivergen\w*\b|\bneurodivers\w*\b|\bnd[- ]?friendly\b|"
    r"\bautis\w*\b|\basd\b|\basperger'?s?\b|"
    r"\bdyslexi\w*\b|\bdyscalculi\w*\b|\bdysprax\w*\b|"
    r"\btourette'?s?\b|"
    r"\bsensory\s+processing\b|\bexecutive\s+function\w*\b"
)
ND_RX = re.compile(ND_TERMS, re.I)

def is_special_review(title: str | None, body: str | None) -> bool:
    t = (title or "")
    b = (body or "")
    return bool(ND_RX.search(f"{t}\n{b}"))

# ------------------ common helpers ------------------

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

# ------------------ input (apps) ------------------

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
                        "play": "PlayStore", "ios": "AppStore", "appstore": "AppStore",
                        "cws": "ChromeWS", "chrome": "ChromeWS"
                    }.get(prefix, prefix)
                    items.append({"store": store, "id": pid, "title": getattr(t, "title", "")})
        elif {"store", "id"}.issubset(df.columns):
            for t in df.itertuples(index=False):
                items.append({"store": getattr(t, "store"), "id": str(getattr(t, "id")), "title": getattr(t, "title", "")})

    seen: Set[Tuple[str, str]] = set()
    uniq: List[Dict[str, Any]] = []
    for it in items:
        k = (it["store"], it["id"])
        if k in seen:
            continue
        seen.add(k)
        uniq.append(it)
    return uniq

# ------------------ CSV I/O ------------------

CSV_COLS = [
    "app_key", "store", "app_id", "country", "lang",
    "review_id", "user_name", "rating", "title", "body",
    "version", "at", "special_reviews"
]

def ensure_cols(row: Dict[str, Any]) -> Dict[str, Any]:
    return {c: row.get(c, None) for c in CSV_COLS}

def append_rows_to_csv(csv_path: Path, rows: List[Dict[str, Any]], header_if_new=True):
    if not rows:
        return
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not csv_path.exists()
    pd.DataFrame([ensure_cols(r) for r in rows], columns=CSV_COLS).to_csv(
        csv_path,
        mode="a",
        index=False,
        header=(header_if_new and new_file),
        quoting=csv.QUOTE_MINIMAL,
        quotechar='"',
        doublequote=True,
        lineterminator="\n",
    )

# ------------------ de-dup helpers (ID + TEXT) ------------------

_URL_RX = re.compile(r"https?://\S+|www\.\S+", re.I)
PUNCT_RX = re.compile(r"[^\w\s]", re.UNICODE)

def _norm_review_text(title: str, body: str) -> str:
    t = (title or "").strip().lower()
    b = (body or "").strip().lower()
    s = f"{t} {b}".strip()
    s = _URL_RX.sub(" ", s)
    s = PUNCT_RX.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _text_hash(app_key: str, title: str, body: str) -> str:
    return hashlib.md5((app_key + "|" + _norm_review_text(title, body)).encode("utf-8")).hexdigest()

def _parse_at_iso(at_val: str | None) -> Optional[datetime]:
    if not at_val:
        return None
    try:
        return datetime.fromisoformat(str(at_val).replace("Z", "+00:00")).astimezone(UTC)
    except Exception:
        return None

def read_seen_keys(csv_path: Path, scope: str = "global"):
    """
    Build 'seen' sets from existing CSV.
      - seen_by_id:   (app_key, review_id)            if scope='global'
                      (app_key, country, review_id)    if scope='country'
      - best_by_text: mapping key -> kept_row (choose best by date/length)
                      key is (app_key, text_hash) or (app_key, country, text_hash)
    """
    seen_by_id: set = set()
    best_by_text: dict = {}
    if not csv_path.exists():
        return seen_by_id, best_by_text

    try:
        base = pd.read_csv(csv_path, dtype=str, low_memory=False).fillna("")
    except Exception:
        return seen_by_id, best_by_text

    for t in base.itertuples(index=False):
        store = getattr(t, "store", "") or ""
        app_id = getattr(t, "app_id", "") or ""
        ak = getattr(t, "app_key", "") or compute_app_key(store, app_id)
        rid = getattr(t, "review_id", "") or ""
        ctry = getattr(t, "country", "") or ""
        title = getattr(t, "title", "")
        body  = getattr(t, "body", "")
        thash = _text_hash(ak, title, body)
        key_id = (ak, rid) if scope == "global" else (ak, ctry, rid)
        key_tx = (ak, thash) if scope == "global" else (ak, ctry, thash)

        if rid:
            seen_by_id.add(key_id)

        # choose best existing row
        cur = best_by_text.get(key_tx)
        cur_dt = _parse_at_iso(cur.get("at")) if cur else None
        new_dt = _parse_at_iso(getattr(t, "at", None))
        cur_len = len((cur.get("body","") if cur else "") or "")
        new_len = len(body or "")

        def better(_cur_dt, _new_dt, _cur_len, _new_len):
            if _cur_dt and _new_dt:
                return _new_dt > _cur_dt
            if _cur_dt or _new_dt:
                return bool(_new_dt)
            return _new_len > _cur_len

        if (cur is None) or better(cur_dt, new_dt, cur_len, new_len):
            best_by_text[key_tx] = {
                "app_key": ak, "store": store, "app_id": app_id,
                "country": ctry, "lang": getattr(t, "lang", ""),
                "review_id": rid, "user_name": getattr(t, "user_name", ""),
                "rating": getattr(t, "rating", ""),
                "title": title, "body": body,
                "version": getattr(t, "version", ""),
                "at": getattr(t, "at", ""),
                "special_reviews": getattr(t, "special_reviews", False)
            }
    return seen_by_id, best_by_text

def _pick_better(a: Dict[str,Any] | None, b: Dict[str,Any]) -> Dict[str,Any]:
    """Choose newer date; if tie/missing, longer body; else keep existing."""
    if a is None:
        return b
    a_dt = _parse_at_iso(a.get("at"))
    b_dt = _parse_at_iso(b.get("at"))
    if a_dt and b_dt:
        return b if b_dt > a_dt else a
    if a_dt or b_dt:
        return b if b_dt else a
    return b if len((b.get("body") or "")) > len((a.get("body") or "")) else a

# ------------------ Play reviews ------------------

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
            count=batch, continuation_token=token
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
            all_rows.append({
                "store": "PlayStore",
                "app_id": app_id,
                "app_key": compute_app_key("PlayStore", app_id),
                "country": country,
                "lang": lang,
                "review_id": r.get("reviewId"),
                "user_name": r.get("userName"),
                "rating": r.get("score"),
                "title": title,
                "body": body,
                "version": r.get("reviewCreatedVersion"),
                "at": at_iso,
                "special_reviews": is_special_review(title, body),
            })
        remaining -= len(result)
        if token is None:
            break
        time.sleep(0.35)
    return all_rows

# ------------------ iOS reviews ------------------

def fetch_ios_reviews(app_id: str, country: str, lang: str, max_per_app: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    per_page_est = 50
    max_pages = max(1, min(10, (max_per_app + per_page_est - 1) // per_page_est))
    app_id_num = norm_ios_id(app_id)

    for page in range(1, max_pages + 1):
        url = f"https://itunes.apple.com/{country}/rss/customerreviews/page={page}/id={app_id_num}/sortby=mostrecent/json"
        params = {"l": lang}
        resp = requests.get(url, params=params, timeout=20)
        if resp.status_code != 200:
            if page == 1:
                return rows
            break
        data = resp.json()
        feed = data.get("feed", {})
        entries = feed.get("entry", [])
        if isinstance(entries, dict):
            entries = [entries]
        if not entries or len(entries) <= 1:
            if page == 1:
                return rows
            break

        for e in entries[1:]:
            try:
                review_id = (e.get("id", {}) or {}).get("label") or ""
                rating = int((e.get("im:rating", {}) or {}).get("label") or 0)
                title = (e.get("title", {}) or {}).get("label") or ""
                body = (e.get("content", {}) or {}).get("label") or ""
                author = ((e.get("author", {}) or {}).get("name", {}) or {}).get("label") or ""
                updated = (e.get("updated", {}) or {}).get("label")
                version = (e.get("im:version", {}) or {}).get("label") or ""
                at_iso = None
                if updated:
                    try:
                        at_iso = datetime.fromisoformat(updated.replace("Z", "+00:00")).astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
                    except Exception:
                        at_iso = None
                rows.append({
                    "store": "AppStore",
                    "app_id": app_id_num,
                    "app_key": compute_app_key("AppStore", app_id_num),
                    "country": country,
                    "lang": lang,
                    "review_id": review_id,
                    "user_name": author,
                    "rating": rating,
                    "title": title,
                    "body": body,
                    "version": version,
                    "at": at_iso,
                    "special_reviews": is_special_review(title, body),
                })
                if len(rows) >= max_per_app:
                    return rows
            except Exception:
                continue
        time.sleep(0.25)
    return rows

# ------------------ runner ------------------

def filter_items_by_store(items: List[Dict[str, Any]], target: str) -> List[Dict[str, Any]]:
    return [x for x in items if x.get("store") == target]

def run_reviews_to_csv(dump_path: str, out_csv: str, stores: List[str], countries: List[str], langs: List[str],
                       max_per_app: int, since_days: int, flush_every: int,
                       dedupe_scope: str = "global", overwrite: bool = False):
    dump = Path(dump_path)
    out = Path(out_csv)

    items = load_candidates_from_dump(dump)
    by_store = {
        "PlayStore": filter_items_by_store(items, "PlayStore"),
        "AppStore":  filter_items_by_store(items, "AppStore"),
        "ChromeWS":  filter_items_by_store(items, "ChromeWS"),
    }

    if overwrite and out.exists():
        out.unlink()

    seen_by_id, best_by_text = read_seen_keys(out, scope=dedupe_scope)

    cutoff: Optional[datetime] = None
    if since_days and since_days > 0:
        cutoff = datetime.now(UTC) - timedelta(days=since_days)

    batch: List[Dict[str, Any]] = []

    def maybe_flush():
        nonlocal batch, seen_by_id, best_by_text
        if not batch:
            return

        # 1) date filter (if any)
        if cutoff:
            tmp = []
            for r in batch:
                at = r.get("at")
                if not at:
                    continue
                try:
                    dtv = datetime.fromisoformat(str(at).replace("Z", "+00:00"))
                    if dtv >= cutoff:
                        tmp.append(r)
                except Exception:
                    tmp.append(r)
            in_rows = tmp
        else:
            in_rows = batch

        # 2) in-memory global de-dup by ID and TEXT
        for r in in_rows:
            ak = r.get("app_key") or compute_app_key(r.get("store"), r.get("app_id"))
            ctry = r.get("country","")
            rid = (r.get("review_id") or "").strip()
            title = r.get("title","")
            body  = r.get("body","")
            thash = _text_hash(ak, title, body)

            key_id = (ak, rid) if dedupe_scope == "global" else (ak, ctry, rid)
            key_tx = (ak, thash) if dedupe_scope == "global" else (ak, ctry, thash)

            # already have same review id? skip
            if rid and key_id in seen_by_id:
                continue

            # if we already have a text-equivalent, keep the "better" one
            existed = best_by_text.get(key_tx)
            cur = {
                "app_key": ak, "store": r.get("store"), "app_id": r.get("app_id"),
                "country": ctry, "lang": r.get("lang"),
                "review_id": rid, "user_name": r.get("user_name"),
                "rating": r.get("rating"),
                "title": title, "body": body,
                "version": r.get("version"), "at": r.get("at"),
                "special_reviews": r.get("special_reviews", False),
            }
            chosen = _pick_better(existed, cur)
            best_by_text[key_tx] = chosen
            if rid:
                seen_by_id.add(key_id)

        # 3) write the current best snapshot
        out_rows = list(best_by_text.values())
        append_rows_to_csv(out, out_rows, header_if_new=True)
        # reset batch; keep best_by_text as state (so we don't re-write the same rows next batch)
        batch.clear()

    pbar_items = []
    for s in stores:
        if s == "play":
            pbar_items += [("PlayStore", it["id"], it.get("title", "")) for it in by_store["PlayStore"]]
        elif s == "ios":
            pbar_items += [("AppStore", it["id"], it.get("title", "")) for it in by_store["AppStore"]]
        elif s == "cws":
            pbar_items += [("ChromeWS", it["id"], it.get("title", "")) for it in by_store["ChromeWS"]]

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
                    continue

    maybe_flush()
    print(f"[DONE] appended reviews -> {out}")

def main():
    ap = argparse.ArgumentParser(description="Review scraper pipeline (global de-dup by ID+TEXT)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_all = sub.add_parser("all", help="Fetch reviews (Play + iOS) and append directly to CSV")
    p_all.add_argument("--in", dest="inp", default="data/curated/apps_clean.csv",
                       help="apps_clean.csv or full_dump.jsonl")
    p_all.add_argument("--out-csv", default="data/curated/reviews.csv")
    p_all.add_argument("--stores", default="play,ios", help="comma list: play,ios,cws (CWS not implemented)")
    p_all.add_argument("--countries", default="au,us,gb", help="comma list of country codes")
    p_all.add_argument("--langs", default="en", help="comma list of languages")
    p_all.add_argument("--max-per-app", type=int, default=300)
    p_all.add_argument("--since-days", type=int, default=365)
    p_all.add_argument("--flush-every", type=int, default=200)
    p_all.add_argument("--dedupe-scope", choices=["country","global"], default="global",
                       help="global (default) removes cross-country duplicates by text")
    p_all.add_argument("--overwrite", action="store_true",
                       help="delete output CSV before writing (fresh run)")

    args = ap.parse_args()
    if args.cmd == "all":
        stores = [s.strip() for s in args.stores.split(",") if s.strip()]
        countries = [c.strip() for c in args.countries.split(",") if c.strip()]
        langs = [l.strip() for l in args.langs.split(",") if l.strip()]
        run_reviews_to_csv(
            args.inp, args.out_csv, stores, countries, langs,
            args.max_per_app, args.since_days, args.flush_every,
            dedupe_scope=args.dedupe_scope, overwrite=args.overwrite
        )

if __name__ == "__main__":
    main()
