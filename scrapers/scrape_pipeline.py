# scrapers/scrape_pipeline.py
import argparse, json, subprocess, yaml, sys
from pathlib import Path
import pandas as pd

from scrapers.store_play import scrape_play_details
from scrapers.store_cws import scrape_cws_details
from scrapers.store_ios import scrape_ios_details
from scrapers.common import setup_logger

# Discovery modules
from scrapers.discovery.search_play import discover_play_by_keywords, discover_play_similar
from scrapers.discovery.search_cws import discover_cws_by_keywords
from scrapers.discovery.search_ios import discover_ios_by_keywords
from scrapers.discovery.filtering import keep_title_or_desc

DATA_DIR = Path("data")
INPUT_DIR = DATA_DIR / "input"
CURATED_DIR = DATA_DIR / "curated"
SEEDS = Path(__file__).parent / "seeds.yml"
DISCOVERY_CFG = Path(__file__).parent / "discovery.yml"
logger = setup_logger("orchestrator")


# ---------- helpers ----------
def write_jsonl(rows, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def load_curated_apps() -> pd.DataFrame:
    p = CURATED_DIR / "apps.csv"
    return pd.read_csv(p) if p.exists() else pd.DataFrame(
        columns=["app_key", "store", "title", "last_update"]
    )


def _load_discovery_cfg():
    if DISCOVERY_CFG.exists():
        try:
            return yaml.safe_load(DISCOVERY_CFG.read_text())
        except Exception:
            logger.warning("discovery.yml exists but could not be parsed; using defaults")
    return {}


def discover_catalog_from_seeds() -> list[dict]:
    if not SEEDS.exists():
        return []
    cfg = yaml.safe_load(SEEDS.read_text())
    items = []
    for pid in cfg.get("play_ids", []):
        items.append({"store": "PlayStore", "id": pid})
    for eid in cfg.get("cws_ids", []):
        items.append({"store": "ChromeWS", "id": eid})
    for iid in cfg.get("ios_ids", []):
        items.append({"store": "AppStore", "id": iid})
    return items


# ---------- scraping ----------
def scrape_details(items: list[dict]) -> list[dict]:
    rows = []
    total = len(items)
    for i, it in enumerate(items, start=1):
        logger.info(f"Scraping {i}/{total}: {it['store']} {it['id']}")
        try:
            if it["store"] == "PlayStore":
                rows.append(scrape_play_details(it["id"]))
            elif it["store"] == "ChromeWS":
                rows.append(scrape_cws_details(it["id"]))
            else:
                rows.append(scrape_ios_details(it["id"]))
        except Exception as e:
            logger.error(f"Failed {it}: {e}")
    return rows


# ---------- discovery ----------
def cmd_discover(out_candidates="data/input/candidates.jsonl"):
    cfg = _load_discovery_cfg()
    kw = cfg.get("keywords", [])
    limits = cfg.get("limits", {})
    per_kw = int(limits.get("per_keyword_per_store", 25))
    similar_per = int(limits.get("similar_per_app", 12))
    max_total = int(limits.get("max_total_candidates", 300))

    # 1) keyword discovery per store
    play_ids = discover_play_by_keywords(kw, per_kw)
    cws_ids = discover_cws_by_keywords(kw, per_kw)
    ios_ids = discover_ios_by_keywords(kw, per_kw)

    # 2) expand similar for play
    play_similar = discover_play_similar(list(play_ids)[:20], similar_per)

    # 3) merge + dedupe
    seen = set()
    items = []

    def add(store, sid):
        key = (store, sid)
        if key in seen:
            return
        seen.add(key)
        items.append({"store": store, "id": sid})

    for sid in play_ids:
        add("PlayStore", sid)
    for sid in play_similar:
        add("PlayStore", sid)
    for sid in cws_ids:
        add("ChromeWS", sid)
    for sid in ios_ids:
        add("AppStore", sid)

    # soft cap
    if len(items) > max_total:
        items = items[:max_total]

    write_jsonl(items, Path(out_candidates))
    logger.info(f"discover -> wrote {len(items)} candidates at {out_candidates}")
    return items


# ---------- subprocess wrappers ----------
def _run_normalize(in_jsonl: str, out_csv: str, include_terms: list[str], exclude_terms: list[str]):
    logger.info(f"normalize -> {out_csv}")
    inc_arg = ",".join(include_terms or [])
    exc_arg = ",".join(exclude_terms or [])
    cmd = [
        sys.executable, "etl/normalize_apps.py",
        "--in", in_jsonl,
        "--out", out_csv,
    ]
    if inc_arg:
        cmd += ["--include-terms", inc_arg]
    if exc_arg:
        cmd += ["--exclude-terms", exc_arg]
    subprocess.run(cmd, check=True)


def _run_clean(in_csv: str, out_clean: str, out_dropped: str,
               min_rating_count: int, min_play_installs: int,
               min_cws_users: int, min_relevance: float):
    logger.info(f"clean -> {out_clean}")
    subprocess.run(
        [
            sys.executable, "-m", "etl.clean_apps",
            "--in", in_csv,
            "--out", out_clean,
            "--out-dropped", out_dropped,
            "--min-rating-count", str(min_rating_count),
            "--min-play-installs", str(min_play_installs),
            "--min-cws-users", str(min_cws_users),
            "--min-relevance", str(min_relevance),
        ],
        check=True,
    )


def _run_websites(apps_csv: str, out_csv: str,
                  max_sites: int | None, sleep: float,
                  resume: bool, js_fallback: bool, js_min_len: int | None):
    logger.info(f"websites -> {out_csv} (apps={apps_csv})")
    cmd = [
        sys.executable, "-m", "etl.scrape_websites",
        "--apps", apps_csv,
        "--out", out_csv,
        "--sleep", str(sleep),
    ]
    if max_sites is not None:
        cmd += ["--max", str(max_sites)]
    if resume:
        cmd += ["--resume"]
    if js_fallback:
        cmd += ["--js-fallback"]
    if js_min_len is not None:
        cmd += ["--js-min-len", str(js_min_len)]
    subprocess.run(cmd, check=True)


# ---------- full run ----------
def cmd_full(out: str = "data/input/full_dump.jsonl",
             use_discovery: bool = False,
             no_filter: bool = False,
             do_clean: bool = True,
             # cleaner thresholds
             min_rating_count: int = None,
             min_play_installs: int = None,
             min_cws_users: int = None,
             min_relevance: float = None,
             # website scraping
             do_websites: bool = False,
             web_out: str = str(CURATED_DIR / "websites.csv"),
             web_max: int | None = None,
             web_sleep: float = 1.0,
             web_resume: bool = True,
             web_js_fallback: bool = True,
             web_js_min_len: int | None = None):
    cfg = _load_discovery_cfg()
    filters = cfg.get("filters", {})
    include_terms = filters.get("include_terms", [])
    exclude_terms = filters.get("exclude_terms", [])

    # cleaner defaults (YAML -> CLI override)
    min_rating_count = int(min_rating_count if min_rating_count is not None else filters.get("min_rating_count", 10))
    min_play_installs = int(min_play_installs if min_play_installs is not None else filters.get("min_play_installs", 50_000))
    min_cws_users = int(min_cws_users if min_cws_users is not None else filters.get("min_cws_users", 10_000))
    min_relevance = float(min_relevance if min_relevance is not None else filters.get("min_relevance", 0.15))

    logger.info("FULL: discover from seeds…")
    seeds = discover_catalog_from_seeds()
    items = seeds

    if use_discovery:
        logger.info("running discovery.yml …")
        discovered = cmd_discover(out_candidates=str(INPUT_DIR / "candidates.jsonl"))
        existing = {(i["store"], i["id"]) for i in items}
        for d in discovered:
            if (d["store"], d["id"]) not in existing:
                items.append(d)

    logger.info(f"total candidates={len(items)}; scraping…")
    rows = scrape_details(items)

    # Optional pre-filter before normalize
    if not no_filter and (include_terms or exclude_terms):
        logger.info("pre-filtering scraped rows using filters.include_terms/exclude_terms from discovery.yml …")
        filtered = []
        for r in rows:
            if keep_title_or_desc(r.get("title"), r.get("description"), include_terms, exclude_terms):
                filtered.append(r)
        if filtered:
            rows = filtered
            logger.info(f"pre-filter kept {len(rows)} rows")

    # Write raw dump (JSONL)
    write_jsonl(rows, Path(out))

    # Normalize -> apps_all.csv (relevance v2 inside)
    apps_all_csv = str(CURATED_DIR / "apps_all.csv")
    _run_normalize(out, apps_all_csv, include_terms, exclude_terms)

    # Clean -> apps_clean.csv + apps_dropped.csv
    apps_clean_csv = str(CURATED_DIR / "apps_clean.csv")
    apps_dropped_csv = str(CURATED_DIR / "apps_dropped.csv")
    if do_clean:
        _run_clean(
            in_csv=apps_all_csv,
            out_clean=apps_clean_csv,
            out_dropped=apps_dropped_csv,
            min_rating_count=min_rating_count,
            min_play_installs=min_play_installs,
            min_cws_users=min_cws_users,
            min_relevance=min_relevance,
        )

    # Websites (cleaned list preferred)
    if do_websites:
        apps_csv_for_web = apps_clean_csv if do_clean and Path(apps_clean_csv).exists() else apps_all_csv
        if apps_csv_for_web == apps_all_csv:
            logger.warning("apps_clean.csv not available (no-clean run?) -> scraping websites from apps_all.csv")
        _run_websites(
            apps_csv=apps_csv_for_web,
            out_csv=web_out,
            max_sites=web_max,
            sleep=web_sleep,
            resume=web_resume,
            js_fallback=web_js_fallback,
            js_min_len=web_js_min_len,
        )

    logger.info("FULL done")


# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(description="Scrape orchestrator")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # full
    p_full = sub.add_parser("full", help="Full scrape → normalize → clean (→ websites optional)")
    p_full.add_argument("--out", default=str(INPUT_DIR / "full_dump.jsonl"))
    p_full.add_argument("--discover", action="store_true", help="Enable keyword/related discovery")
    p_full.add_argument("--no-filter", action="store_true", help="Skip pre-normalize title/desc filter")
    p_full.add_argument("--no-clean", action="store_true", help="Skip cleaner step (only write apps_all.csv)")
    # cleaner thresholds
    p_full.add_argument("--min-rating-count", type=int, default=None)
    p_full.add_argument("--min-play-installs", type=int, default=None)
    p_full.add_argument("--min-cws-users", type=int, default=None)
    p_full.add_argument("--min-relevance", type=float, default=None)
    # website scraping
    p_full.add_argument("--websites", action="store_true", help="Scrape websites after cleaning (uses apps_clean.csv)")
    p_full.add_argument("--web-out", default=str(CURATED_DIR / "websites.csv"))
    p_full.add_argument("--web-max", type=int, default=None)
    p_full.add_argument("--web-sleep", type=float, default=1.0)
    p_full.add_argument("--web-no-resume", action="store_true", help="Do not resume (default is resume)")
    p_full.add_argument("--web-no-js-fallback", action="store_true", help="Disable JS rendering fallback")
    p_full.add_argument("--web-js-min-len", type=int, default=None)

    # delta (placeholder)
    sub.add_parser("delta", help="Delta update (TODO)")

    # discover
    p_disc = sub.add_parser("discover", help="Discovery only (write candidates)")
    p_disc.add_argument("--out", default=str(INPUT_DIR / "candidates.jsonl"))

    args = parser.parse_args()
    if args.cmd == "full":
        cmd_full(
            out=args.out,
            use_discovery=args.discover,
            no_filter=args.no_filter,
            do_clean=not args.no_clean,
            # thresholds
            min_rating_count=args.min_rating_count,
            min_play_installs=args.min_play_installs,
            min_cws_users=args.min_cws_users,
            min_relevance=args.min_relevance,
            # websites
            do_websites=args.websites,
            web_out=args.web_out,
            web_max=args.web_max,
            web_sleep=args.web_sleep,
            web_resume=not args.web_no_resume,
            web_js_fallback=not args.web_no_js_fallback,
            web_js_min_len=args.web_js_min_len,
        )
    elif args.cmd == "discover":
        cmd_discover(args.out)
    else:
        print("delta mode not implemented yet; run 'full' or 'discover'.")


if __name__ == "__main__":
    main()
