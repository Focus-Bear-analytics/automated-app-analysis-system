# etl/scrape_websites.py
import argparse
import time
import re
from urllib.parse import urlparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

# Text extraction
try:
    import trafilatura  # optional (pip install trafilatura)
except Exception:
    trafilatura = None

import httpx
from bs4 import BeautifulSoup

# Optional Playwright fallback (uses your existing helper)
try:
    from scrapers.browser import chromium_page, run  # already in repo
    _HAVE_PLAYWRIGHT = True
except Exception:
    chromium_page = None
    run = None
    _HAVE_PLAYWRIGHT = False

# -------- HTTP defaults --------
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/127.0.0.0 Safari/537.36"
)
HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
}
TIMEOUT = 30
MAX_CHARS = 100_000

_SKIP_SCHEMES = re.compile(r"^(mailto:|tel:|sms:|market:|intent:|itms)", re.I)


# -------- helpers: parsing --------
def simple_extract(html: str) -> str:
    """Fallback extraction: strip script/style, collapse whitespace."""
    try:
        soup = BeautifulSoup(html or "", "lxml")
    except Exception:
        soup = BeautifulSoup(html or "", "html.parser")
    for bad in soup(["script", "style", "noscript", "svg", "iframe"]):
        try:
            bad.decompose()
        except Exception:
            pass
    text = soup.get_text(separator="\n")
    text = re.sub(r"\n{2,}", "\n\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text.strip()


def _title_from_html(html: str) -> Optional[str]:
    try:
        try:
            soup = BeautifulSoup(html or "", "lxml")
        except Exception:
            soup = BeautifulSoup(html or "", "html.parser")
        t = soup.title.string if soup.title else None
        return (t or "").strip() or None
    except Exception:
        return None


def _extract_from_html(html: str) -> str:
    if trafilatura:
        try:
            out = trafilatura.extract(html, include_comments=False, include_tables=False) or ""
        except Exception:
            out = ""
        if out:
            return out
    return simple_extract(html)


def _normalize_and_validate(u: str) -> str:
    """
    Returns a valid http(s) URL or "" to skip.
    - Skips non-web schemes (mailto:, tel:, market:, itms-*, etc.)
    - Adds https:// if scheme missing
    - Ensures netloc (domain) exists
    """
    if not isinstance(u, str):
        return ""
    u = u.strip()
    if not u or _SKIP_SCHEMES.match(u):
        return ""
    if not re.match(r"^https?://", u, re.I):
        u = "https://" + u
    pr = urlparse(u)
    if not pr.netloc or "." not in pr.netloc:
        return ""
    return u


def _trim(txt: str) -> str:
    txt = re.sub(r"\s+\n", "\n", txt or "").strip()
    return txt[:MAX_CHARS] if len(txt) > MAX_CHARS else txt


# -------- HTTP fetch (with retries) --------
def fetch_http(url: str, attempts: int = 3) -> Tuple[Optional[int], str, Optional[str], Optional[str]]:
    """
    Return (status_code, html, final_url, page_title). On failure: (None, "", None, None)
    """
    if not url:
        return None, "", None, None
    last_status, last_final, html, page_title = None, None, "", None
    for attempt in range(attempts):
        try:
            with httpx.Client(timeout=TIMEOUT, headers=HEADERS, follow_redirects=True, http2=True) as c:
                r = c.get(url)
            last_status = r.status_code
            last_final = str(r.url)
            ctype = (r.headers.get("content-type") or "").lower()
            if last_status == 200 and "html" in ctype and r.text:
                html = r.text
                page_title = _title_from_html(html)
                break
        except Exception:
            pass
        time.sleep(0.8 * (attempt + 1))
    return last_status, html, last_final, page_title


# -------- Playwright render --------
async def _render_and_extract_async(url: str) -> Tuple[str, Optional[str]]:
    """Render with Playwright, return (html, page_title)."""
    async with chromium_page(headless=True) as page:
        await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        for _ in range(2):
            await page.mouse.wheel(0, 1200)
            await page.wait_for_timeout(200)
        html = await page.content()
        title = await page.title()
        return html or "", (title or "").strip() or None


def render_and_extract(url: str) -> Tuple[str, Optional[str]]:
    if not _HAVE_PLAYWRIGHT:
        return "", None
    try:
        return run(_render_and_extract_async(url))
    except Exception:
        return "", None


# -------- Orchestrator (JS-first when enabled) --------
def fetch_text(
    url: str,
    use_js_fallback: bool,
    min_len_for_js: int = 500,
) -> Tuple[Optional[int], str, Optional[str], Optional[str]]:
    """
    Returns (status_code, extracted_text, final_url, page_title).
    If JS fallback is requested, try Playwright FIRST (most reliable), then HTTP.
    """
    if not url:
        return None, "", None, None

    # 1) JS render first (if requested & available)
    if use_js_fallback and _HAVE_PLAYWRIGHT:
        r_html, r_title = render_and_extract(url)
        if r_html:
            extracted = _extract_from_html(r_html)
            if len(extracted) >= min_len_for_js:
                return 200, _trim(extracted), url, r_title

    # 2) Plain HTTP
    status, html, final_url, page_title = fetch_http(url)
    extracted = _extract_from_html(html) if html else ""

    # 3) If HTTP is weak and JS allowed, try render now
    if use_js_fallback and _HAVE_PLAYWRIGHT and len(extracted) < min_len_for_js:
        r_html, r_title = render_and_extract(final_url or url)
        if r_html and len(r_html) > len(html):
            extracted = _extract_from_html(r_html)
            page_title = r_title or page_title
            return 200, _trim(extracted), (final_url or url), page_title

    return status, _trim(extracted), final_url, page_title


# -------- main CLI --------
def main(
    apps_csv: str,
    out_csv: str,
    max_sites: Optional[int],
    sleep_sec: float,
    resume: bool,
    js_fallback: bool,
    js_min_len: int
):
    apps = pd.read_csv(apps_csv)

    need = {"app_key", "website_url"}
    if not need.issubset(apps.columns):
        raise SystemExit("apps.csv must contain app_key and website_url")

    # carry-through columns (if present)
    carry_cols = [c for c in ("store", "id", "title") if c in apps.columns]

    # Optional resume support
    done = set()
    out_path = Path(out_csv)
    if resume and out_path.exists():
        try:
            prev = pd.read_csv(out_path, usecols=["app_key"])
            done = set(prev["app_key"].dropna().astype(str))
        except Exception:
            done = set()

    rows = []
    count = 0
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for t in apps.itertuples(index=False):
        app_key = getattr(t, "app_key")
        url_raw = getattr(t, "website_url", None)

        if resume and app_key in done:
            continue

        url_norm = _normalize_and_validate(url_raw)
        if not url_norm:
            print(f"[skip] {app_key} invalid-or-nonweb url: {url_raw}")
            continue

        status, text, final_url, page_title = fetch_text(
            url_norm,
            use_js_fallback=js_fallback,
            min_len_for_js=js_min_len
        )

        base = {
            "app_key": app_key,
            "website_url": url_raw,
            "final_url": final_url,
            "website_status": status,
            "content_len": len(text),
            "website_text": text,
            "page_title": page_title,
            "scraped_at": ts,
        }
        for c in carry_cols:
            base[c] = getattr(t, c, None)

        rows.append(base)
        count += 1
        print(f"[{count}] {app_key} -> url={url_norm} status={status} len={len(text)}")

        if max_sites and count >= max_sites:
            break
        time.sleep(sleep_sec)  # be polite

    # Write/append with stable column order
    out_path.parent.mkdir(parents=True, exist_ok=True)
    cols = ["app_key"] + carry_cols + [
        "website_url", "final_url", "website_status", "content_len", "website_text", "page_title", "scraped_at"
    ]
    df = pd.DataFrame(rows, columns=cols)
    if resume and out_path.exists():
        df.to_csv(out_path, mode="a", index=False, header=False)
    else:
        df.to_csv(out_path, index=False)

    print(f"[web-scrape] wrote {len(df)} rows -> {out_path}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apps", default="data/curated/apps.csv", help="Input apps CSV (must have app_key, website_url)")
    ap.add_argument("--out", default="data/curated/websites.csv", help="Output CSV for website text")
    ap.add_argument("--max", type=int, default=None, help="Limit number of sites (for testing)")
    ap.add_argument("--sleep", type=float, default=1.0, help="Delay between requests (seconds)")
    ap.add_argument("--resume", action="store_true", help="Append & skip already-scraped app_keys")
    ap.add_argument("--js-fallback", action="store_true", help="Render with Playwright if HTTP text is tiny or blocked")
    ap.add_argument("--js-min-len", type=int, default=500, help="Threshold chars to trigger JS fallback when enabled")
    args = ap.parse_args()
    main(args.apps, args.out, args.max, args.sleep, args.resume, args.js_fallback, args.js_min_len)
