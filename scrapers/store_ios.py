# scrapers/store_ios.py
import httpx
import datetime as dt
import re

LOOKUP = "https://itunes.apple.com/lookup"
_PRICE_RX = re.compile(r'[$£€]\s?(\d+(?:\.\d{2})?)')

def _norm_date(s):
    if not s:
        return None
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "")).date().isoformat()
    except Exception:
        return None

def _extract_ios_iap_range(store_url: str) -> dict:
    """
    Best-effort scrape of the App Store preview page to summarize In-App Purchases.
    Returns: {'currency': str|None, 'iap_count': int, 'iap_min': float|None, 'iap_max': float|None}
    """
    if not store_url:
        return {}

    headers = {
        # A simple UA helps Apple serve the server-rendered preview
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.8",
    }
    try:
        with httpx.Client(timeout=25.0, follow_redirects=True, headers=headers) as c:
            html = c.get(store_url).text
    except Exception:
        return {}

    low = html.lower()
    start = low.find("in-app purchases")
    if start == -1:
        # No visible IAP section
        return {"iap_count": 0, "iap_min": None, "iap_max": None}

    # Heuristic: take up to the next H2 block or end of document
    end = low.find("<h2", start + 10)
    chunk = html[start:end] if end != -1 else html[start:]

    prices = []
    for m in _PRICE_RX.finditer(chunk):
        try:
            prices.append(float(m.group(1)))
        except Exception:
            pass

    if prices:
        return {
            "iap_count": len(prices),
            "iap_min": float(min(prices)),
            "iap_max": float(max(prices)),
        }
    else:
        return {"iap_count": 0, "iap_min": None, "iap_max": None}

def scrape_ios_details(app_id: str, country: str = "us") -> dict:
    """
    Scrape iOS app metadata via iTunes Lookup + enrich with IAP summary from the App Store page.
    Returns a dict with normalized fields used by the rest of the pipeline.
    """
    # app_id comes like "id1146560473" → strip "id"
    numeric = app_id[2:] if app_id.startswith("id") else app_id
    params = {"id": numeric, "country": country}

    with httpx.Client(timeout=30.0) as c:
        r = c.get(LOOKUP, params=params)
        r.raise_for_status()
        js = r.json()

    results = js.get("results", [])
    if not results:
        raise RuntimeError(f"iOS lookup returned no results for {app_id}")

    x = results[0]

    # Base fields
    title = x.get("trackName")
    developer = x.get("sellerName") or x.get("artistName")
    category = x.get("primaryGenreName")
    rating_avg = x.get("averageUserRating") or x.get("averageUserRatingForCurrentVersion")
    rating_count = x.get("userRatingCount") or x.get("userRatingCountForCurrentVersion")
    store_url = x.get("trackViewUrl")
    website_url = x.get("sellerUrl")
    description = x.get("description")
    release_date = _norm_date(x.get("releaseDate"))
    last_update = _norm_date(x.get("currentVersionReleaseDate"))
    version = x.get("version")
    icon_url = x.get("artworkUrl512") or x.get("artworkUrl100")
    currency = x.get("currency")  # e.g., USD, AUD

    # Price (numeric) from API; pricing_raw similar to Play style for transparency
    raw_price = x.get("price")  # float like 0.0, 3.99, etc.
    if raw_price is None:
        price_app = None
        pricing_raw = "Free"
    else:
        price_app = float(raw_price)
        pricing_raw = "Free" if price_app == 0.0 else f"${price_app:.2f}"

    details = {
        "store": "AppStore",
        "id": app_id,
        "app_key": f"ios:{app_id}",
        "title": title,
        "developer": developer,
        "category": category,
        "rating_avg": rating_avg,
        "rating_count": rating_count,
        "rating_histogram": None,   # iTunes lookup doesn’t provide histogram
        "installs_or_users": None,  # N/A for iOS
        "pricing_raw": pricing_raw,
        "price_app": price_app,     # NEW (numeric)
        "currency": currency,       # NEW (ISO code)
        "website_url": website_url,
        "store_url": store_url,
        "description": description,
        "release_date": release_date,
        "last_update": last_update,
        "version": version,
        "icon_url": icon_url,
        "scraped_at": dt.date.today().isoformat(),
    }

    # Enrich with IAP min/max/count (best-effort)
    iap = _extract_ios_iap_range(store_url)
    if iap:
        details.update(iap)

    return details
