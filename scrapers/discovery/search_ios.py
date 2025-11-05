# scrapers/discovery/search_ios.py
import httpx
from urllib.parse import quote

def discover_ios_by_keywords(keywords, limit_per_kw=25, country="us"):
    ids = []
    with httpx.Client(timeout=30) as c:
        for kw in keywords:
            url = f"https://itunes.apple.com/search?term={quote(kw)}&entity=software&country={country}&limit={limit_per_kw}"
            r = c.get(url)
            r.raise_for_status()
            data = r.json()
            for item in data.get("results", []):
                track_id = item.get("trackId")
                if track_id:
                    ids.append(f"id{track_id}")
    return ids
