# scrapers/discovery/search_cws.py
import re
from typing import List, Set
from scrapers.browser import chromium_page, run

SEARCH_URL = "https://chromewebstore.google.com/search/{kw}?hl=en"

async def _search_cws_async(keywords: List[str], per_kw: int = 25) -> Set[str]:
    found: Set[str] = set()
    for kw in keywords:
        url = SEARCH_URL.format(kw=kw)
        async with chromium_page(headless=True) as page:
            await page.goto(url, wait_until="domcontentloaded", timeout=60_000)

            # scroll to trigger lazy-load
            for _ in range(5):
                await page.mouse.wheel(0, 2000)
                await page.wait_for_timeout(300)

            # CWS uses ABSOLUTE links; match any link containing "/detail/"
            links = page.locator('a[href*="/detail/"]')
            count = await links.count()

            taken = 0
            for i in range(count):
                if taken >= per_kw:
                    break
                href = await links.nth(i).get_attribute("href")
                if not href:
                    continue
                # extract the 32-char extension id after /detail/
                m = re.search(r"/detail/([a-p0-9]{32})", href)
                if not m:
                    # fallback: last path segment often is the id
                    m = re.search(r"/detail/[^/]+/([a-p0-9]{32})", href)
                if m:
                    ext_id = m.group(1)
                    if ext_id not in found:
                        found.add(ext_id)
                        taken += 1
    return found

def discover_cws_by_keywords(keywords, limit_per_kw=25) -> list[str]:
    return sorted(list(run(_search_cws_async(keywords, per_kw=limit_per_kw))))
