# scrapers/discovery/search_play.py
import re, asyncio
from typing import List, Set
from scrapers.browser import chromium_page, run

SEARCH_URL = "https://play.google.com/store/search?q={kw}&c=apps&hl=en&gl=US"

async def _search_play_async(keywords: List[str], per_kw: int = 25) -> Set[str]:
    found: Set[str] = set()
    async with chromium_page(headless=True) as page:
        for kw in keywords:
            url = SEARCH_URL.format(kw=kw)
            await page.goto(url, wait_until="domcontentloaded", timeout=45_000)
            # App cards link to /store/apps/details?id=PACKAGE
            links = page.locator('a[href*="/store/apps/details?id="]')
            count = await links.count()
            for i in range(min(count, per_kw)):
                href = await links.nth(i).get_attribute("href")
                if not href: continue
                m = re.search(r"id=([a-zA-Z0-9._]+)", href)
                if m: found.add(m.group(1))
    return found

async def _similar_from_details_async(package_ids: List[str], limit_each: int = 12) -> Set[str]:
    found: Set[str] = set()
    async with chromium_page(headless=True) as page:
        for pid in package_ids:
            url = f"https://play.google.com/store/apps/details?id={pid}&hl=en&gl=US"
            await page.goto(url, wait_until="domcontentloaded", timeout=45_000)
            # “Similar apps” carousels often reuse same link pattern
            links = page.locator('a[href*="/store/apps/details?id="]')
            count = await links.count()
            take = 0
            for i in range(count):
                if take >= limit_each: break
                href = await links.nth(i).get_attribute("href")
                if not href: continue
                m = re.search(r"id=([a-zA-Z0-9._]+)", href)
                if m:
                    pkg = m.group(1)
                    if pkg != pid:
                        found.add(pkg)
                        take += 1
    return found

def discover_play_by_keywords(keywords, limit_per_kw=25) -> list[str]:
    return sorted(list(run(_search_play_async(keywords, per_kw=limit_per_kw))))

def discover_play_similar(seed_ids: list[str], similar_per_app=12) -> list[str]:
    if not seed_ids: return []
    return sorted(list(run(_similar_from_details_async(seed_ids, limit_each=similar_per_app))))
