# scrapers/store_cws.py
import json, re, datetime as dt
from typing import Optional
from scrapers.browser import chromium_page, run


# ==============================
# Helpers
# ==============================

def _to_int_compact(text: Optional[str]) -> Optional[int]:
    """Turn strings like '8.4K', '600,000', '1.2M' into ints."""
    if not text:
        return None
    t = text.strip().replace(",", "")
    m = re.fullmatch(r"(\d+(?:\.\d+)?)([KkMm])", t)
    if m:
        n = float(m.group(1))
        return int(round(n * (1_000 if m.group(2).lower() == "k" else 1_000_000)))
    m2 = re.search(r"(\d[\d,]*)", text)
    if m2:
        try:
            return int(m2.group(1).replace(",", ""))
        except Exception:
            return None
    return None


def _parse_users(text: Optional[str]) -> Optional[int]:
    """Extract '600,000 users' → 600000."""
    if not text:
        return None
    m = re.search(r"([\d,\.KkMm]+)\s+users\b", text, re.I)
    return _to_int_compact(m.group(1)) if m else None


def _clean_developer(txt: Optional[str]) -> Optional[str]:
    """Keep only the developer name (drop addresses / EU 'Trader...' disclosure)."""
    if not txt:
        return None
    t = " ".join(txt.split())
    # Break on common separators and keep first chunk
    t = re.split(r"\s{2,}|\n|\r|,|·", t)[0]
    # Remove trailing office numbers etc.
    t = re.sub(r"\s+\d{3,}.*$", "", t).strip()
    # Remove EU 'Trader ...' if glued
    t = re.sub(r"^Trader\b.*", "", t).strip() or t
    return t or None


def _norm_date(text: Optional[str]) -> Optional[str]:
    """Normalize 'September 17, 2025' → '2025-09-17' (leave as-is if parse fails)."""
    if not text:
        return None
    text = text.strip()
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d"):
        try:
            return dt.datetime.strptime(text, fmt).date().isoformat()
        except Exception:
            pass
    return text


async def _first_external_href(container) -> Optional[str]:
    """Return first non-Google external link inside container."""
    try:
        links = container.locator('a[href^="http"]')
        cnt = await links.count()
        for i in range(cnt):
            a = links.nth(i)
            href = await a.get_attribute("href")
            if not href:
                continue
            if not re.search(r"https?://(?:[\w.-]+\.)?(?:google|gstatic|googleusercontent)\.", href):
                return href
    except Exception:
        pass
    return None


async def _details_value_scoped(page, label_regex: str) -> Optional[str]:
    """
    Find a label/value *inside the Details section only*,
    walking to the nearest following sibling cell.
    """
    sec = page.locator("section").filter(
        has=page.get_by_role("heading", name=re.compile(r"^\s*Details\s*$", re.I))
    ).first
    if await sec.count() == 0:
        return None

    lab = sec.get_by_text(re.compile(label_regex, re.I), exact=True).first
    if await lab.count() == 0:
        return None

    for xp in (
        "xpath=../following-sibling::*[1]",
        "xpath=../../following-sibling::*[1]",
        "xpath=ancestor::*[1]/following-sibling::*[1]",
        "xpath=ancestor::*[2]/following-sibling::*[1]",
    ):
        try:
            el = lab.locator(xp)
            if await el.count() > 0:
                t = await el.text_content()
                if t:
                    return " ".join(t.split()).strip()
        except Exception:
            pass
    return None


async def _overview_text(page) -> Optional[str]:
    """
    Click 'See more' in Overview (if present) and return the full Overview text,
    trimmed before the next section like 'Details', 'Privacy', or 'Support'.
    """
    # Click 'See more' *within* the Overview section only
    try:
        overview_sec = page.locator("section").filter(
            has=page.get_by_role("heading", name=re.compile(r"^\s*Overview\s*$", re.I))
        ).first
        if await overview_sec.count() > 0:
            see_more = overview_sec.get_by_role("button", name=re.compile(r"^\s*See more\s*$", re.I)).first
            if await see_more.count() > 0:
                await see_more.click()
                await page.wait_for_timeout(200)
    except Exception:
        pass

    # Now read text within that Overview section
    try:
        overview_sec = page.locator("section").filter(
            has=page.get_by_role("heading", name=re.compile(r"^\s*Overview\s*$", re.I))
        ).first
        if await overview_sec.count() > 0:
            txt = await overview_sec.text_content()
            if txt:
                body = " ".join(txt.split())
                body = re.split(r"\bDetails\b|\bPrivacy\b|\bSupport\b", body, maxsplit=1)[0]
                # Drop any rating header line if it creeps in
                body = re.sub(r"^\s*\d+(?:\.\d+)?\s*out of 5.*?reviews?\.\s*", "", body, flags=re.I)
                return body.strip()
    except Exception:
        pass
    return None


def _from_jsonld_maybe(data, key, default=None):
    try:
        return data.get(key, default)
    except Exception:
        return default


async def _rating_bits(page):
    """
    Extract rating average AND rating count from the header just under the title.
    Works whether the star is a text glyph (★) or an SVG icon next to '4.5'.
    """
    rating_avg = rating_count = None

    # (A) aria-label like "4.5 out of 5"
    try:
        a11y = page.locator('[aria-label*="out of 5"], [aria-label*="out of five"]').first
        if await a11y.count() > 0:
            label = await a11y.get_attribute("aria-label") or ""
            m = re.search(r'([0-9]+(?:\.[0-9]+)?)\s*(?:out of|/)\s*5', label, re.I)
            if m:
                rating_avg = float(m.group(1))
    except Exception:
        pass

    # (B) Header cluster near "(8.4K ratings)"
    try:
        cluster = page.locator("div").filter(
            has=page.get_by_text(re.compile(r"\((?:[\d,\.KkMm]+)\s+ratings?\)", re.I))
        ).first
        if await cluster.count() > 0:
            txt = await cluster.text_content() or ""

            # rating count
            m_cnt = re.search(r"\(([\d,\.KkMm]+)\s+ratings?\)", txt, re.I)
            if m_cnt:
                rating_count = _to_int_compact(m_cnt.group(1))

            # rating avg — allow with or without a star glyph
            head = txt.split("(")[0]
            nums = re.findall(r"([0-9]+(?:\.[0-9]+)?)", head)
            for n in nums:
                try:
                    v = float(n)
                    if 0 < v <= 5.0:
                        rating_avg = rating_avg or v
                        break
                except Exception:
                    pass

            # explicit star glyph fallback
            if rating_avg is None:
                m_avg = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*★", txt)
                if m_avg:
                    rating_avg = float(m_avg.group(1))
    except Exception:
        pass

    # (C) Very last resort: a bare "4.5 ★" element or sibling of ratings-count node
    if rating_avg is None:
        try:
            node = page.get_by_text(re.compile(r"^\s*[0-9]+(?:\.[0-9]+)?\s*★\s*$")).first
            if await node.count() > 0:
                m = re.search(r"([0-9]+(?:\.[0-9]+)?)", await node.text_content() or "")
                if m:
                    rating_avg = float(m.group(1))
        except Exception:
            pass
        if rating_avg is None:
            try:
                cluster2 = page.get_by_text(re.compile(r"\bratings?\b", re.I)).first
                if await cluster2.count() > 0:
                    t2 = await cluster2.text_content() or ""
                    head2 = t2.split("(")[0]
                    m2 = re.search(r"([0-9]+(?:\.[0-9]+)?)", head2)
                    if m2:
                        v = float(m2.group(1))
                        if 0 < v <= 5:
                            rating_avg = v
            except Exception:
                pass

    return rating_avg, rating_count


# ==============================
# Main
# ==============================

async def _scrape_cws_async(ext_id: str) -> dict:
    url = f"https://chromewebstore.google.com/detail/{ext_id}?hl=en"
    async with chromium_page(headless=True) as page:
        await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        await page.mouse.wheel(0, 1800)  # trigger lazy blocks
        await page.wait_for_timeout(300)

        title = developer = category = None
        rating_avg = rating_count = None
        users = None
        price = "Free"
        website_url = None
        description = None
        version = last_update = None
        icon_url = None

        # ---------- JSON-LD (primary when present)
        try:
            scripts = page.locator('script[type="application/ld+json"]')
            for i in range(await scripts.count()):
                raw = await scripts.nth(i).text_content()
                if not raw:
                    continue
                data = json.loads(raw)
                # Sometimes it's a list of dicts
                if isinstance(data, list):
                    data = next(
                        (d for d in data if isinstance(d, dict) and d.get("@type") in {"Product", "SoftwareApplication"}),
                        data[0] if data else {}
                    )
                if isinstance(data, dict) and data.get("@type") in {"Product", "SoftwareApplication"}:
                    title = title or _from_jsonld_maybe(data, "name")
                    description = description or _from_jsonld_maybe(data, "description")
                    image = _from_jsonld_maybe(data, "image")
                    if isinstance(image, str):
                        icon_url = icon_url or image
                    agg = _from_jsonld_maybe(data, "aggregateRating", {})
                    try:
                        rating_avg = rating_avg or (float(agg.get("ratingValue")) if agg.get("ratingValue") else None)
                    except Exception:
                        pass
                    try:
                        rating_count = rating_count or (int(agg.get("ratingCount")) if agg.get("ratingCount") else None)
                    except Exception:
                        pass
        except Exception:
            pass

        # ---------- Title (fallback)
        if not title:
            try:
                h1 = page.get_by_role("heading").first
                if await h1.count() > 0:
                    title = (await h1.text_content() or "").strip()
            except Exception:
                pass

        # ---------- Category chip near the header (e.g., Workflow & Planning)
        try:
            chip_row = page.locator("div").filter(has=page.get_by_text(re.compile(r"\busers\b", re.I))).first
            chip = chip_row.get_by_text(re.compile(r"(Workflow\s*&\s*Planning|Productivity|Tools|Extension|Extensions)", re.I)).first
            if await chip.count() > 0:
                category = (await chip.text_content() or "").strip()
        except Exception:
            pass

        # ---------- Ratings (robust)
        if rating_avg is None or rating_count is None:
            ra, rc = await _rating_bits(page)
            rating_avg = rating_avg if rating_avg is not None else ra
            rating_count = rating_count if rating_count is not None else rc

        # ---------- Users ("600,000 users")
        if users is None:
            try:
                users_node = page.get_by_text(re.compile(r"\b[\d,\.KkMm]+\s+users\b", re.I)).first
                if await users_node.count() > 0:
                    users = _parse_users(await users_node.text_content() or "")
            except Exception:
                pass

        # ---------- Overview (expand & capture)
        if not description:
            description = await _overview_text(page)
        if not description:
            # Fallback to meta description
            try:
                meta = page.locator('meta[name="description"], meta[itemprop="description"]').first
                if await meta.count() > 0:
                    description = await meta.get_attribute("content")
            except Exception:
                pass

        # ---------- Details section (scoped)
        version_raw = await _details_value_scoped(page, r"^\s*Version\s*$")
        if version_raw:
            version = version_raw.strip()

        updated_raw = await _details_value_scoped(page, r"^\s*Updated\s*$")
        if updated_raw:
            last_update = _norm_date(updated_raw)

        # Developer cell (and Website link inside same cell)
        dev_cell_text = await _details_value_scoped(page, r"^\s*Developer\s*$")
        if dev_cell_text:
            developer = _clean_developer(dev_cell_text)
            try:
                det_sec = page.locator("section").filter(
                    has=page.get_by_role("heading", name=re.compile(r"^\s*Details\s*$", re.I))
                ).first
                dev_lab = det_sec.get_by_text(re.compile(r"^\s*Developer\s*$", re.I), exact=True).first
                # Find the *single* next sibling node (avoid union selectors)
                dev_cell = dev_lab.locator("xpath=../following-sibling::*[1]")
                if await dev_cell.count() == 0:
                    dev_cell = dev_lab.locator("xpath=../../following-sibling::*[1]")
                if await dev_cell.count() > 0:
                    # Prefer the link literally labeled "Website"
                    link = dev_cell.get_by_role("link", name=re.compile(r"Website", re.I)).first
                    if await link.count() > 0:
                        href = await link.get_attribute("href")
                        if href:
                            website_url = href
                    if not website_url:
                        website_url = await _first_external_href(dev_cell)
            except Exception:
                pass

        # ---------- Icon fallback
        if not icon_url:
            try:
                og = page.locator('meta[property="og:image"]').first
                if await og.count() > 0:
                    icon_url = await og.get_attribute("content")
            except Exception:
                pass
        if not icon_url:
            try:
                img = page.locator('img[src*="googleusercontent.com"], img[src*="gstatic"]').first
                if await img.count() > 0:
                    icon_url = await img.get_attribute("src")
            except Exception:
                pass

        return {
            "store": "ChromeWS",
            "id": ext_id,
            "app_key": f"cws:{ext_id}",
            "title": title,
            "developer": developer,
            "category": category,
            "rating_avg": rating_avg,
            "rating_count": rating_count,
            "rating_histogram": None,
            "installs_or_users": users,
            "pricing_raw": price,
            "website_url": website_url,
            "website_content_raw": None,
            "store_url": url,
            "description": description,
            "release_date": None,
            "last_update": last_update,
            "version": version,
            "icon_url": icon_url,
            "scraped_at": dt.date.today().isoformat(),
        }


def scrape_cws_details(ext_id: str) -> dict:
    return run(_scrape_cws_async(ext_id))
