# scrapers/store_play.py
import re
import datetime as dt
from scrapers.browser import chromium_page, run

# --------------------------- helpers ---------------------------

def _num_with_suffix(text: str | None):
    """
    Parse '742K', '10M+', '10,000,000+', '782K reviews', etc. -> int
    """
    if not text:
        return None
    t = text.replace(",", "").replace("+", "").strip()
    m = re.search(r"(\d+(?:\.\d+)?)([kKmM])?", t)
    if not m:
        return None
    n = float(m.group(1))
    suf = (m.group(2) or "").lower()
    if suf == "k":
        n *= 1_000
    elif suf == "m":
        n *= 1_000_000
    return int(round(n))

def _to_installs(text: str | None):
    # e.g. "10,000,000+ downloads" or "10M+ installs"
    if not text:
        return None
    return _num_with_suffix(text)

def _norm_date(text: str | None):
    if not text:
        return None
    t = " ".join(text.split())
    # "Sep 12, 2025"
    for fmt in ("%b %d, %Y", "%B %d, %Y"):
        try:
            return dt.datetime.strptime(t, fmt).date().isoformat()
        except Exception:
            pass
    return t  # fallback: keep original

async def _rating_bits(page):
    rating_avg = rating_count = None

    # average rating from aria-label
    try:
        star = page.locator('[aria-label*="Rated"]').first
        if await star.count() > 0:
            lab = await star.get_attribute("aria-label") or ""
            m = re.search(r"Rated\s+([0-9.]+)\s+stars", lab)
            if m:
                rating_avg = float(m.group(1))
    except Exception:
        pass

    # total reviews text near the stars
    try:
        node = page.get_by_text(re.compile(r"\b[\d,\.]+(?:[KkMm])?\s+reviews?\b")).first
        if await node.count() > 0:
            rating_count = _num_with_suffix(await node.text_content() or "")
    except Exception:
        pass

    return rating_avg, rating_count

async def _expand_description(page):
    """Scroll and click 'See more' to reveal the full long description."""
    try:
        about = page.get_by_text(re.compile(r"About this app", re.I)).first
        if await about.count() > 0:
            await about.scroll_into_view_if_needed()
            await page.wait_for_timeout(150)
    except Exception:
        pass

    for sel in [
        'button:has-text("See more")',
        'button:has-text("More")',
        '[aria-label*="See more"]',
        '[aria-label*="More"]',
    ]:
        try:
            btn = page.locator(sel).first
            if await btn.count() > 0:
                await btn.click()
                await page.wait_for_timeout(150)
                break
        except Exception:
            pass

async def _read_long_description(page):
    for sel in ('[data-g-id="description"]', '[itemprop="description"]'):
        try:
            node = page.locator(sel).first
            if await node.count() > 0:
                txt = await node.text_content()
                if txt:
                    return re.sub(r"^\s*About this app\s*arrow_forward\s*", "", txt).strip()
        except Exception:
            pass
    return None

async def _open_about_modal(page):
    """
    Click the arrow/popup button next to 'About this app' and return the dialog locator.
    """
    # try precise aria-label first
    for sel in (
        'button[aria-label="About this app"]',
        'button[aria-label*="About this app"]',
    ):
        try:
            b = page.locator(sel).first
            if await b.count() > 0:
                await b.click()
                break
        except Exception:
            pass
    else:
        # fallback: h2 sibling button
        try:
            h = page.locator('h2:has-text("About this app")').first
            if await h.count() > 0:
                await h.scroll_into_view_if_needed()
                btn = h.locator('xpath=following::button[1]')
                if await btn.count() > 0:
                    await btn.click()
        except Exception:
            pass

    modal = page.locator('div[role="dialog"], div[aria-modal="true"]').first
    try:
        await modal.wait_for(state="visible", timeout=5_000)
        return modal
    except Exception:
        return None

async def _modal_value(modal, label_texts):
    """
    Inside the About modal, find the value that is the *immediate sibling*
    of the element whose normalized text equals one of label_texts.
    """
    # exact-text match first (fast & precise)
    for lab in label_texts:
        try:
            el = modal.locator(f'xpath=.//*[normalize-space()="{lab}"]/following-sibling::*[1]').first
            if await el.count() > 0:
                t = await el.text_content()
                if t and t.strip():
                    return " ".join(t.split())
        except Exception:
            pass
    # relaxed contains (last resort)
    for lab in label_texts:
        try:
            el = modal.locator(f'xpath=.//*[contains(normalize-space(),"{lab}")]/following-sibling::*[1]').first
            if await el.count() > 0:
                t = await el.text_content()
                if t and t.strip():
                    return " ".join(t.split())
        except Exception:
            pass
    return None

async def _grab_icon(page):
    try:
        m = page.locator('meta[itemprop="image"]').first
        if await m.count() > 0:
            c = await m.get_attribute("content")
            if c:
                return c
    except Exception:
        pass
    try:
        m = page.locator('link[rel="image_src"]').first
        if await m.count() > 0:
            c = await m.get_attribute("href")
            if c:
                return c
    except Exception:
        pass
    try:
        img = page.locator('img[src*="play-lh.googleusercontent.com"]').first
        if await img.count() > 0:
            return await img.get_attribute("src")
        img = page.locator('img[srcset*="play-lh.googleusercontent.com"]').first
        if await img.count() > 0:
            ss = await img.get_attribute("srcset")
            if ss:
                return ss.split(",")[0].strip().split(" ")[0]
    except Exception:
        pass
    return None

# ---- developer website (Visit website in "Developer contact") ----

async def _expand_developer_contact(page):
    """
    Ensure the 'Developer contact' section is expanded so links are visible.
    """
    try:
        # direct expand button by aria-label
        btn = page.locator('button[aria-label*="Developer contact"]').first
        if await btn.count() > 0:
            await btn.scroll_into_view_if_needed()
            await page.wait_for_timeout(100)
            try:
                await btn.click()
                await page.wait_for_timeout(200)
            except Exception:
                pass
            return

        # fallback: scroll to the section and click the first toggle button nearby
        section = page.get_by_text(re.compile(r"Developer contact", re.I)).first
        if await section.count() > 0:
            await section.scroll_into_view_if_needed()
            await page.wait_for_timeout(150)
            # try a button inside / following the section
            for sel in ('xpath=ancestor::section//button', 'xpath=..//button', 'xpath=following::button[1]'):
                b = section.locator(sel).first
                if await b.count() > 0:
                    try:
                        await b.click()
                        await page.wait_for_timeout(200)
                        break
                    except Exception:
                        pass
    except Exception:
        pass

async def _grab_website_url(page):
    """
    Locate the 'Visit website' link in the Developer contact section.
    """
    # try without expanding (sometimes already open)
    for sel in (
        'a[aria-label*="Visit website"]',
        'a:has-text("Visit website")',
        'a[aria-label*="Website"]',
    ):
        try:
            a = page.locator(sel).first
            if await a.count() > 0:
                href = await a.get_attribute("href")
                if href:
                    return href
        except Exception:
            pass

    # expand and try again
    await _expand_developer_contact(page)
    for sel in (
        'a[aria-label*="Visit website"]',
        'a:has-text("Visit website")',
        'a[aria-label*="Website"]',
    ):
        try:
            a = page.locator(sel).first
            if await a.count() > 0:
                href = await a.get_attribute("href")
                if href:
                    return href
        except Exception:
            pass

    # last resort: any http(s) link inside the developer contact block
    try:
        block = page.get_by_text(re.compile(r"Developer contact", re.I)).first
        if await block.count() > 0:
            # search a link in siblings after revealing the section
            link = block.locator('xpath=following::a[starts-with(@href,"http")][1]').first
            if await link.count() > 0:
                href = await link.get_attribute("href")
                if href:
                    return href
    except Exception:
        pass

    return None

# --------------------------- main ---------------------------

async def _scrape_play_async(package_id: str) -> dict:
    url = f"https://play.google.com/store/apps/details?id={package_id}&hl=en&gl=US"
    async with chromium_page(headless=True) as page:
        await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        await page.wait_for_selector("h1", timeout=10_000)
        await page.mouse.wheel(0, 2400)
        await page.wait_for_timeout(200)

        # Title
        try:
            title = (await page.locator("h1").first.text_content() or "").strip()
        except Exception:
            title = None

        # Developer (top area; if missing we'll use 'Offered by' from modal)
        developer = None
        try:
            dev = page.get_by_text(re.compile(r"Offered by", re.I)).first
            if await dev.count() > 0:
                developer = (await dev.locator("xpath=../following-sibling::*[1]").text_content() or "").strip()
        except Exception:
            pass

        # Category — best effort; avoid top-site nav "Kids"
        category = None
        try:
            cat = page.locator('a[href*="/store/apps/category/"]').first
            if await cat.count() > 0:
                category = (await cat.text_content() or "").strip()
        except Exception:
            pass

        # Ratings
        rating_avg, rating_count = await _rating_bits(page)

        # Long description
        await _expand_description(page)
        description = await _read_long_description(page)

        # Open 'About this app' modal and read values
        requires_android = content_rating = pricing_raw = last_update = release_date = version = None
        installs_or_users = None
        offered_by = None

        modal = await _open_about_modal(page)
        if modal:
            version = await _modal_value(modal, ["Version"])
            last_update = await _modal_value(modal, ["Updated on", "Updated"])
            installs_txt = await _modal_value(modal, ["Downloads", "Installs"])
            requires_android = await _modal_value(modal, ["Requires Android"])
            content_rating = await _modal_value(modal, ["Content rating"])
            pricing_raw = await _modal_value(modal, ["In-app purchases", "Price", "Offers in-app purchases"])
            release_date_raw = await _modal_value(modal, ["Released on", "Release date", "Released"])
            offered_by = await _modal_value(modal, ["Offered by"])

            installs_or_users = _to_installs(installs_txt)
            release_date = _norm_date(release_date_raw)
            last_update = _norm_date(last_update)

            # Prefer 'Offered by' if developer was not found above
            if not developer and offered_by:
                developer = offered_by

            # Close modal (optional)
            try:
                close = modal.get_by_role("button", name=re.compile(r"Close|Back|^×$")).first
                if await close.count() > 0:
                    await close.click()
            except Exception:
                pass

        # Icon
        icon_url = await _grab_icon(page)

        # Developer website (Developer contact → Visit website)
        website_url = await _grab_website_url(page)

        return {
            "store": "PlayStore",
            "id": package_id,
            "app_key": f"play:{package_id}",
            "title": title,
            "developer": developer,
            "category": category,
            "rating_avg": rating_avg,
            "rating_count": rating_count,
            "rating_histogram": None,
            "installs_or_users": installs_or_users,
            "pricing_raw": pricing_raw,
            "website_url": website_url,
            "website_content_raw": None,
            "store_url": url,
            "description": description,
            "release_date": release_date,
            "last_update": last_update,
            "version": version,
            "requires_android": requires_android,
            "icon_url": icon_url,
            "content_rating": content_rating,
            "scraped_at": dt.date.today().isoformat(),
        }

def scrape_play_details(package_id: str) -> dict:
    return run(_scrape_play_async(package_id))
