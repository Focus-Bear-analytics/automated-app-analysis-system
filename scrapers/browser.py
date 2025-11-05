# scrapers/browser.py
import asyncio
from contextlib import asynccontextmanager
from playwright.async_api import async_playwright

_CHROME_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/127.0.0.0 Safari/537.36"
)

@asynccontextmanager
async def chromium_page(headless: bool = True, user_agent: str | None = None, locale: str = "en-US"):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        ctx = await browser.new_context(
            user_agent=user_agent or _CHROME_UA,
            locale=locale,
            viewport={"width": 1366, "height": 900},
        )
        page = await ctx.new_page()
        try:
            yield page
        finally:
            await ctx.close()
            await browser.close()

def run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)
