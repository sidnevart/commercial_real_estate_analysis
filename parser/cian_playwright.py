# parser/cian_playwright.py
"""CIAN parser via Playwright with universal JSON extraction and anti-bot bypassing."""

from __future__ import annotations
import json, logging, random, re, time
from pathlib import Path
from typing import List, Optional

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError, Page
from parser.proxy_pool import get as proxy_get
from parser.retry import retry
from core.models import Offer

log = logging.getLogger(__name__)

URLS = [
    "https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=offices&region=1,4593",
    "https://www.cian.ru/commercial/sale/?deal_type=sale&region=1",
    "https://www.cian.ru/commercial/"
]

XHR_RE = re.compile(r"search-offers|officeFeed|find")
COOKIES_PATH = Path("browser_cookies.json")
VIEWPORT_SIZES = [
    {"width": 1920, "height": 1080},
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
]

UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
]

# Script patterns to extract data
SCRIPT_PATTERNS = [
    r'"products":\s*(\[.*?\])',
    r'"offers":\s*(\[.*?\])',
    r'"items":\s*(\[.*?\])',
    r"window\._cianConfig\s*=\s*(\{.*?\});",
    r"window\.__INITIAL_DATA__\s*=\s*(\{.*?\});",
    r"window\.__INITIAL_STATE__\s*=\s*(\{.*?\});",
    r"c\.ca\([^,]+,\s*(\{.*\})\)"
]


def _json_to_offers(seq: List[dict]) -> List[Offer]:
    offers = []
    for j in seq:
        try:
            # Handle different JSON structures
            if "id" in j or "cianId" in j:
                oid = str(j.get("cianId") or j.get("id"))
                
                # Extract price from different possible structures
                price = None
                if "bargainTerms" in j and "priceRur" in j["bargainTerms"]:
                    price = j["bargainTerms"]["priceRur"]
                elif "price" in j:
                    price = j["price"]
                
                # Extract area
                area = j.get("totalArea")
                
                if price and area:
                    offers.append(
                        Offer(
                            id=oid,
                            lot_uuid=None,
                            price=price,
                            area=area,
                            url=f"https://www.cian.ru/sale/commercial/{oid}/",
                            type="sale",
                        )
                    )
        except Exception as e:
            log.warning(f"Error processing offer: {e}")
    
    return offers


async def add_human_behavior(page: Page):
    """Add human-like behavior to avoid detection."""
    # Random mouse movements
    for _ in range(random.randint(2, 5)):
        await page.mouse.move(
            random.randint(100, 800),
            random.randint(100, 600)
        )
        await page.wait_for_timeout(random.randint(200, 800))
    
    # Random scrolling
    for _ in range(random.randint(1, 3)):
        await page.mouse.wheel(0, random.randint(200, 600))
        await page.wait_for_timeout(random.randint(500, 1500))
    
    # Sometimes click around
    if random.random() < 0.3:  # 30% chance
        try:
            elements = await page.query_selector_all('a, button')
            if elements:
                random_element = random.choice(elements)
                await random_element.hover()
                # Don't actually click to avoid navigation
        except Exception:
            pass


@retry()
async def fetch_offers() -> List[Offer]:
    proxy = proxy_get()
    ua = random.choice(UAS)
    viewport = random.choice(VIEWPORT_SIZES)
    offers: List[Offer] = []

    async with async_playwright() as pw:
        # Try non-headless first if we're getting blocked
        headless = random.choice([True, False, False])  # 33% chance of headless
        
        browser_options = {
            "headless": headless,
            "proxy": {"server": proxy} if proxy else None
        }
        
        br = await pw.chromium.launch(**browser_options)
        
        # Configure context with realistic browser parameters
        context_options = {
            "viewport": viewport,
            "user_agent": ua,
            "extra_http_headers": {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate", 
                "Sec-Fetch-Site": "none"
            }
        }
        
        # Load cookies if they exist
        if COOKIES_PATH.exists():
            cookies = json.loads(COOKIES_PATH.read_text())
            context_options["storage_state"] = {"cookies": cookies}
            log.debug("Loaded cookies from previous session")
        
        ctx = await br.new_context(**context_options)
        
        # Disable webdriver flag
        await ctx.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });
        Object.defineProperty(navigator, 'platform', { 
            get: () => 'Win32' 
        });
        """)
        
        page = await ctx.new_page()
        
        # Try different URLs if needed
        successful_load = False
        html = ""
        
        for url in URLS:
            try:
                log.info("Trying URL: %s", url)
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                
                # Wait a bit and add human behavior
                await page.wait_for_timeout(random.randint(2000, 5000))
                await add_human_behavior(page)
                
                # Wait for content to fully load
                await page.wait_for_load_state("networkidle")
                
                # Get the page content
                html = await page.content()
                
                # Check if we got a real page, not just anti-bot
                if len(html) > 10000 and "captcha" not in html.lower():
                    successful_load = True
                    break
                else:
                    log.warning(f"URL {url} returned suspicious content")
                    
            except Exception as e:
                log.warning(f"Error loading {url}: {e}")
                await page.wait_for_timeout(2000)
        
        if not successful_load:
            await br.close()
            raise RuntimeError("Could not load any URLs successfully")
        
        # Сохраняем дамп для отладки
        Path("cian_dump.html").write_text(html, encoding="utf-8")
        log.warning("Saved page HTML to cian_dump.html (%d bytes)", len(html))
        
        # Check for anti-bot measures
        if "captcha" in html.lower() or "robot" in html.lower():
            log.warning("Possible CAPTCHA/anti-bot detection on page")
            await br.close()
            raise RuntimeError("Anti-bot detection triggered")

        # Save cookies for future use
        cookies = await ctx.cookies()
        COOKIES_PATH.write_text(json.dumps(cookies))
        log.debug("Saved cookies for future sessions")
            
        # 1) Пытаемся поймать XHR
        try:
            log.debug("Waiting for XHR response...")
            resp = await page.wait_for_event(
                "response",
                predicate=lambda r: XHR_RE.search(r.url or ""),
                timeout=15000,
            )
            
            log.debug(f"Found XHR: {resp.url}")
            
            # Extract the data from response
            try:
                json_data = await resp.json()
                
                # Handle different API response formats
                if "data" in json_data and "offersSerialized" in json_data["data"]:
                    data = json_data["data"]["offersSerialized"]
                elif "results" in json_data and "offers" in json_data["results"]:
                    data = json_data["results"]["offers"]
                elif "items" in json_data:
                    data = json_data["items"]
                else:
                    data = []
                    log.warning(f"Unknown API response structure: {list(json_data.keys())}")
                
                offers = _json_to_offers(data)
                log.debug("Found %d offers via XHR", len(offers))
                
            except Exception as e:
                log.warning(f"Error parsing XHR response: {e}")
                data = []

        except TimeoutError:
            log.debug("XHR not found, parsing inline scripts")

            # 2) Parse HTML for scripts containing data
            soup = BeautifulSoup(html, "lxml")
            
            # Debug script content
            scripts_content = [tag.string for tag in soup.find_all("script") if tag.string]
            log.debug("Found %d scripts in HTML", len(scripts_content))
            
            # Look for all script tags
            all_scripts_text = "".join([s for s in scripts_content if s])
            
            # Try to extract data using different patterns
            data_extracted = False
            
            for pattern in SCRIPT_PATTERNS:
                try:
                    matches = re.search(pattern, all_scripts_text, re.DOTALL)
                    if matches:
                        log.debug(f"Matched pattern: {pattern}")
                        json_str = matches.group(1)
                        
                        try:
                            json_data = json.loads(json_str)
                            
                            # Handle different data structures
                            if isinstance(json_data, list):
                                items = json_data
                            elif isinstance(json_data, dict):
                                # Look for offers in different possible locations
                                items = (json_data.get("products") or 
                                         json_data.get("offers") or 
                                         json_data.get("items") or 
                                         json_data.get("results", {}).get("offers") or
                                         [])
                            else:
                                items = []
                                
                            if items:
                                offers = _json_to_offers(items)
                                log.debug("Parsed %d offers from inline script", len(offers))
                                data_extracted = True
                                break
                                
                        except json.JSONDecodeError as e:
                            log.warning(f"JSON parse error: {e}")
                            
                except Exception as e:
                    log.warning(f"Error with pattern {pattern}: {e}")
            
            if not data_extracted:
                # As a last resort, look for any JSON-like structure with offers
                try:
                    all_json_objects = re.findall(r'(\{[^{}]*"offers"\s*:\s*\[[^\]]*\][^{}]*\})', all_scripts_text)
                    
                    for json_obj in all_json_objects:
                        try:
                            # Try to fix malformed JSON (missing quotes, etc)
                            fixed_json = re.sub(r'([{,])\s*(\w+):', r'\1"\2":', json_obj)
                            data = json.loads(fixed_json).get("offers", [])
                            if data:
                                offers = _json_to_offers(data)
                                log.debug("Parsed %d offers from generic JSON", len(offers))
                                data_extracted = True
                                break
                        except:
                            continue
                except Exception as e:
                    log.warning(f"Final extraction attempt failed: {e}")

        await br.close()

    log.info("CIAN offers collected: %d", len(offers))
    return offers