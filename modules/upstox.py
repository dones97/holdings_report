"""
upstox.py — Holdings Ingestion Module

Handles:
  1. Fully automated Upstox login using Playwright (headless Chromium)
     - Opens the Upstox OAuth dialog in a hidden browser
     - Fills mobile number, PIN, and TOTP automatically
     - Captures the auth code from the redirect URL
     - Exchanges it for an access token
  2. Fetching long-term holdings from Upstox v2 API
  3. Enriching with sector data from sector_map.yaml
  4. Saving holdings.json to /data/

Required .env keys:
  UPSTOX_CLIENT_ID, UPSTOX_CLIENT_SECRET, UPSTOX_REDIRECT_URI
  UPSTOX_MOBILE, UPSTOX_PIN, UPSTOX_TOTP_SECRET
"""

import json
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path

import httpx
import pyotp
import yaml
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError, Error as PlaywrightError

load_dotenv()
logger = logging.getLogger(__name__)

# -- Paths --------------------------------------------------------------------
BASE_DIR = Path(__file__).parent.parent
SECTOR_MAP_PATH = Path(__file__).parent / "sector_map.yaml"
HOLDINGS_PATH = BASE_DIR / "data" / "holdings.json"

# -- Upstox endpoints ---------------------------------------------------------
UPSTOX_BASE = "https://api.upstox.com"
AUTH_DIALOG_URL = (
    "https://api.upstox.com/v2/login/authorization/dialog"
    "?response_type=code&client_id={client_id}&redirect_uri={redirect_uri}"
)
TOKEN_URL = f"{UPSTOX_BASE}/v2/login/authorization/token"
HOLDINGS_URL = f"{UPSTOX_BASE}/v2/portfolio/long-term-holdings"


def _load_sector_map() -> dict:
    if not SECTOR_MAP_PATH.exists():
        logger.warning("sector_map.yaml not found — sector data unavailable")
        return {}
    with open(SECTOR_MAP_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _get_access_token() -> str:
    """
    Automated Upstox login via headless Chromium (Playwright).

    Flow:
      1. Open the Upstox OAuth dialog URL in a hidden browser
      2. Fill mobile number -> click Continue
      3. Fill PIN -> click Continue  
      4. Fill TOTP -> click Continue
      5. Capture the auth code from the redirect URL
      6. Exchange auth code for access token via REST
    """
    client_id = os.getenv("UPSTOX_CLIENT_ID", "")
    client_secret = os.getenv("UPSTOX_CLIENT_SECRET", "")
    redirect_uri = os.getenv("UPSTOX_REDIRECT_URI", "http://127.0.0.1")
    mobile = os.getenv("UPSTOX_MOBILE", "")
    pin = os.getenv("UPSTOX_PIN", "")
    totp_secret = os.getenv("UPSTOX_TOTP_SECRET", "")

    missing = [k for k, v in {
        "UPSTOX_CLIENT_ID": client_id,
        "UPSTOX_CLIENT_SECRET": client_secret,
        "UPSTOX_MOBILE": mobile,
        "UPSTOX_PIN": pin,
        "UPSTOX_TOTP_SECRET": totp_secret,
    }.items() if not v or v == "FILL_IN"]

    if missing:
        raise EnvironmentError(
            f"Missing Upstox credentials in .env: {', '.join(missing)}\n"
            "Run: python scripts/get_token.py  for setup instructions."
        )

    auth_url = AUTH_DIALOG_URL.format(
        client_id=client_id,
        redirect_uri=redirect_uri,
    )

    logger.info("Upstox: starting headless browser login...")
    auth_code = None

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
            ]
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
        )
        page = context.new_page()

        try:
            # Step 1: Navigate to Upstox auth dialog
            logger.info("Upstox: opening auth dialog...")
            page.goto(auth_url, wait_until="networkidle", timeout=30000)
            page.wait_for_timeout(2000)

            # Step 2: Enter mobile number
            # Selector confirmed from live page: input[id*='mobileNum']
            logger.info("Upstox: entering mobile number...")
            mobile_input = page.wait_for_selector(
                "input[id*='mobileNum'], input[type='tel'], input[type='text']",
                timeout=15000,
                state="visible",
            )
            mobile_input.fill(mobile)
            page.wait_for_timeout(500)

            page.click("button:has-text('Get OTP')", timeout=10000)
            logger.info("Upstox: clicked 'Get OTP', waiting for OTP/TOTP screen...")
            page.wait_for_timeout(3000)

            # Step 3: Enter TOTP
            # After mobile submit, Upstox shows "Enter OTP or TOTP" — a single text field.
            # We enter the TOTP code here (no SMS needed when TOTP is enabled).
            # Field confirmed from live page: input[type='text'] on OTP screen
            logger.info("Upstox: entering TOTP on OTP/TOTP screen...")
            totp = pyotp.TOTP(totp_secret)
            otp_code = totp.now()
            logger.info("Upstox: TOTP code generated (%s)", otp_code)

            otp_input = page.wait_for_selector(
                "input[type='text']:visible, input[type='number']:visible",
                timeout=10000,
                state="visible",
            )
            otp_input.fill(otp_code)
            page.wait_for_timeout(500)

            page.click("button:has-text('Continue')", timeout=8000)
            logger.info("Upstox: submitted TOTP, waiting for PIN screen...")
            page.wait_for_timeout(3000)

            # Step 4: Enter PIN
            # After TOTP, Upstox shows "Enter 6-digit PIN" — a single password field
            # Field confirmed from live page: input[id='pinCode'][type='password']
            logger.info("Upstox: entering PIN...")
            pin_input = page.wait_for_selector(
                "input[id='pinCode'], input[type='password']",
                timeout=10000,
                state="visible",
            )
            pin_input.fill(pin)
            page.wait_for_timeout(500)

            page.click("button:has-text('Continue')", timeout=8000)
            logger.info("Upstox: submitted PIN, waiting for redirect...")

            # Step 5: Capture the auth code from the redirect URL
            # Upstox redirects to redirect_uri?code=XXXXX.
            # Since http://127.0.0.1 has no server, the browser gets
            # ERR_CONNECTION_REFUSED — but we capture the URL via a
            # request listener BEFORE the error fires.
            logger.info("Upstox: waiting for redirect with auth code...")
            redirect_url_holder = []

            def on_request(request):
                url = request.url
                if redirect_uri in url and "code=" in url:
                    redirect_url_holder.append(url)

            page.on("request", on_request)

            try:
                page.wait_for_url(
                    f"{redirect_uri}*",
                    timeout=20000,
                    wait_until="commit",
                )
            except (PlaywrightTimeoutError, PlaywrightError):
                pass  # Expected — 127.0.0.1 has no server

            # Try URL from listener first, then from page.url
            current_url = redirect_url_holder[0] if redirect_url_holder else page.url
            logger.info("Upstox: redirect URL: %s", current_url[:100])

            match = re.search(r"[?&]code=([^&]+)", current_url)
            if match:
                auth_code = match.group(1)
                logger.info("Upstox: auth code obtained")

        except Exception as e:
            logger.error("Upstox browser login failed at: %s", str(e))
            try:
                screenshot_path = BASE_DIR / "logs" / "upstox_error.png"
                screenshot_path.parent.mkdir(exist_ok=True)
                page.screenshot(path=str(screenshot_path))
                logger.info("Screenshot saved to %s", screenshot_path)
            except Exception:
                pass
            raise

        finally:
            browser.close()


    if not auth_code:
        raise RuntimeError(
            "Could not extract auth code from Upstox redirect. "
            "Check logs/upstox_error.png for a screenshot of where login got stuck."
        )

    # Step 6: Exchange auth code for access token
    logger.info("Upstox: exchanging auth code for access token...")
    with httpx.Client(timeout=30.0) as client:
        resp = client.post(
            TOKEN_URL,
            data={
                "code": auth_code,
                "client_id": client_id,
                "client_secret": client_secret,
                "redirect_uri": redirect_uri,
                "grant_type": "authorization_code",
            },
            headers={"accept": "application/json"},
        )
        resp.raise_for_status()
        token_data = resp.json()

    access_token = token_data.get("access_token", "")
    if not access_token:
        raise RuntimeError(f"No access token in response: {token_data}")

    logger.info("Upstox: access token obtained successfully")
    return access_token


def _fetch_holdings(access_token: str) -> list[dict]:
    """Call the Upstox holdings endpoint and return raw holding records."""
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {access_token}",
    }
    with httpx.Client(timeout=30.0) as client:
        resp = client.get(HOLDINGS_URL, headers=headers)
        resp.raise_for_status()
        data = resp.json()

    if data.get("status") != "success":
        raise RuntimeError(f"Upstox holdings API error: {data}")

    raw_holdings = data.get("data", [])
    logger.info("Upstox: fetched %d holdings", len(raw_holdings))
    return raw_holdings


def _normalise_holding(raw: dict, sector_map: dict) -> dict:
    """Convert a raw Upstox holding to the standard pipeline schema."""
    symbol = raw.get("tradingsymbol", "UNKNOWN")
    meta = sector_map.get(symbol, {})

    avg_price = raw.get("average_price", 0) or 0
    last_price = raw.get("last_price", 0) or 0
    pnl = raw.get("pnl", 0) or 0

    pnl_pct = 0.0
    if avg_price and avg_price > 0:
        pnl_pct = round(((last_price - avg_price) / avg_price) * 100, 2)

    return {
        "symbol": symbol,
        "isin": raw.get("isin", ""),
        "company_name": meta.get("company_name") or raw.get("company_name", symbol),
        "sector": meta.get("sector", "Unknown"),
        "bse_code": meta.get("bse_code", ""),
        "quantity": raw.get("quantity", 0),
        "avg_price": round(avg_price, 2),
        "current_price": round(last_price, 2),
        "pnl_abs": round(pnl, 2),
        "pnl_pct": pnl_pct,
        # Enrichment fields filled by later modules
        "has_earnings": False,
        "earnings_headline": None,
        "news_articles": [],
        "industry_articles": [],
    }


def get_holdings() -> list[dict]:
    """
    Main entry point. Returns enriched list of holdings dicts.
    Also saves holdings.json to the data/ directory.
    """
    HOLDINGS_PATH.parent.mkdir(exist_ok=True)
    sector_map = _load_sector_map()

    access_token = _get_access_token()
    raw_holdings = _fetch_holdings(access_token)

    if not raw_holdings:
        logger.warning("No holdings returned from Upstox — check account or API status")
        return []

    holdings = [_normalise_holding(h, sector_map) for h in raw_holdings]

    payload = {
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "count": len(holdings),
        "holdings": holdings,
    }
    with open(HOLDINGS_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    logger.info("Holdings saved to %s (%d records)", HOLDINGS_PATH, len(holdings))
    return holdings


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    holdings = get_holdings()
    for h in holdings:
        print(f"{h['symbol']:15} {h['company_name']:40} P&L: {h['pnl_pct']:+.1f}%")
