"""
upstox.py — Holdings Ingestion Module

Handles:
  1. Fully automated Upstox login using mobile/PIN/TOTP (zero touch)
  2. Fetching long-term holdings from Upstox v2 API
  3. Enriching with sector data from sector_map.yaml
  4. Saving holdings.json to /data/

Token strategy (in order of preference):
  A. Automated login via Upstox v3 API (mobile + PIN + pyotp) — fully headless
  B. If credentials missing, raises a clear error with instructions
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

load_dotenv()
logger = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent.parent
SECTOR_MAP_PATH = Path(__file__).parent / "sector_map.yaml"
HOLDINGS_PATH = BASE_DIR / "data" / "holdings.json"

# ── Upstox API endpoints ───────────────────────────────────────────────────────
UPSTOX_BASE = "https://api.upstox.com"
LOGIN_URL = f"{UPSTOX_BASE}/v3/login/authorization/users"
OTP_VERIFY_URL = f"{UPSTOX_BASE}/v3/login/authorization/otp/verify"
AUTH_DIALOG_URL = f"{UPSTOX_BASE}/v3/login/authorization/dialog"
TOKEN_URL = f"{UPSTOX_BASE}/v2/login/authorization/token"
HOLDINGS_URL = f"{UPSTOX_BASE}/v2/portfolio/long-term-holdings"


def _load_sector_map() -> dict:
    """Load the symbol → sector/BSE code mapping."""
    if not SECTOR_MAP_PATH.exists():
        logger.warning("sector_map.yaml not found — sector data will be unavailable")
        return {}
    with open(SECTOR_MAP_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _get_access_token() -> str:
    """
    Obtain a fresh Upstox access token using automated headless login.

    Flow:
      1. POST mobile + PIN  → get session token
      2. POST TOTP code     → get auth token
      3. GET dialog         → get redirect with auth code
      4. POST auth code     → get access token
    """
    client_id = os.getenv("UPSTOX_CLIENT_ID")
    client_secret = os.getenv("UPSTOX_CLIENT_SECRET")
    redirect_uri = os.getenv("UPSTOX_REDIRECT_URI", "http://127.0.0.1")
    mobile = os.getenv("UPSTOX_MOBILE")
    pin = os.getenv("UPSTOX_PIN")
    totp_secret = os.getenv("UPSTOX_TOTP_SECRET")

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

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
    }

    with httpx.Client(timeout=30.0) as session:
        # ── Step 1: Login with mobile + PIN ───────────────────────────────────
        logger.info("Upstox: initiating automated login for mobile %s***", mobile[:4])
        resp1 = session.post(
            LOGIN_URL,
            json={"mobile_number": mobile, "pin": pin},
            headers=headers,
        )
        resp1.raise_for_status()
        data1 = resp1.json()

        if data1.get("status") != "success":
            raise RuntimeError(f"Upstox login failed: {data1}")
        session_token = data1["data"]["token"]
        logger.debug("Upstox: login step 1 succeeded")

        # ── Step 2: Verify TOTP ───────────────────────────────────────────────
        totp = pyotp.TOTP(totp_secret)
        otp_code = totp.now()
        logger.info("Upstox: submitting TOTP code")

        resp2 = session.post(
            OTP_VERIFY_URL,
            json={
                "otp": otp_code,
                "is_push_notification": False,
                "token": session_token,
            },
            headers=headers,
        )
        resp2.raise_for_status()
        data2 = resp2.json()

        if data2.get("status") != "success":
            raise RuntimeError(f"Upstox TOTP verification failed: {data2}")
        auth_token = data2["data"]["token"]
        logger.debug("Upstox: TOTP verified")

        # ── Step 3: Get authorisation code via redirect ───────────────────────
        resp3 = session.get(
            AUTH_DIALOG_URL,
            params={
                "client_id": client_id,
                "redirect_uri": redirect_uri,
                "response_type": "code",
                "token": auth_token,
            },
            follow_redirects=False,
        )

        # The response is a redirect to redirect_uri?code=XXXXX
        location = resp3.headers.get("location", "")
        match = re.search(r"[?&]code=([^&]+)", location)
        if not match:
            raise RuntimeError(
                f"Could not extract auth code from redirect. Location: {location}"
            )
        auth_code = match.group(1)
        logger.debug("Upstox: auth code obtained")

        # ── Step 4: Exchange auth code for access token ───────────────────────
        resp4 = session.post(
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
        resp4.raise_for_status()
        token_data = resp4.json()

        access_token = token_data.get("access_token")
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
    """
    Convert raw Upstox holding to the standard schema used throughout the pipeline.

    Upstox field reference:
      tradingsymbol, isin, company_name, quantity, average_price,
      last_price, pnl, day_change_percentage, close_price
    """
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

    # ── Get fresh access token ────────────────────────────────────────────────
    access_token = _get_access_token()

    # ── Fetch and normalise holdings ──────────────────────────────────────────
    raw_holdings = _fetch_holdings(access_token)

    if not raw_holdings:
        logger.warning("No holdings returned from Upstox — check account or API status")
        return []

    holdings = [_normalise_holding(h, sector_map) for h in raw_holdings]

    # ── Persist to disk ───────────────────────────────────────────────────────
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
