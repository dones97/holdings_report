"""
earnings.py — BSE Earnings Detection Module

For each holding, checks if the company published quarterly/annual financial
results in the past 7 days using the BSE Corporate Announcements API.

Flow:
  1. Primary: BSE Corporate Announcements API (free, public)
  2. Fallback: Brave Search for earnings news if BSE API fails

Returns holdings list with has_earnings and earnings_headline populated.
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent
EARNINGS_DIR = BASE_DIR / "data" / "earnings"

# ── BSE Corporate Announcements API ─────────────────────────────────────────
BSE_ANN_URL = (
    "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"
)
BSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.bseindia.com/",
    "Accept": "application/json, text/plain, */*",
}

EARNINGS_KEYWORDS = [
    "financial results",
    "quarterly results",
    "annual results",
    "unaudited financial results",
    "audited financial results",
    "standalone financial results",
    "consolidated financial results",
]


def _dates() -> tuple[str, str]:
    """Return (7_days_ago, today) as DD/MM/YYYY strings (BSE format)."""
    today = datetime.now()
    week_ago = today - timedelta(days=7)
    fmt = "%d/%m/%Y"
    return week_ago.strftime(fmt), today.strftime(fmt)


def _check_bse(holding: dict) -> tuple[bool, str | None]:
    """
    Query BSE Corporate Announcements API for this holding.
    Returns (has_earnings, headline_or_None).
    """
    bse_code = holding.get("bse_code", "")
    if not bse_code:
        logger.debug("%s: no BSE code in sector_map — skipping BSE check", holding["symbol"])
        return False, None

    from_date, to_date = _dates()

    params = {
        "pageno": "1",
        "strCat": "Financial Results",
        "strPrevDate": from_date,
        "strScrip": bse_code,
        "strSearch": "P",
        "strToDate": to_date,
        "strType": "C",
    }

    try:
        with httpx.Client(timeout=15.0) as client:
            resp = client.get(BSE_ANN_URL, params=params, headers=BSE_HEADERS)
            resp.raise_for_status()
            data = resp.json()

        announcements = data.get("Table", [])
        if not announcements:
            return False, None

        for ann in announcements:
            subject = (ann.get("SLONGNAME") or ann.get("NEWSSUB") or "").lower()
            category = (ann.get("CATEGORYNAME") or "").lower()
            combined = subject + " " + category

            if any(kw in combined for kw in EARNINGS_KEYWORDS):
                headline = ann.get("NEWSSUB") or ann.get("SLONGNAME") or "Financial Results"
                logger.info(
                    "%s: EARNINGS DETECTED via BSE — '%s'",
                    holding["symbol"], headline
                )
                return True, headline

        return False, None

    except httpx.HTTPStatusError as e:
        logger.warning("%s: BSE API HTTP error %s", holding["symbol"], e.response.status_code)
        return False, None
    except Exception as e:
        logger.warning("%s: BSE API error — %s", holding["symbol"], str(e))
        return False, None


def _check_brave_fallback(holding: dict) -> tuple[bool, str | None]:
    """
    Fallback: use Brave Search to find earnings announcements for this holding.
    Only called when BSE API fails or BSE code is missing.
    """
    api_key = os.getenv("BRAVE_SEARCH_API_KEY", "")
    if not api_key or api_key == "FILL_IN":
        return False, None

    company = holding.get("company_name", holding["symbol"])
    query = f'"{company}" quarterly results financial results site:bseindia.com OR site:nseindia.com OR site:moneycontrol.com'

    from_date_iso = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

    try:
        with httpx.Client(timeout=15.0) as client:
            resp = client.get(
                "https://api.search.brave.com/res/v1/web/search",
                params={
                    "q": query,
                    "count": 3,
                    "freshness": f"{from_date_iso}to{datetime.now().strftime('%Y-%m-%d')}",
                },
                headers={
                    "Accept": "application/json",
                    "Accept-Encoding": "gzip",
                    "X-Subscription-Token": api_key,
                },
            )
            resp.raise_for_status()
            results = resp.json().get("web", {}).get("results", [])

        for r in results:
            title = (r.get("title") or "").lower()
            if any(kw in title for kw in EARNINGS_KEYWORDS):
                headline = r.get("title", "Financial Results")
                logger.info(
                    "%s: EARNINGS DETECTED via Brave fallback — '%s'",
                    holding["symbol"], headline
                )
                return True, headline

        return False, None

    except Exception as e:
        logger.warning("%s: Brave earnings fallback error — %s", holding["symbol"], str(e))
        return False, None


def detect_earnings(holdings: list[dict]) -> list[dict]:
    """
    Main entry point. Enriches each holding with has_earnings and earnings_headline.
    Saves per-holding JSON to data/earnings/.
    """
    EARNINGS_DIR.mkdir(parents=True, exist_ok=True)

    for holding in holdings:
        symbol = holding["symbol"]
        logger.info("Checking earnings for %s...", symbol)

        # Try BSE first
        has_earnings, headline = _check_bse(holding)

        # Fallback to Brave if BSE gave nothing
        if not has_earnings and not holding.get("bse_code"):
            has_earnings, headline = _check_brave_fallback(holding)

        holding["has_earnings"] = has_earnings
        holding["earnings_headline"] = headline

        # Save to disk
        earnings_file = EARNINGS_DIR / f"{symbol}.json"
        with open(earnings_file, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "symbol": symbol,
                    "has_earnings": has_earnings,
                    "earnings_headline": headline,
                    "checked_at": datetime.utcnow().isoformat() + "Z",
                },
                f,
                indent=2,
            )

        # Brief pause to be respectful of BSE's public API
        time.sleep(0.5)

    earnings_count = sum(1 for h in holdings if h["has_earnings"])
    logger.info(
        "Earnings detection complete: %d/%d holdings have recent results",
        earnings_count, len(holdings)
    )
    return holdings


if __name__ == "__main__":
    import yaml
    logging.basicConfig(level=logging.INFO)

    # Test with a sample holding
    test = [{
        "symbol": "RELIANCE",
        "company_name": "Reliance Industries Ltd",
        "bse_code": "500325",
    }]
    result = detect_earnings(test)
    print(f"has_earnings: {result[0]['has_earnings']}")
    print(f"headline: {result[0]['earnings_headline']}")
