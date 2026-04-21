"""
test_run.py — Manual Pipeline Trigger

Runs the full pipeline immediately and sends you the email.
Use this to validate everything works before relying on the schedule.

Usage:
  python scripts/test_run.py              # Full real pipeline
  python scripts/test_run.py --mock       # Use mock data (no API calls)
  python scripts/test_run.py --email-only # Re-send last digest without fetching new data
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

BASE_DIR = Path(__file__).parent.parent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("test_run")

MOCK_HOLDINGS = [
    {
        "symbol": "TCS",
        "company_name": "Tata Consultancy Services Ltd",
        "sector": "Information Technology",
        "pnl_pct": 12.5,
        "avg_price": 3450.0,
        "current_price": 3881.25,
        "has_earnings": True,
        "earnings_headline": "TCS Q3 FY25 Results: PAT up 5.5% YoY to ₹12,200 Cr",
        "news_articles": [
            {
                "title": "TCS wins $250M transformation deal from European banking group",
                "description": "The contract spans 5 years covering cloud migration and AI integration across 12 countries.",
                "source": "Economic Times",
            },
            {
                "title": "TCS to hire 40,000 freshers in FY26 amid demand recovery signals",
                "description": "Management indicated that discretionary spending in BFSI vertical is showing early recovery signs.",
                "source": "Mint",
            },
        ],
    },
    {
        "symbol": "HDFCBANK",
        "company_name": "HDFC Bank Ltd",
        "sector": "Banking & Financial Services",
        "pnl_pct": -3.2,
        "avg_price": 1720.0,
        "current_price": 1664.96,
        "has_earnings": False,
        "earnings_headline": None,
        "news_articles": [
            {
                "title": "HDFC Bank loan growth re-accelerates to 13% YoY in Q3",
                "description": "Deposits also grew 16% YoY. Management signalled NIM has likely bottomed out.",
                "source": "Business Standard",
            },
        ],
    },
    {
        "symbol": "RELIANCE",
        "company_name": "Reliance Industries Ltd",
        "sector": "Energy & Petrochemicals",
        "pnl_pct": 8.1,
        "avg_price": 2580.0,
        "current_price": 2789.18,
        "has_earnings": False,
        "earnings_headline": None,
        "news_articles": [
            {
                "title": "Reliance Jio to raise tariffs by 15-20% citing infrastructure investments",
                "description": "The hike, if implemented, would significantly boost ARPU and Jio's EBITDA margins.",
                "source": "Moneycontrol",
            },
        ],
    },
]

MOCK_SECTOR_NEWS = {
    "Information Technology": [
        {
            "title": "Indian IT sector sees cautious start to 2025 amid US macro uncertainty",
            "description": "NASSCOM revised FY25 revenue growth forecast to 4.5-5%, citing muted discretionary spend.",
        }
    ],
    "Banking & Financial Services": [
        {
            "title": "RBI holds rates steady; signals accommodative stance for FY26",
            "description": "The MPC decision supports credit growth momentum across PSU and private sector banks.",
        }
    ],
    "Energy & Petrochemicals": [
        {
            "title": "Brent crude drops to $78 — positive for India's import bill and energy sector margins",
            "description": "Lower crude benefits downstream operators like Reliance while hurting upstream ONGC.",
        }
    ],
}


def run_mock():
    """Send a test email using mock data — no API calls needed."""
    logger.info("Running in MOCK mode — no API calls will be made")

    from modules import llm, email_sender

    logger.info("Step 1/2: Synthesising digest with Gemini...")
    digest = llm.synthesise(MOCK_HOLDINGS, MOCK_SECTOR_NEWS)

    run_summary = {
        "modules_ok": ["upstox[mock]", "earnings[mock]", "news[mock]", "llm"],
        "modules_failed": [],
        "total_queries": 0,
        "run_duration_sec": 0,
    }

    logger.info("Step 2/2: Sending test email...")
    email_sender.send_digest(digest, run_summary)
    logger.info("✅ Test email sent! Check your inbox.")


def run_full():
    """Run the real pipeline end-to-end."""
    logger.info("Running FULL pipeline with real API calls...")
    logger.info("This will fetch your live Upstox holdings.")
    logger.info("")

    import main as pipeline_main
    pipeline_main.setup_logging()
    run_summary = pipeline_main.run_pipeline()

    failed = run_summary.get("modules_failed", [])
    if failed:
        logger.warning("Some modules failed: %s", failed)
        logger.warning("Check logs/pipeline.log for details")
        return 1
    return 0


def run_email_only():
    """Re-render and resend using cached data from last run."""
    holdings_file = BASE_DIR / "data" / "holdings.json"
    if not holdings_file.exists():
        logger.error("No cached holdings found. Run a full pipeline first.")
        sys.exit(1)

    logger.info("Email-only mode: using cached holdings from last run")
    with open(holdings_file, "r", encoding="utf-8") as f:
        cached = json.load(f)

    holdings = cached.get("holdings", [])
    logger.info("Loaded %d holdings from cache (fetched %s)", len(holdings), cached.get("fetched_at", "unknown"))

    from modules import llm, email_sender
    digest = llm.synthesise(holdings, {})

    run_summary = {
        "modules_ok": ["llm"],
        "modules_failed": [],
        "total_queries": 0,
        "run_duration_sec": 0,
    }
    email_sender.send_digest(digest, run_summary)
    logger.info("✅ Email re-sent from cached data.")


def main():
    parser = argparse.ArgumentParser(
        description="Holdings Report Agent — Manual Test Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/test_run.py              Full run with real Upstox + Brave + Gemini
  python scripts/test_run.py --mock       Mock data — only needs Gemini + Gmail keys
  python scripts/test_run.py --email-only Re-send last digest without new API calls
        """,
    )
    parser.add_argument("--mock", action="store_true", help="Use mock data (no Upstox/Brave calls)")
    parser.add_argument("--email-only", action="store_true", help="Resend using cached holdings")
    args = parser.parse_args()

    print()
    print("=" * 51)
    print("  Holdings Report Agent -- Test Run")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 51)
    print()

    if args.mock:
        run_mock()
    elif getattr(args, "email_only"):
        run_email_only()
    else:
        exit_code = run_full()
        sys.exit(exit_code)


if __name__ == "__main__":
    main()
