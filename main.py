"""
main.py — Pipeline Orchestrator

Runs the full weekly portfolio digest pipeline:
  1. Fetch holdings from Upstox
  2. Detect earnings via BSE
  3. Fetch news via Brave Search
  4. Synthesise digest via Gemini
  5. Send email

Designed to be run:
  a) Manually:       python main.py
  b) Scheduled:      python main.py --scheduled  (skips interactive prompts)
  c) Test mode:      python main.py --test        (sends email without waiting for Sunday)

Error handling: each step is isolated. Failures are logged and noted in the
email rather than crashing the pipeline. The email ALWAYS goes out.
"""

import argparse
import json
import logging
import logging.handlers
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

# ── Load env before importing modules ────────────────────────────────────────
BASE_DIR = Path(__file__).parent
load_dotenv(BASE_DIR / ".env")

from modules import upstox, earnings, news, llm, email_sender, returns  # noqa: E402

# ── Logging Setup ─────────────────────────────────────────────────────────────
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

def setup_logging():
    log_file = LOG_DIR / "pipeline.log"
    formatter = logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Rotating file handler — max 5 MB, keep 4 weeks
    file_handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=5 * 1024 * 1024, backupCount=4, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(file_handler)
    root.addHandler(console_handler)


logger = logging.getLogger("main")


# ── Pipeline ──────────────────────────────────────────────────────────────────

def run_pipeline() -> dict:
    """
    Execute the full pipeline. Returns a run_summary dict.
    Each module failure is captured — the pipeline continues.
    """
    start_time = time.time()
    run_summary = {
        "started_at": datetime.utcnow().isoformat() + "Z",
        "modules_ok": [],
        "modules_failed": [],
        "total_queries": 0,
        "run_duration_sec": 0,
    }

    holdings = []
    sector_news_data = {}
    digest = {
        "week_ending": datetime.now().strftime("%Y-%m-%d"),
        "portfolio_summary": "Pipeline encountered errors — see run summary below.",
        "holdings": [],
        "_llm_ok": False,
        "_llm_model": "N/A",
        "_raw_llm_response": "",
    }

    # ── Step 1: Fetch Holdings ────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("STEP 1/5: Fetching holdings from Upstox")
    logger.info("=" * 60)
    try:
        holdings = upstox.get_holdings()
        if not holdings:
            raise ValueError("Upstox returned empty holdings list")
        run_summary["modules_ok"].append("upstox")
        logger.info("OK Holdings: %d positions fetched", len(holdings))
    except Exception as e:
        run_summary["modules_failed"].append("upstox")
        logger.error("FAIL Upstox FAILED: %s", str(e))
        # Cannot continue without holdings — send failure email
        digest["portfolio_summary"] = (
            f"WARNING Pipeline failed at Step 1 (Holdings fetch): {str(e)[:200]}. "
            "No report could be generated this week. Check logs/pipeline.log for details."
        )
        _send_email_safe(digest, run_summary, start_time)
        return run_summary

    # ── Step 1.5: Calculate Weekly Returns ────────────────────────────────────
    try:
        weekly_returns = returns.get_weekly_returns(holdings)
        run_summary.update(weekly_returns)
    except Exception as e:
        logger.error("Failed to calculate weekly returns: %s", str(e))

    # ── Step 2: Earnings Detection ────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("STEP 2/5: Detecting earnings (%d holdings)", len(holdings))
    logger.info("=" * 60)
    try:
        holdings = earnings.detect_earnings(holdings)
        earnings_count = sum(1 for h in holdings if h.get("has_earnings"))
        run_summary["modules_ok"].append("earnings")
        logger.info("OK Earnings: %d/%d holdings have recent results", earnings_count, len(holdings))
    except Exception as e:
        run_summary["modules_failed"].append("earnings")
        logger.error("FAIL Earnings detection FAILED: %s — continuing without earnings data", str(e))
        # Non-fatal — mark all holdings as no_earnings and continue
        for h in holdings:
            h.setdefault("has_earnings", False)
            h.setdefault("earnings_headline", None)

    # ── Step 3: News Enrichment ───────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("STEP 3/5: Fetching news (%d holdings)", len(holdings))
    logger.info("=" * 60)
    try:
        holdings, sector_news_data = news.fetch_news(holdings)
        total_articles = sum(len(h.get("news_articles", [])) for h in holdings)
        run_summary["modules_ok"].append("news")
        run_summary["total_queries"] = len(holdings) * 2 + len(sector_news_data)
        logger.info(
            "OK News: %d articles across %d holdings, %d sector summaries",
            total_articles, len(holdings), len(sector_news_data)
        )
    except Exception as e:
        run_summary["modules_failed"].append("news")
        logger.error("FAIL News FAILED: %s — continuing without news data", str(e))
        for h in holdings:
            h.setdefault("news_articles", [])

    # ── Step 4: LLM Synthesis ────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("STEP 4/5: Synthesising digest via LLM")
    logger.info("=" * 60)
    try:
        digest = llm.synthesise(holdings, sector_news_data)
        if digest.get("_llm_ok"):
            run_summary["modules_ok"].append("llm")
            logger.info("OK LLM: digest synthesised for %d holdings", len(digest.get("holdings", [])))
        else:
            run_summary["modules_failed"].append("llm")
            logger.warning("FAIL LLM: synthesis failed — raw text fallback in email")
    except Exception as e:
        run_summary["modules_failed"].append("llm")
        logger.error("FAIL LLM FAILED: %s", str(e))
        digest["_raw_llm_response"] = str(e)

    # ── Step 5: Send Email ────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("STEP 5/5: Sending email")
    logger.info("=" * 60)
    _send_email_safe(digest, run_summary, start_time)

    return run_summary


def _send_email_safe(digest: dict, run_summary: dict, start_time: float):
    """Send the email — catches and logs errors without re-raising."""
    run_summary["run_duration_sec"] = round(time.time() - start_time)
    try:
        email_sender.send_digest(digest, run_summary)
        run_summary["modules_ok"].append("email")
        logger.info("OK Email delivered successfully")
    except Exception as e:
        run_summary["modules_failed"].append("email")
        logger.error("FAIL Email delivery FAILED: %s", str(e))
        logger.error("  The digest was synthesised but could not be delivered.")
        logger.error("  Check GMAIL_ADDRESS and GMAIL_APP_PASSWORD in .env")


def _log_run_summary(run_summary: dict):
    """Print a clean summary of the pipeline run."""
    logger.info("")
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info("  Duration   : %ds", run_summary.get("run_duration_sec", 0))
    logger.info("  Queries    : %d", run_summary.get("total_queries", 0))

    ok = run_summary.get("modules_ok", [])
    failed = run_summary.get("modules_failed", [])
    if ok:
        logger.info("  OK         : %s", ", ".join(ok))
    if failed:
        logger.warning("  FAILED     : %s", ", ".join(failed))
    logger.info("=" * 60)


# ── Entry Point ───────────────────────────────────────────────────────────────

def main():
    setup_logging()

    parser = argparse.ArgumentParser(description="Holdings Report Agent — Weekly Digest Pipeline")
    parser.add_argument(
        "--test", action="store_true",
        help="Run immediately regardless of schedule (for testing)"
    )
    parser.add_argument(
        "--scheduled", action="store_true",
        help="Scheduled mode — no interactive prompts"
    )
    args = parser.parse_args()

    logger.info("")
    logger.info("=" * 50)
    logger.info("  Holdings Report Agent -- Pipeline Start")
    logger.info("=" * 50)
    logger.info("Mode: %s", "TEST" if args.test else "SCHEDULED" if args.scheduled else "MANUAL")
    logger.info("Time: %s IST", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("")

    run_summary = run_pipeline()
    _log_run_summary(run_summary)

    # Exit with error code if critical modules failed
    failed = run_summary.get("modules_failed", [])
    if "upstox" in failed or "email" in failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
