"""
email_sender.py — Email Rendering & Delivery Module

Renders the LLM digest into a rich HTML email using Jinja2
and delivers it via Gmail SMTP.
"""

import logging
import os
import smtplib
import ssl
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from jinja2 import Environment, FileSystemLoader
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent
TEMPLATE_DIR = BASE_DIR / "templates"


def _render_html(digest: dict, run_summary: dict) -> str:
    """Render the Jinja2 template with digest data."""
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATE_DIR)),
        autoescape=True,
    )
    template = env.get_template("email_template.html")

    context = {
        "digest": digest,
        "run_summary": run_summary,
        "week_ending": digest.get("week_ending", datetime.now().strftime("%Y-%m-%d")),
        "llm_ok": digest.get("_llm_ok", False),
        "raw_llm_response": digest.get("_raw_llm_response", ""),
        "generated_at": datetime.now().strftime("%d %b %Y, %I:%M %p IST"),
    }
    return template.render(**context)


def _send_smtp(subject: str, html_body: str) -> bool:
    """Send email via Gmail SMTP. Returns True on success."""
    sender = os.getenv("GMAIL_ADDRESS", "")
    password = os.getenv("GMAIL_APP_PASSWORD", "")
    recipient = os.getenv("RECIPIENT_EMAIL", sender)

    if not sender or sender == "FILL_IN":
        raise EnvironmentError("GMAIL_ADDRESS not set in .env")
    if not password or password == "FILL_IN":
        raise EnvironmentError(
            "GMAIL_APP_PASSWORD not set in .env. "
            "Generate one at https://myaccount.google.com/apppasswords"
        )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"Portfolio Digest <{sender}>"
    msg["To"] = recipient

    # Plain text fallback
    plain = (
        f"Portfolio Digest — Week of {datetime.now().strftime('%d %b %Y')}\n\n"
        "This email requires an HTML-capable email client.\n"
        "Please view it in Gmail or Outlook."
    )
    msg.attach(MIMEText(plain, "plain"))
    msg.attach(MIMEText(html_body, "html"))

    context = ssl.create_default_context()
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.ehlo()
            server.starttls(context=context)
            server.ehlo()
            server.login(sender, password)
            server.sendmail(sender, recipient, msg.as_string())

        logger.info("Email delivered to %s", recipient)
        return True

    except smtplib.SMTPAuthenticationError:
        logger.error(
            "Gmail authentication failed. "
            "Check GMAIL_ADDRESS and GMAIL_APP_PASSWORD in .env. "
            "Ensure 2FA is enabled and App Password is correct."
        )
        raise
    except smtplib.SMTPException as e:
        logger.error("SMTP error: %s", str(e))
        raise


def send_digest(digest: dict, run_summary: dict) -> bool:
    """
    Main entry point. Renders and sends the weekly digest email.

    Args:
        digest:      Structured dict returned by llm.synthesise()
        run_summary: Dict with pipeline stats (modules_ok, modules_failed, etc.)

    Returns:
        True on successful delivery, raises on failure.
    """
    holdings_count = len(digest.get("holdings", []))
    week_of = digest.get("week_ending", datetime.now().strftime("%Y-%m-%d"))

    subject = (
        f"Portfolio Digest \u2014 Week of {week_of} | {holdings_count} holdings reviewed"
    )
    if not digest.get("_llm_ok", True):
        subject = f"\u26a0\ufe0f {subject} [LLM Error]"

    logger.info("Rendering email for %d holdings...", holdings_count)
    html_body = _render_html(digest, run_summary)

    logger.info("Sending email: %s", subject)
    return _send_smtp(subject, html_body)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Test with mock digest
    mock_digest = {
        "week_ending": "2025-01-19",
        "_llm_ok": True,
        "_llm_model": "gemini-2.5-flash",
        "portfolio_summary": "Your portfolio showed resilience this week amid mixed market conditions. IT sector stocks underperformed due to global macro concerns while banking holdings held steady.",
        "holdings": [
            {
                "symbol": "TCS",
                "company_name": "Tata Consultancy Services Ltd",
                "signal": "HOLD WITH CAUTION",
                "signal_reason": "Q3 results missed revenue estimates; management guidance cautious on near-term demand.",
                "news_summary": "TCS reported Q3 FY25 revenue of ₹63,000 cr, up 4.5% YoY but below street estimates of ₹63,800 cr. Deal wins remain healthy at $8.1B TCV.",
                "earnings_summary": "Revenue grew 4.5% YoY to ₹63,000 cr. PAT rose 5.5% to ₹12,200 cr. EBIT margin at 24.5%, flat QoQ. Headcount declined 2,300 reflecting weak hiring sentiment.",
                "has_earnings": True,
                "industry_insight": "Indian IT sector faces near-term pressure from US discretionary spend cuts. Large deals pipeline remains healthy for FY26.",
                "flags": [
                    "Watch Q4 deal conversion rate closely",
                    "BFSI vertical recovery expected in H1 FY26",
                ],
            },
            {
                "symbol": "HDFCBANK",
                "company_name": "HDFC Bank Ltd",
                "signal": "HOLD CONFIDENTLY",
                "signal_reason": "Loan growth re-accelerating, NIM pressure stabilising, and asset quality remains pristine.",
                "news_summary": "HDFC Bank reported strong Q3 with advances growing 13% YoY. Management indicated NIM bottomed out and expects gradual improvement.",
                "has_earnings": False,
                "industry_insight": "Indian banking sector benefits from strong credit demand and improving deposit growth post-merger adjustments.",
                "flags": ["Deposit growth trajectory key to watch"],
            },
        ],
    }

    mock_run_summary = {
        "modules_ok": ["upstox", "earnings", "news", "llm"],
        "modules_failed": [],
        "total_queries": 28,
        "run_duration_sec": 87,
    }

    send_digest(mock_digest, mock_run_summary)
