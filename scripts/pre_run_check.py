"""
pre_run_check.py — Sunday Pre-Run Health Check

Runs at 12:00 IST, 6 hours before the main pipeline.
Validates all API keys and sends a warning email if anything is broken.
This gives you time to fix issues before the Sunday 18:00 run.
"""

import os
import sys
import smtplib
import ssl
import logging
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("pre_check")


def check_env_vars() -> list[str]:
    """Check all required .env variables are set."""
    required = [
        "UPSTOX_CLIENT_ID", "UPSTOX_CLIENT_SECRET",
        "UPSTOX_MOBILE", "UPSTOX_PIN", "UPSTOX_TOTP_SECRET",
        "GOOGLE_API_KEY", "BRAVE_SEARCH_API_KEY",
        "GMAIL_ADDRESS", "GMAIL_APP_PASSWORD", "RECIPIENT_EMAIL",
    ]
    issues = []
    for var in required:
        val = os.getenv(var, "")
        if not val or val in ("FILL_IN", "your_api_key_here"):
            issues.append(f"{var} is not set")
    return issues


def check_brave() -> str | None:
    """Quick Brave Search API validation."""
    key = os.getenv("BRAVE_SEARCH_API_KEY", "")
    if not key or key == "FILL_IN":
        return "BRAVE_SEARCH_API_KEY not set"
    try:
        with httpx.Client(timeout=10) as c:
            r = c.get(
                "https://api.search.brave.com/res/v1/web/search",
                params={"q": "test", "count": 1},
                headers={"X-Subscription-Token": key, "Accept": "application/json"},
            )
        if r.status_code == 401:
            return "Brave Search API key invalid (401)"
        if r.status_code == 429:
            return "Brave Search quota exceeded (429) — check usage"
        return None
    except Exception as e:
        return f"Brave Search unreachable: {e}"


def check_gemini() -> str | None:
    """Quick Gemini API validation."""
    key = os.getenv("GOOGLE_API_KEY", "")
    if not key or key == "FILL_IN":
        return "GOOGLE_API_KEY not set"
    try:
        from google import genai
        client = genai.Client(api_key=key)
        resp = client.models.generate_content(
            model="gemini-2.5-flash",
            contents="Reply with just: OK",
        )
        if resp.text:
            return None
        return "Gemini returned empty response"
    except Exception as e:
        return f"Gemini error: {str(e)[:100]}"


def check_gmail() -> str | None:
    """Quick Gmail SMTP validation."""
    addr = os.getenv("GMAIL_ADDRESS", "")
    pwd = os.getenv("GMAIL_APP_PASSWORD", "")
    if not addr or not pwd or pwd == "FILL_IN":
        return "Gmail credentials not set"
    try:
        ctx = ssl.create_default_context()
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=10) as s:
            s.starttls(context=ctx)
            s.login(addr, pwd)
        return None
    except smtplib.SMTPAuthenticationError:
        return "Gmail auth failed — check App Password"
    except Exception as e:
        return f"Gmail SMTP error: {e}"


def send_warning_email(issues: list[str]):
    """Send a warning email listing the issues found."""
    addr = os.getenv("GMAIL_ADDRESS", "")
    pwd = os.getenv("GMAIL_APP_PASSWORD", "")
    recipient = os.getenv("RECIPIENT_EMAIL", addr)

    if not addr or not pwd:
        logger.error("Cannot send warning email — Gmail credentials missing")
        return

    body = f"""
Subject: ⚠️ Holdings Report Agent — Pre-Run Issues Detected

WARNING: The pre-run check found {len(issues)} issue(s) at {datetime.now().strftime('%H:%M IST')}.

The main pipeline runs at 18:00 IST. Please fix these now:

""" + "\n".join(f"  ✗ {issue}" for issue in issues) + """

How to fix:
  Edit your .env file with the correct values,
  then re-run: python scripts/test_run.py --mock

If issues persist, the email will still be sent but may be incomplete.
"""
    try:
        ctx = ssl.create_default_context()
        with smtplib.SMTP("smtp.gmail.com", 587) as s:
            s.starttls(context=ctx)
            s.login(addr, pwd)
            s.sendmail(addr, recipient, body)
        logger.info("Warning email sent to %s", recipient)
    except Exception as e:
        logger.error("Could not send warning email: %s", e)


def main():
    logger.info("Pre-run check starting at %s", datetime.now().strftime("%Y-%m-%d %H:%M"))
    all_issues = []

    # 1. Environment variables
    env_issues = check_env_vars()
    if env_issues:
        all_issues.extend(env_issues)
        logger.warning("ENV issues: %s", env_issues)
    else:
        logger.info("✓ All environment variables set")

    # 2. Brave Search
    brave_issue = check_brave()
    if brave_issue:
        all_issues.append(brave_issue)
        logger.warning("✗ Brave: %s", brave_issue)
    else:
        logger.info("✓ Brave Search API OK")

    # 3. Gemini
    gemini_issue = check_gemini()
    if gemini_issue:
        all_issues.append(gemini_issue)
        logger.warning("✗ Gemini: %s", gemini_issue)
    else:
        logger.info("✓ Gemini API OK")

    # 4. Gmail
    gmail_issue = check_gmail()
    if gmail_issue:
        all_issues.append(gmail_issue)
        logger.warning("✗ Gmail: %s", gmail_issue)
    else:
        logger.info("✓ Gmail SMTP OK")

    if all_issues:
        logger.warning("Pre-run check found %d issue(s) — sending warning email", len(all_issues))
        send_warning_email(all_issues)
        sys.exit(1)
    else:
        logger.info("Pre-run check passed — pipeline is ready for 18:00 IST run")
        sys.exit(0)


if __name__ == "__main__":
    main()
