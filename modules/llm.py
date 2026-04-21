"""
llm.py — LLM Synthesis Module (Google Gemini)

Accepts enriched holdings data (with news + earnings flags) and returns
a structured JSON digest suitable for rendering into an HTML email.

Uses Gemini 2.5 Flash (free tier) via the google-genai SDK.
Falls back to raw text if JSON parsing fails.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from google import genai
from google.genai import types
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

# ── Model Configuration ───────────────────────────────────────────────────────
MODEL_NAME = "gemini-2.5-flash"
TEMPERATURE = 0.3
MAX_OUTPUT_TOKENS = 8192
RETRY_ATTEMPTS = 3
RETRY_DELAY = 15  # seconds between retries

# ── Prompt Templates ──────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a senior equity research analyst specialising in Indian stock markets (NSE/BSE).
Your job is to review a portfolio of Indian stocks and produce a concise, insightful weekly digest.

Guidelines:
- Be direct and factual. Avoid generic statements like "the market is uncertain."
- Ground every observation in the data provided (news, earnings, sector signals).
- When has_earnings is true, lead with an earnings breakdown before news.
- Use Indian market context (Nifty, Sensex, RBI, SEBI, FII/DII flows where relevant).
- Signals must be one of: HOLD CONFIDENTLY | HOLD WITH CAUTION | WATCH CLOSELY | REVIEW POSITION
- HOLD CONFIDENTLY: strong fundamentals, positive momentum, no red flags
- HOLD WITH CAUTION: mixed signals, wait-and-watch, no urgent action needed
- WATCH CLOSELY: meaningful risk or uncertainty, monitor closely this week
- REVIEW POSITION: significant negative development; consider whether thesis still holds

Output ONLY valid JSON matching the schema exactly. No markdown, no explanation, just the JSON object."""

USER_PROMPT_TEMPLATE = """Today is {today}. Analyse the following Indian equity portfolio.

PORTFOLIO DATA:
{portfolio_json}

SECTOR NEWS CONTEXT:
{sector_news_json}

Return exactly this JSON structure (fill all fields, omit earnings_summary only if has_earnings is false):

{{
  "week_ending": "{today}",
  "portfolio_summary": "2-3 sentences of overall portfolio commentary. Be specific — mention sectors, macro themes, or key events from the data.",
  "holdings": [
    {{
      "symbol": "NSE_SYMBOL",
      "company_name": "Full Company Name",
      "signal": "HOLD CONFIDENTLY",
      "signal_reason": "One crisp sentence explaining why this signal.",
      "news_summary": "2-3 sentences covering the most important news this week. Be specific.",
      "earnings_summary": "3-4 sentences on the quarterly results if has_earnings is true. Cover revenue, PAT, margins, management commentary. Omit this field entirely if has_earnings is false.",
      "industry_insight": "1-2 sentences on sector tailwinds or headwinds relevant to this holding.",
      "flags": ["Specific risk or catalyst to watch — be concrete, not generic"]
    }}
  ]
}}"""


def _build_portfolio_context(holdings: list[dict]) -> str:
    """Build a compact JSON representation of holdings for the prompt."""
    context = []
    for h in holdings:
        entry = {
            "symbol": h["symbol"],
            "company_name": h["company_name"],
            "sector": h.get("sector", "Unknown"),
            "pnl_pct": h.get("pnl_pct", 0),
            "has_earnings": h.get("has_earnings", False),
            "earnings_headline": h.get("earnings_headline"),
            "recent_news": [
                {
                    "title": a.get("title", ""),
                    "description": a.get("description", ""),
                    "source": a.get("source", ""),
                }
                for a in h.get("news_articles", [])[:5]  # Cap at 5 articles per holding
            ],
        }
        context.append(entry)
    return json.dumps(context, indent=2, ensure_ascii=False)


def _build_sector_context(sector_news: dict[str, list[dict]]) -> str:
    """Build a compact JSON representation of sector news."""
    context = {}
    for sector, articles in sector_news.items():
        context[sector] = [
            {
                "title": a.get("title", ""),
                "description": a.get("description", ""),
            }
            for a in articles[:3]
        ]
    return json.dumps(context, indent=2, ensure_ascii=False)


def _call_gemini(prompt_text: str) -> str:
    """Make a single Gemini API call. Returns raw text response."""
    api_key = os.getenv("GOOGLE_API_KEY", "")
    if not api_key or api_key == "FILL_IN":
        raise EnvironmentError(
            "GOOGLE_API_KEY not set in .env. "
            "Get a free key at https://aistudio.google.com"
        )

    client = genai.Client(api_key=api_key)

    response = client.models.generate_content(
        model=MODEL_NAME,
        contents=prompt_text,
        config=types.GenerateContentConfig(
            system_instruction=SYSTEM_PROMPT,
            temperature=TEMPERATURE,
            max_output_tokens=MAX_OUTPUT_TOKENS,
            response_mime_type="application/json",  # Force JSON output
        ),
    )
    return response.text


def _parse_digest(raw_text: str) -> dict | None:
    """
    Parse LLM JSON response. Returns dict on success, None on failure.
    Attempts to strip markdown code fences if present.
    """
    text = raw_text.strip()

    # Strip markdown fences if model ignored the instruction
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(
            line for line in lines
            if not line.startswith("```")
        ).strip()

    try:
        parsed = json.loads(text)
        # Validate required top-level keys
        required = {"week_ending", "portfolio_summary", "holdings"}
        if not required.issubset(parsed.keys()):
            logger.warning("LLM response missing keys: %s", required - set(parsed.keys()))
            return None
        return parsed
    except json.JSONDecodeError as e:
        logger.warning("JSON parse failed: %s", str(e))
        return None


def synthesise(
    holdings: list[dict],
    sector_news: dict[str, list[dict]],
) -> dict:
    """
    Main entry point.

    Returns a structured digest dict ready for email rendering.
    On LLM failure, returns a minimal dict with an error flag so the
    email can still be sent with a warning banner.
    """
    today = datetime.now().strftime("%Y-%m-%d")

    portfolio_json = _build_portfolio_context(holdings)
    sector_json = _build_sector_context(sector_news)

    prompt = USER_PROMPT_TEMPLATE.format(
        today=today,
        portfolio_json=portfolio_json,
        sector_news_json=sector_json,
    )

    logger.info("LLM: sending prompt to %s (~%d chars)", MODEL_NAME, len(prompt))

    raw_response = None
    parsed = None
    last_error = None

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            raw_response = _call_gemini(prompt)
            logger.info("LLM: received response (~%d chars)", len(raw_response or ""))
            parsed = _parse_digest(raw_response)

            if parsed:
                logger.info(
                    "LLM: digest synthesised for %d holdings", len(parsed.get("holdings", []))
                )
                parsed["_raw_llm_response"] = raw_response
                parsed["_llm_model"] = MODEL_NAME
                parsed["_llm_ok"] = True
                return parsed

            # JSON failed — try repair on retry
            logger.warning("LLM attempt %d: JSON parse failed, retrying...", attempt)
            time.sleep(RETRY_DELAY)

        except Exception as e:
            last_error = str(e)
            logger.error("LLM attempt %d error: %s", attempt, last_error)
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_DELAY * attempt)

    # All attempts failed — return degraded digest with raw text
    logger.error("LLM synthesis failed after %d attempts. Including raw response.", RETRY_ATTEMPTS)
    return {
        "week_ending": today,
        "portfolio_summary": "LLM synthesis unavailable this week. See raw output below.",
        "holdings": [],
        "_raw_llm_response": raw_response or f"Error: {last_error}",
        "_llm_ok": False,
        "_llm_model": MODEL_NAME,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Test with mock data
    test_holdings = [
        {
            "symbol": "TCS",
            "company_name": "Tata Consultancy Services Ltd",
            "sector": "Information Technology",
            "pnl_pct": 12.5,
            "has_earnings": True,
            "earnings_headline": "TCS Q3 FY25 Results: PAT up 5.5% YoY",
            "news_articles": [
                {
                    "title": "TCS wins $250M deal from European banking client",
                    "description": "TCS signed a major transformation deal...",
                    "source": "Economic Times",
                },
            ],
        }
    ]
    test_sector_news = {
        "Information Technology": [
            {
                "title": "Indian IT sector faces headwinds from US slowdown",
                "description": "NASSCOM revised growth forecast downward...",
            }
        ]
    }

    result = synthesise(test_holdings, test_sector_news)
    print(json.dumps(result, indent=2, default=str))
