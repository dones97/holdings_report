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
MAX_OUTPUT_TOKENS = 16384
RETRY_ATTEMPTS = 3
RETRY_DELAY = 15  # seconds between retries

# ── Prompt Templates ──────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a senior equity research analyst at a Mumbai-based fund, specialising in Indian small-cap and mid-cap stocks (NSE/BSE).
Your job is to produce an insightful, actionable weekly portfolio digest for a retail investor.

Critical guidelines:
- For each stock, combine the PROVIDED news/data with YOUR OWN knowledge about the company, its business model, competitive position, promoter background, and sector dynamics.
- If news_article_count is low (0-3), lean more heavily on your knowledge. Never say "no information available" — always provide useful context.
- Be direct and specific. Avoid generic statements like "the market is uncertain" or "results were in line."
- When has_earnings is true, lead with concrete numbers: revenue, PAT, margins, YoY growth.
- Always use Indian market context: Nifty/Sensex trends, RBI policy, FII/DII flows, rupee movement, commodity prices where relevant.
- For small/mid-cap stocks, mention: promoter holdings, any pledging concerns, liquidity, and business moat if known.
- Signals:
    HOLD CONFIDENTLY   = strong fundamentals, positive momentum, thesis intact
    HOLD WITH CAUTION  = mixed signals, no urgent action but watch carefully
    WATCH CLOSELY      = meaningful risk or uncertainty, monitor this week
    REVIEW POSITION    = significant negative development; re-examine investment thesis

Output ONLY valid JSON. No markdown fences, no explanation outside the JSON."""

USER_PROMPT_TEMPLATE = """Today is {today}. Analyse this Indian equity portfolio and produce a weekly digest.

PORTFOLIO DATA (each holding includes news fetched this week + your own knowledge fills gaps):
{portfolio_json}

SECTOR & MACRO CONTEXT:
{sector_news_json}

INSTRUCTIONS:
1. For holdings with few or no news articles, draw on your knowledge of: the company's business, sector trends, recent quarterly results (if you know them), promoter track record, and any known risks.
2. Cross-reference sector context above with individual holdings in that sector.
3. Every field must be substantive — no placeholder text.
4. flags must be SPECIFIC (e.g., "NALCO's aluminium realisations depend on LME prices — watch for global inventory builds" vs just "commodity risk").

Return exactly this JSON (omit earnings_summary only if has_earnings is false AND you have no earnings knowledge):

{{
  "week_ending": "{today}",
  "portfolio_summary": "3-4 sentences of overall portfolio commentary covering: (1) key theme across holdings this week, (2) macro backdrop relevant to this portfolio mix, (3) any holdings showing outsized moves or risks. Be specific — name stocks and sectors.",
  "holdings": [
    {{
      "symbol": "NSE_SYMBOL",
      "company_name": "Full Company Name",
      "signal": "HOLD CONFIDENTLY",
      "signal_reason": "One crisp, specific sentence explaining this signal — cite a concrete data point.",
      "news_summary": "2-4 sentences: prioritise the supplied news articles. If sparse, use your knowledge of recent developments, management statements, or sector news that would affect this stock. Always be specific.",
      "earnings_summary": "3-4 sentences on the most recent quarterly results: revenue, PAT, EBITDA margin, YoY comparison, and one management commentary point. Include if you have this knowledge even if has_earnings is false in the data.",
      "industry_insight": "2-3 sentences on sector tailwinds or headwinds. Mention specific factors: e.g., for solar — module price trends and domestic vs export mix; for defence — order book visibility and DRDO tech transfer progress.",
      "flags": [
        "Specific, concrete risk or catalyst — mention a number, name, or event where possible",
        "Second flag if applicable"
      ]
    }}
  ]
}}"""


def _build_portfolio_context(holdings: list[dict]) -> str:
    """Build a compact JSON representation of holdings for the prompt."""
    context = []
    for h in holdings:
        news_articles = h.get("news_articles", [])
        entry = {
            "symbol": h["symbol"],
            "company_name": h["company_name"],
            "sector": h.get("sector", "Unknown"),
            "pnl_pct": h.get("pnl_pct", 0),
            "avg_price": h.get("avg_price", 0),
            "current_price": h.get("current_price", 0),
            "has_earnings": h.get("has_earnings", False),
            "earnings_headline": h.get("earnings_headline"),
            "news_article_count": len(news_articles),  # Tells LLM how much data we have
            "recent_news": [
                {
                    "title": a.get("title", ""),
                    "description": a.get("description", ""),
                    "source": a.get("source", ""),
                    "age_days": a.get("freshness_days", 7),
                }
                for a in news_articles[:6]  # Up from 5 to 6
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
                
                # Merge original data back to preserve metrics not generated by LLM
                orig_by_sym = {h.get("symbol"): h for h in holdings}
                for dh in parsed.get("holdings", []):
                    orig = orig_by_sym.get(dh.get("symbol"))
                    if orig:
                        for k, v in orig.items():
                            if k not in dh:
                                dh[k] = v

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
