"""
news.py — News & Industry Enrichment Module

Fetches recent news for each holding and sector using Brave Search API.

Per holding:
  Query 1: "<company> NSE stock news"
  Query 2: "<company> analyst recommendation outlook"

Per unique sector:
  Query: "Indian <sector> sector week outlook"

All results are filtered to the past 7 days.
Saves news_<SYMBOL>.json to data/news/.
"""

import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import httpx
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent
NEWS_DIR = BASE_DIR / "data" / "news"

BRAVE_SEARCH_URL = "https://api.search.brave.com/res/v1/web/search"
ARTICLES_PER_QUERY = 3
MAX_TOTAL_QUERIES = 60  # Safety cap per run


def _brave_search(query: str, api_key: str, count: int = 3) -> list[dict]:
    """
    Execute one Brave Search query. Returns a list of article dicts.
    Each article: {title, url, description, published_date}
    """
    from_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    to_date = datetime.now().strftime("%Y-%m-%d")

    try:
        with httpx.Client(timeout=15.0) as client:
            resp = client.get(
                BRAVE_SEARCH_URL,
                params={
                    "q": query,
                    "count": count,
                    "freshness": f"{from_date}to{to_date}",
                    "search_lang": "en",
                    "country": "IN",
                    "text_decorations": False,
                },
                headers={
                    "Accept": "application/json",
                    "Accept-Encoding": "gzip",
                    "X-Subscription-Token": api_key,
                },
            )
            resp.raise_for_status()

        results = resp.json().get("web", {}).get("results", [])
        articles = []
        for r in results:
            articles.append({
                "title": r.get("title", ""),
                "url": r.get("url", ""),
                "description": r.get("description", ""),
                "published_date": r.get("page_age", ""),
                "source": r.get("profile", {}).get("name", ""),
            })
        return articles

    except httpx.HTTPStatusError as e:
        logger.warning("Brave Search HTTP %s for query: %s", e.response.status_code, query[:60])
        return []
    except Exception as e:
        logger.warning("Brave Search error for query '%s': %s", query[:60], str(e))
        return []


def _fetch_holding_news(holding: dict, api_key: str) -> dict:
    """Fetch news articles for a single holding. Returns the holding with news_articles populated."""
    company = holding.get("company_name", holding["symbol"])
    symbol = holding["symbol"]

    queries = [
        f'"{company}" NSE stock news India',
        f'"{company}" analyst recommendation outlook India',
    ]

    all_articles = []
    seen_urls = set()

    for query in queries:
        articles = _brave_search(query, api_key, count=ARTICLES_PER_QUERY)
        for article in articles:
            url = article.get("url", "")
            if url and url not in seen_urls:
                seen_urls.add(url)
                all_articles.append(article)
        time.sleep(0.3)  # Gentle rate limiting

    holding["news_articles"] = all_articles
    logger.info("%s: fetched %d news articles", symbol, len(all_articles))

    # Save to disk
    NEWS_DIR.mkdir(parents=True, exist_ok=True)
    news_file = NEWS_DIR / f"{symbol}.json"
    with open(news_file, "w", encoding="utf-8") as f:
        json.dump(
            {
                "symbol": symbol,
                "company_name": company,
                "fetched_at": datetime.utcnow().isoformat() + "Z",
                "articles": all_articles,
            },
            f,
            indent=2,
            ensure_ascii=False,
        )

    return holding


def _fetch_sector_news(sector: str, api_key: str) -> list[dict]:
    """Fetch broader sector news for one unique sector."""
    query = f"Indian {sector} sector week outlook NSE 2024"
    articles = _brave_search(query, api_key, count=3)
    logger.info("Sector '%s': fetched %d articles", sector, len(articles))
    return articles


def fetch_news(holdings: list[dict]) -> tuple[list[dict], dict[str, list[dict]]]:
    """
    Main entry point.

    Returns:
      holdings     — each holding now has news_articles populated
      sector_news  — dict of {sector_name: [articles]} for unique sectors
    """
    api_key = os.getenv("BRAVE_SEARCH_API_KEY", "")
    if not api_key or api_key == "FILL_IN":
        logger.error(
            "BRAVE_SEARCH_API_KEY not set in .env — news enrichment skipped. "
            "Get a free key at https://api.search.brave.com"
        )
        return holdings, {}

    NEWS_DIR.mkdir(parents=True, exist_ok=True)
    query_count = 0

    # ── Per-holding news (parallel, up to 5 workers) ─────────────────────────
    enriched = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(_fetch_holding_news, holding, api_key): holding
            for holding in holdings
            if query_count < MAX_TOTAL_QUERIES
        }
        for future in as_completed(futures):
            try:
                result = future.result()
                enriched.append(result)
                query_count += 2  # 2 queries per holding
            except Exception as e:
                original = futures[future]
                logger.error(
                    "News fetch failed for %s: %s", original.get("symbol", "?"), str(e)
                )
                original["news_articles"] = []
                enriched.append(original)

    # Preserve original order
    symbol_order = {h["symbol"]: i for i, h in enumerate(holdings)}
    enriched.sort(key=lambda h: symbol_order.get(h["symbol"], 999))

    # ── Sector news (one query per unique sector) ─────────────────────────────
    unique_sectors = list({h.get("sector", "Unknown") for h in holdings if h.get("sector") != "Unknown"})
    sector_news: dict[str, list[dict]] = {}

    for sector in unique_sectors:
        if query_count >= MAX_TOTAL_QUERIES:
            logger.warning("Query cap reached — skipping remaining sector queries")
            break
        sector_news[sector] = _fetch_sector_news(sector, api_key)
        query_count += 1
        time.sleep(0.3)

    logger.info(
        "News enrichment complete: %d holding queries + %d sector queries = %d total",
        len(holdings) * 2, len(sector_news), query_count
    )
    return enriched, sector_news


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_holdings = [
        {
            "symbol": "TCS",
            "company_name": "Tata Consultancy Services Ltd",
            "sector": "Information Technology",
        }
    ]
    result_holdings, result_sectors = fetch_news(test_holdings)
    for article in result_holdings[0].get("news_articles", []):
        print(f"  • {article['title']}")
    print(f"\nSector articles for IT: {len(result_sectors.get('Information Technology', []))}")
