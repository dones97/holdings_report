"""
news.py — News & Industry Enrichment Module (v2)

Improvements over v1:
  - 5 query types per holding (up from 2): general news, analyst ratings,
    BSE/corporate announcements, industry discovery, broader market context
  - Adaptive freshness fallback: 7d -> 30d -> 90d for sparse stocks
  - Automatic sector discovery for holdings tagged as "Unknown"
  - Per-sector news still runs, but now covers all discovered sectors
  - Saves per-holding JSON to data/news/ for caching/debugging

Free Brave Search tier: 2,000 queries/month = ~50/week easily within budget
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

# Freshness windows tried in order for sparse results
FRESHNESS_WINDOWS = [7, 30, 90]          # days
MIN_ARTICLES_THRESHOLD = 2               # if fewer than this, try wider window
ARTICLES_PER_QUERY = 3
MAX_WORKERS = 4                          # parallel holding fetches


def _brave_search(
    query: str,
    api_key: str,
    count: int = 3,
    days_back: int = 7,
) -> list[dict]:
    """
    Execute one Brave Search query. Returns list of article dicts.
    Each article: {title, url, description, published_date, source}
    """
    from_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
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
                    "extra_snippets": True,         # richer descriptions
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
            # Use extra_snippet for a richer description when available
            desc = (
                r.get("extra_snippets", [""])[0]
                or r.get("description", "")
            )
            articles.append({
                "title": r.get("title", ""),
                "url": r.get("url", ""),
                "description": desc,
                "published_date": r.get("page_age", ""),
                "source": r.get("profile", {}).get("name", ""),
                "freshness_days": days_back,
            })
        return articles

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 429:
            logger.warning("Brave Search rate limit hit — sleeping 10s")
            time.sleep(10)
        else:
            logger.warning("Brave Search HTTP %s: %s", e.response.status_code, query[:60])
        return []
    except Exception as e:
        logger.warning("Brave Search error for '%s': %s", query[:60], str(e)[:80])
        return []


def _search_with_fallback(
    query: str,
    api_key: str,
    count: int = 3,
    label: str = "",
) -> list[dict]:
    """
    Try progressively wider date windows until MIN_ARTICLES_THRESHOLD is met.
    Returns the best results found.
    """
    best = []
    for days in FRESHNESS_WINDOWS:
        articles = _brave_search(query, api_key, count=count, days_back=days)
        if len(articles) >= MIN_ARTICLES_THRESHOLD:
            if days > FRESHNESS_WINDOWS[0]:
                logger.info("  %s: used %d-day window (7-day was sparse)", label, days)
            return articles
        if articles and len(articles) > len(best):
            best = articles
        time.sleep(0.2)
    return best


def _discover_sector(company: str, symbol: str, api_key: str) -> tuple[str, list[dict]]:
    """
    For holdings with sector='Unknown', search for what sector/industry
    this company operates in and return both the sector name and context articles.
    """
    query = f"{company} NSE {symbol} business sector industry overview India"
    articles = _brave_search(query, api_key, count=3, days_back=180)  # 6-month window

    # Extract sector hint from titles/descriptions heuristically
    sector_keywords = {
        "Information Technology": ["IT", "software", "technology", "SaaS", "digital", "tech"],
        "Banking & Financial Services": ["bank", "NBF", "finance", "lending", "insurance", "NBFC"],
        "Automotive": ["auto", "automobile", "vehicle", "car", "EV", "tractor", "automotive"],
        "Chemicals": ["chemical", "specialty chemical", "agrochemical", "fertiliser", "fluorine"],
        "Pharmaceuticals": ["pharma", "drug", "API", "generics", "healthcare", "hospital"],
        "Energy & Power": ["power", "energy", "solar", "renewable", "wind", "electricity"],
        "Infrastructure & Construction": ["infra", "construction", "road", "EPC", "real estate"],
        "Defence & Aerospace": ["defence", "defense", "aerospace", "naval", "shipbuilding", "DRDO"],
        "FMCG": ["FMCG", "consumer goods", "packaged food", "beverages", "distillery", "spirits"],
        "Metals & Mining": ["steel", "aluminium", "aluminum", "copper", "metal", "mining"],
        "Agriculture & Food": ["agro", "agriculture", "seeds", "food", "grain", "edible oil"],
        "Media & Entertainment": ["media", "entertainment", "OTT", "broadcasting", "film"],
        "Logistics & Transport": ["logistics", "transport", "shipping", "freight", "marine", "port"],
        "Capital Goods": ["capital goods", "engineering", "manufacturing", "industrial", "machinery"],
        "Textiles": ["textiles", "garment", "yarn", "fabric", "apparel"],
        "Real Estate": ["real estate", "REIT", "housing", "property", "township"],
    }

    combined_text = " ".join(
        (a.get("title", "") + " " + a.get("description", "")).lower()
        for a in articles
    )

    detected_sector = "Unknown"
    for sector, keywords in sector_keywords.items():
        if any(kw.lower() in combined_text for kw in keywords):
            detected_sector = sector
            break

    return detected_sector, articles


def _build_queries_for_holding(company: str, symbol: str, sector: str) -> list[tuple[str, str]]:
    """
    Build a list of (query_string, label) tuples for a holding.
    Returns 5 queries covering different angles.
    """
    # Shorten company name for cleaner queries
    short_name = company.split(" ")[0] if len(company.split(" ")) > 1 else company

    queries = [
        # 1. General recent news
        (f'"{company}" NSE stock news India', "news"),

        # 2. Analyst ratings & target prices
        (f'"{company}" OR "{symbol}" analyst rating target price India 2025', "analyst"),

        # 3. BSE/NSE filings, results, dividends, corporate actions
        (f'"{company}" BSE NSE results quarterly earnings dividend India', "filings"),

        # 4. Industry & business context (uses shorter name for broader results)
        (f'"{short_name}" business outlook competitors India {sector if sector != "Unknown" else ""}', "sector"),

        # 5. Risk & regulatory context (management changes, regulatory, deals)
        (f'"{company}" acquisition deal expansion risk India', "risk"),
    ]
    return queries


def _fetch_holding_news(holding: dict, api_key: str) -> dict:
    """
    Fetch comprehensive news for a single holding.
    Uses adaptive freshness and 5 query types.
    Updates holding with news_articles, and discovered_sector if was Unknown.
    """
    company = holding.get("company_name", holding["symbol"])
    symbol = holding["symbol"]
    sector = holding.get("sector", "Unknown")

    all_articles: list[dict] = []
    seen_urls: set[str] = set()

    def add_articles(new_articles: list[dict]):
        for article in new_articles:
            url = article.get("url", "")
            if url and url not in seen_urls:
                seen_urls.add(url)
                all_articles.append(article)

    # Step 1: Auto-discover sector if Unknown — gets context articles too
    if sector == "Unknown":
        discovered_sector, discovery_articles = _discover_sector(company, symbol, api_key)
        if discovered_sector != "Unknown":
            holding["sector"] = discovered_sector
            holding["_newly_discovered_sector"] = True
            sector = discovered_sector
            logger.info("%s: discovered sector = %s", symbol, discovered_sector)
        add_articles(discovery_articles)
        time.sleep(0.3)

    # Step 2: Run the 5 targeted queries with adaptive fallback
    queries = _build_queries_for_holding(company, symbol, sector)

    for query, label in queries:
        # Use standard 7-day window first; fallback to 30d/90d if sparse
        articles = _search_with_fallback(query, api_key, count=ARTICLES_PER_QUERY, label=f"{symbol}/{label}")
        add_articles(articles)
        time.sleep(0.25)  # Gentle rate limiting

    holding["news_articles"] = all_articles
    logger.info(
        "%s: %d articles (sector: %s)",
        symbol, len(all_articles), holding.get("sector", "Unknown")
    )

    # Cache to disk
    NEWS_DIR.mkdir(parents=True, exist_ok=True)
    with open(NEWS_DIR / f"{symbol}.json", "w", encoding="utf-8") as f:
        json.dump(
            {
                "symbol": symbol,
                "company_name": company,
                "sector": holding.get("sector", "Unknown"),
                "fetched_at": datetime.utcnow().isoformat() + "Z",
                "article_count": len(all_articles),
                "articles": all_articles,
            },
            f, indent=2, ensure_ascii=False,
        )

    return holding


def _fetch_sector_news(sector: str, api_key: str) -> list[dict]:
    """Fetch broader macro/sector context for one unique sector."""
    # Two angles: outlook and key events
    queries = [
        f"Indian {sector} sector outlook NSE week 2025",
        f"{sector} India sector trends risks FII FDI 2025",
    ]
    articles = []
    seen = set()
    for q in queries:
        for a in _brave_search(q, api_key, count=3, days_back=30):
            url = a.get("url", "")
            if url and url not in seen:
                seen.add(url)
                articles.append(a)
        time.sleep(0.25)

    logger.info("Sector '%s': fetched %d articles", sector, len(articles))
    return articles


def fetch_news(holdings: list[dict]) -> tuple[list[dict], dict[str, list[dict]]]:
    """
    Main entry point.

    Returns:
      holdings    — each holding with news_articles populated (and sector updated
                    if it was previously Unknown)
      sector_news — {sector_name: [articles]} for unique known sectors
    """
    api_key = os.getenv("BRAVE_SEARCH_API_KEY", "")
    if not api_key or api_key == "FILL_IN":
        logger.error(
            "BRAVE_SEARCH_API_KEY not set in .env — news enrichment skipped. "
            "Get a free key at https://api.search.brave.com"
        )
        return holdings, {}

    NEWS_DIR.mkdir(parents=True, exist_ok=True)

    # ── Per-holding news (parallel) ──────────────────────────────────────────
    enriched: list[dict] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_fetch_holding_news, holding, api_key): holding
            for holding in holdings
        }
        for future in as_completed(futures):
            try:
                result = future.result()
                enriched.append(result)
            except Exception as e:
                original = futures[future]
                logger.error("News fetch failed for %s: %s", original.get("symbol", "?"), str(e))
                original["news_articles"] = []
                enriched.append(original)

    # Preserve original order
    symbol_order = {h["symbol"]: i for i, h in enumerate(holdings)}
    enriched.sort(key=lambda h: symbol_order.get(h["symbol"], 999))

    # ── Sector news — now covers all sectors including newly discovered ones ──
    unique_sectors = list({
        h.get("sector", "Unknown")
        for h in enriched
        if h.get("sector", "Unknown") != "Unknown"
    })
    sector_news: dict[str, list[dict]] = {}

    for sector in unique_sectors:
        sector_news[sector] = _fetch_sector_news(sector, api_key)
        time.sleep(0.3)

    total_articles = sum(len(h.get("news_articles", [])) for h in enriched)
    logger.info(
        "News enrichment complete: %d holdings, %d total articles, %d sectors covered",
        len(enriched), total_articles, len(sector_news),
    )

    # ── Auto-persist newly discovered sectors to sector_map.yaml ─────────────
    newly_discovered = [h for h in enriched if h.pop("_newly_discovered_sector", False)]
    if newly_discovered:
        sector_map_path = BASE_DIR / "modules" / "sector_map.yaml"
        try:
            with open(sector_map_path, "a", encoding="utf-8") as f:
                f.write("\n# Auto-discovered by News Module\n")
                for h in newly_discovered:
                    f.write(f"{h['symbol']}:\n")
                    f.write(f"  sector: \"{h['sector']}\"\n")
                    f.write(f"  bse_code: \"\"\n")
                    f.write(f"  company_name: \"{h['company_name']}\"\n\n")
            logger.info("Saved %d newly discovered sectors to sector_map.yaml", len(newly_discovered))
        except Exception as e:
            logger.error("Failed to update sector_map.yaml: %s", e)

    return enriched, sector_news


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_holdings = [
        {
            "symbol": "NATIONALUM",
            "company_name": "NATIONAL ALUMINIUM CO LTD",
            "sector": "Unknown",
        }
    ]
    result_holdings, result_sectors = fetch_news(test_holdings)
    h = result_holdings[0]
    print(f"\nSector discovered: {h.get('sector')}")
    print(f"Articles: {len(h.get('news_articles', []))}")
    for a in h.get("news_articles", []):
        print(f"  [{a.get('freshness_days')}d] {a['title']}")
