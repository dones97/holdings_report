"""
returns.py — Performance & Returns Module

Calculates exact 7-day weekly returns for the portfolio using yfinance,
comparing current values with values ~7 days ago, and returns the NIFTY 50 benchmark.
"""

import logging
import math
import yfinance as yf
import pandas as pd

logger = logging.getLogger(__name__)

def get_weekly_returns(holdings: list[dict]) -> dict:
    """
    Given a list of holdings with at least 'symbol', 'quantity', and 'current_price',
    returns a dictionary defining:
        - portfolio_val_current
        - weekly_abs_gain
        - weekly_pct_gain
        - nifty_pct_gain
    """
    if not holdings:
        return {}

    symbols = [f"{h['symbol']}.NS" for h in holdings]
    all_tickers = symbols + ["^NSEI"]
    
    logger.info("Returns: fetching 7-day historical prices for %d tickers + NIFTY 50", len(symbols))
    
    try:
        hist = yf.download(all_tickers, period="8d", progress=False)
        
        # Handle the returned DataFrame robustly
        if isinstance(hist.columns, pd.MultiIndex):
            if "Close" in hist.columns:
                close_prices = hist["Close"]
            elif "Close" in hist.columns.levels[0]:
                close_prices = hist.xs("Close", level=0, axis=1)
            else:
                close_prices = hist
        else:
            if "Close" in hist.columns:
                close_prices = hist[["Close"]]
            else:
                close_prices = hist

        if close_prices.empty or len(close_prices) < 2:
            raise ValueError("Empty or insufficient history from yfinance")

        # oldest is about 7 calendar days ago
        old_prices = close_prices.iloc[0].to_dict()
        new_prices = close_prices.iloc[-1].to_dict()
        
        total_val_old = 0.0
        total_val_new = 0.0
        
        for h in holdings:
            sym = f"{h['symbol']}.NS"
            qty = h.get("quantity", 0)
            n_p = float(h.get("current_price", 0))
            
            # yfinance returns can sometimes have tuples or directly the value
            # Since yf handles MultiIndex, .to_dict() can yield tuple keys or primitive keys
            # Let's cleanly fetch
            o_p = old_prices.get(sym)
            if isinstance(o_p, pd.Series):
                o_p = o_p.iloc[0] # just in case
                
            if o_p is None or math.isnan(float(o_p)):
                o_p = n_p # Assume constant if data is missing
            else:
                o_p = float(o_p)
                
            total_val_old += qty * o_p
            total_val_new += qty * n_p
            
        abs_gain = total_val_new - total_val_old
        pct_gain = (abs_gain / total_val_old * 100) if total_val_old > 0 else 0.0
        
        nifty_o = old_prices.get("^NSEI")
        nifty_n = new_prices.get("^NSEI")
        
        if isinstance(nifty_o, pd.Series): nifty_o = nifty_o.iloc[0]
        if isinstance(nifty_n, pd.Series): nifty_n = nifty_n.iloc[0]
            
        nifty_pct = 0.0
        if nifty_o is not None and not math.isnan(float(nifty_o)) and float(nifty_o) > 0:
            if nifty_n is None or math.isnan(float(nifty_n)):
                nifty_n = nifty_o
            nifty_pct = ((float(nifty_n) - float(nifty_o)) / float(nifty_o)) * 100
            
        logger.info("Returns: Portfolio Gain = %.2f%% | NIFTY Gain = %.2f%%", pct_gain, nifty_pct)
        
        return {
            "portfolio_val": round(total_val_new, 2),
            "weekly_abs_gain": round(abs_gain, 2),
            "weekly_pct_gain": round(pct_gain, 2),
            "nifty_pct_gain": round(nifty_pct, 2)
        }
    except Exception as e:
        logger.warning("Failed to compute weekly returns: %s", str(e))
        return {}
