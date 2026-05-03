import sys
import os
import json
import warnings
from datetime import datetime, timedelta
from contextlib import redirect_stdout, redirect_stderr

if "GITHUB_WORKSPACE" in os.environ:
    base_dir = os.environ["GITHUB_WORKSPACE"]
    FACTOR_DIR = os.path.join(base_dir, "factoranalysis")
    VALUATION_DIR = os.path.join(base_dir, "Valuation")
else:
    FACTOR_DIR = r"C:\Users\dones\OneDrive\Documents\Investments\factoranalysis"
    VALUATION_DIR = r"C:\Users\dones\OneDrive\Documents\Investments\Valuation"

class DummyContextManager:
    def __enter__(self): return self
    def __exit__(self, exc_type, exc_value, traceback): pass

class DummyStreamlit(DummyContextManager):
    def __init__(self):
        self.session_state = {"current_rf": 6.5}
    def cache_data(self, *args, **kwargs):
        def decorator(func=None):
            return func if func else lambda f: f
        if len(args) == 1 and callable(args[0]): return args[0]
        return decorator
    def set_page_config(self, *args, **kwargs): pass
    def title(self, *args, **kwargs): pass
    def header(self, *args, **kwargs): pass
    def subheader(self, *args, **kwargs): pass
    def text_input(self, *args, **kwargs): return ""
    def number_input(self, *args, **kwargs): return 6.5
    def date_input(self, *args, **kwargs): return None
    def markdown(self, *args, **kwargs): pass
    def plotly_chart(self, *args, **kwargs): pass
    def error(self, *args, **kwargs): pass
    def success(self, *args, **kwargs): pass
    def warning(self, *args, **kwargs): pass
    def info(self, *args, **kwargs): pass
    def write(self, *args, **kwargs): pass
    def dataframe(self, *args, **kwargs): pass
    @property
    def sidebar(self): return self
    def tabs(self, *args, **kwargs): return [self, self]
    def columns(self, *args, **kwargs): return [self]*args[0] if len(args)>0 else [self,self]
    def file_uploader(self, *args, **kwargs): return None
    def button(self, *args, **kwargs): return False
    def spinner(self, *args, **kwargs): return self
    def stop(self): sys.exit()
    def download_button(self, *args, **kwargs): pass

def get_factors(ticker):
    old_cwd = os.getcwd()
    try:
        os.chdir(FACTOR_DIR)
        sys.path.insert(0, FACTOR_DIR)
        
        if 'streamlit' not in sys.modules:
            sys.modules['streamlit'] = DummyStreamlit()
            
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            import portfolio_analyzer_0428_fixed as pa
            today = datetime.now()
            sd = today - timedelta(days=365*10)
            ed = today
            ff = pa.fetch_ff_factors(sd, ed)
            wr = pa.weekly_returns(ticker, sd, ed)
            metrics = pa.compute_factor_metrics_for_stock(ticker, sd, ed, ff)
                    
        if metrics:
            return {
                "Exp_Annual_Rtn": metrics.get("Exp_Annual_Rtn"),
                "Annual_Std": metrics.get("Annual_Std"),
                "Sharpe": metrics.get("Sharpe")
            }
        return {}
    except Exception as e:
        return {"error": str(e)}
    finally:
        if FACTOR_DIR in sys.path: sys.path.remove(FACTOR_DIR)
        os.chdir(old_cwd)

def get_valuation(ticker):
    old_cwd = os.getcwd()
    try:
        os.chdir(VALUATION_DIR)
        sys.path.insert(0, VALUATION_DIR)

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            with open(os.devnull, 'w') as devnull:
                with redirect_stdout(devnull), redirect_stderr(devnull):
                    import pe_prediction_model as pe_mod
                    model = pe_mod.PEPredictionModel('indian_stocks_tickers.csv')
                    model.load_model('pe_prediction_model.pkl')
                    pred = model.predict_pe(ticker)
        
        if pred:
            return {
                "predicted_pe": pred.get("predicted_pe"),
                "current_pe": pred.get("current_pe"),
                "upside_downside_pct": pred.get("upside_downside_pct")
            }
        return {}
    except Exception as e:
        return {"error": str(e)}
    finally:
        if VALUATION_DIR in sys.path: sys.path.remove(VALUATION_DIR)
        os.chdir(old_cwd)

def enrich_holdings(holdings: list[dict]) -> list[dict]:
    """
    Given a list of holding dicts, fetches quantitative metrics 
    from the local Valuation and FactorAnalysis modules.
    """
    for h in holdings:
        ticker = h.get("symbol", "")
        if not ticker: continue
        
        # Suffix handling
        t_ns = ticker if ticker.endswith(".NS") or ticker.endswith(".BO") else f"{ticker}.NS"
        
        fac = get_factors(t_ns)
        val = get_valuation(t_ns)
        
        if fac.get("Exp_Annual_Rtn") is not None: h["expected_return"] = float(fac.get("Exp_Annual_Rtn"))
        if fac.get("Annual_Std") is not None: h["standard_deviation"] = float(fac.get("Annual_Std"))
        if fac.get("Sharpe") is not None: h["sharpe_ratio"] = float(fac.get("Sharpe"))
        
        if val.get("predicted_pe") is not None: h["predicted_pe"] = float(val.get("predicted_pe"))
        if val.get("current_pe") is not None: h["actual_pe"] = float(val.get("current_pe"))
        if val.get("upside_downside_pct") is not None: h["valuation_upside"] = float(val.get("upside_downside_pct"))
        
    return holdings

if __name__ == "__main__":
    # Test script directly
    h = [{"symbol": "RELIANCE"}]
    print(enrich_holdings(h))
