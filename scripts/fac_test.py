import sys
import os
import json
import warnings
from datetime import datetime, timedelta
from contextlib import redirect_stdout, redirect_stderr

FACTOR_DIR = r"C:\Users\dones\OneDrive\Documents\Investments\factoranalysis"

class DummyStreamlit:
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
    def sidebar(self, *args, **kwargs): return self
    def tabs(self, *args, **kwargs): return [self, self]
    def columns(self, *args, **kwargs): return [self]*args[0] if len(args)>0 else [self,self]
    def file_uploader(self, *args, **kwargs): return None
    def button(self, *args, **kwargs): return False
    def spinner(self, *args, **kwargs): 
        class DummyContext:
            def __enter__(self): pass
            def __exit__(self, *a): pass
        return DummyContext()
    def stop(self): sys.exit()
    def download_button(self, *args, **kwargs): pass

def get_factors(ticker):
    old_cwd = os.getcwd()
    try:
        os.chdir(FACTOR_DIR)
        sys.path.insert(0, FACTOR_DIR)
        
        # Inject dummy streamlit
        if 'streamlit' not in sys.modules:
            sys.modules['streamlit'] = DummyStreamlit()
        
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            with open(os.devnull, 'w') as devnull:
                with redirect_stdout(devnull), redirect_stderr(devnull):
                    import portfolio_analyzer_0428_fixed as pa
                    
                    today = datetime.now()
                    sd = today - timedelta(days=365*10)
                    ed = today
                    
                    ff = pa.fetch_ff_factors(sd, ed)
                    metrics = pa.compute_factor_metrics_for_stock(ticker, sd, ed, ff)
                    
        return metrics

    except Exception as e:
        return {"error": str(e)}
    finally:
        if FACTOR_DIR in sys.path:
            sys.path.remove(FACTOR_DIR)
        os.chdir(old_cwd)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        metrics = get_factors(sys.argv[1])
    else:
        metrics = get_factors("RELIANCE.NS")
    
    if metrics and "error" not in metrics:
        print(json.dumps({
            "Exp_Annual_Rtn": metrics.get("Exp_Annual_Rtn"),
            "Annual_Std": metrics.get("Annual_Std"),
            "Sharpe": metrics.get("Sharpe")
        }))
    else:
        print(json.dumps(metrics or {}))
