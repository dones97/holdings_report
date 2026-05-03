import sys
import os
import json
import warnings
from contextlib import redirect_stdout, redirect_stderr

VALUATION_DIR = r"C:\Users\dones\OneDrive\Documents\Investments\Valuation"

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
        
        return pred

    except Exception as e:
        return {"error": str(e)}
    finally:
        if VALUATION_DIR in sys.path:
            sys.path.remove(VALUATION_DIR)
        os.chdir(old_cwd)

if __name__ == "__main__":
    print(json.dumps(get_valuation("RELIANCE.NS")))
