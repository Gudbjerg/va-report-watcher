# workers/kaxcap-factset/factset_client.py
import os
from pathlib import Path
from typing import Dict, Any

import pandas as pd
from dotenv import load_dotenv

# Optional SDK (when FACTSET_USE_SDK=true), else fall back to requests
USE_SDK = (os.environ.get('FACTSET_USE_SDK', 'false').lower() == 'true')
if USE_SDK:
    import fds.sdk.Formula as formula
    from fds.sdk.Formula.apis import TimeSeriesApi
    from fds.sdk.Formula.models import TimeSeriesRequest, TimeSeriesRequestData
else:
    import requests


# --- ENV LOADING -----------------------------------------------------------

# project root = repo root (…/VA OPAL Scraper)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

FACTSET_USERNAME = os.environ.get("FACTSET_USERNAME_SERIAL", "")
FACTSET_API_KEY = os.environ.get("FACTSET_API_KEY", "")
FACTSET_FORMULA_URL = os.environ.get(
    "FACTSET_FORMULA_URL", "https://api.factset.com/formula-api/v1/time-series")


# --- CLIENT ----------------------------------------------------------------

def _get_client():
    """Create an API client using API key auth (SDK) or None for requests."""
    if not USE_SDK:
        return None
    config = formula.Configuration(
        username=FACTSET_USERNAME, password=FACTSET_API_KEY)
    return formula.ApiClient(config)


# --- FETCH RAW KAXCAP DATA -------------------------------------------------

def _universe_expr(region: str) -> str:
    r = (region or 'CPH').upper()
    # Allow env overrides when needed
    override = os.environ.get(f'FORMULA_UNIVERSE_{r}')
    if override:
        return override
    # Known universe ids/symbols
    if r == 'CPH':
        return "(FG_CONSTITUENTS(187183,0,CLOSE))=1"  # Copenhagen All-Share
    if r == 'HEL':
        return "(FG_CONSTITUENTS(180553,0,CLOSE))=1"  # Helsinki All-Share
    if r == 'STO':
        # Use ID or symbol; allow override via env
        return "(FG_CONSTITUENTS(OMXSALLS,0,CLOSE))=1" # Stockholm All-Share
    # Fallback to Copenhagen
    return "(FG_CONSTITUENTS(187183,0,CLOSE))=1"


def _build_formulas(region: str) -> Dict[str, Any]:
    """Return formulas list tuned per region (DKK/EUR/SEK for OMX shares)."""
    r = (region or 'CPH').upper()
    # Currency for EXG_OMX_SHARES based on market
    ccy = 'DKK' if r == 'CPH' else ('EUR' if r == 'HEL' else 'SEK')
    return {
        "formulas": [
            'FSYM_TICKER_EXCHANGE(0,"ID")',
            'FG_COMPANY_NAME',
            'FG_PRICE(NOW)',
            f'EXG_OMX_SHARES(NOW,,,"OMXCALLS","PI","{ccy}","ND")',
            'P_VOLUME_AVG(30)'
        ],
        "flatten": "Y"
    }


def fetch_kaxcap_raw(region: str = 'CPH') -> pd.DataFrame:
    """Fetch raw constituents with ticker, issuer, price, shares, avg_30d_volume, market cap, and region."""
    universe = _universe_expr(region)
    f = _build_formulas(region)

    if USE_SDK:
        with _get_client() as api_client:
            api = TimeSeriesApi(api_client)
            req = TimeSeriesRequest(data=TimeSeriesRequestData(
                universe=universe, formulas=f["formulas"], flatten=f["flatten"]))
            resp = api.get_time_series_data_for_list(req).get_response_200()
            raw = resp.to_dict()["data"]
    else:
        headers = {'Accept': 'application/json',
                   'Content-Type': 'application/json'}
        payload = {"data": {"universe": universe,
                            "formulas": f["formulas"], "flatten": f["flatten"]}}
        r = requests.post(FACTSET_FORMULA_URL, auth=(
            FACTSET_USERNAME, FACTSET_API_KEY), headers=headers, json=payload, verify=True)
        r.raise_for_status()
        raw = r.json().get('data', [])

    df = pd.DataFrame(raw)
    # Debug columns while aligning names
    print("[formula] columns:", df.columns.tolist())
    # Try common output column keys; adjust if your API returns different names
    rename_map = {
        "fsym_ticker_exchange_0_id_": "ticker",
        "fg_company_name": "issuer",
        "fg_price_now_": "price",
        # region-specific currency in name; we match by prefix
    }
    # Shares and volume often come with verbose keys; find best matches
    shares_col = next((c for c in df.columns if c.startswith(
        'exg_omx_shares_now_') and 'omxcalls' in c.lower()), None)
    vol_col = next(
        (c for c in df.columns if c.startswith('p_volume_avg_30')), None)
    if shares_col:
        rename_map[shares_col] = 'shares'
    if vol_col:
        rename_map[vol_col] = 'avg_30d_volume'
    df = df.rename(columns=rename_map)

    # Compute market cap when possible (price * shares)
    if 'price' in df.columns and 'shares' in df.columns:
        try:
            df['mcap'] = pd.to_numeric(
                df['price'], errors='coerce') * pd.to_numeric(df['shares'], errors='coerce')
        except Exception:
            df['mcap'] = None

    # Inject region
    df['region'] = (region or 'CPH').upper()

    cols = ['ticker', 'issuer', 'price', 'shares',
            'avg_30d_volume', 'mcap', 'region']
    for c in cols:
        if c not in df.columns:
            df[c] = None
    return df[cols]
