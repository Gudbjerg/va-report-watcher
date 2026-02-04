# workers/indexes/factset_client.py
import os
from pathlib import Path
from datetime import datetime
import json
from typing import Dict, Any

import pandas as pd
from dotenv import load_dotenv

USE_SDK = (os.environ.get('FACTSET_USE_SDK', 'false').lower() == 'true')
if USE_SDK:
    import fds.sdk.Formula as formula
    from fds.sdk.Formula.apis import CrossSectionalApi
    from fds.sdk.Formula.models import CrossSectionalRequest, CrossSectionalRequestData
    try:
        from fds.sdk.utils.authentication import ConfidentialClient
    except Exception:
        ConfidentialClient = None  # type: ignore
else:
    import requests

# Ensure requests is available for shares-only GET even when USE_SDK=true
try:
    import requests as _req_mod
except Exception:
    _req_mod = None


PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

FACTSET_USERNAME = os.environ.get("FACTSET_USERNAME_SERIAL", "")
FACTSET_API_KEY = os.environ.get("FACTSET_API_KEY", "")
FACTSET_FORMULA_URL = os.environ.get(
    "FACTSET_FORMULA_URL", "https://api.factset.com/formula-api/v1/cross-sectional")


def _get_client():
    if not USE_SDK:
        return None
    oauth_cfg_path = os.environ.get('FACTSET_OAUTH_CONFIG')
    if oauth_cfg_path and ConfidentialClient is not None and Path(oauth_cfg_path).exists():
        config = formula.Configuration(
            fds_oauth_client=ConfidentialClient(oauth_cfg_path)
        )
    else:
        config = formula.Configuration(
            username=FACTSET_USERNAME, password=FACTSET_API_KEY
        )
    verify_ssl_env = os.environ.get('FACTSET_VERIFY_SSL', 'true').lower()
    if verify_ssl_env in ('0', 'false', 'no'):
        try:
            config.verify_ssl = False
        except Exception:
            pass
    return formula.ApiClient(config)


def _universe_expr(region: str) -> str:
    r = (region or 'CPH').upper()
    override = os.environ.get(f'FORMULA_UNIVERSE_{r}')
    if override:
        return override
    if r == 'CPH':
        # Use capped KAXCAP index id 140476 per formulas builder
        return "(FG_CONSTITUENTS(140476,0,CLOSE))=1"
    if r == 'HEL':
        # Update to capped HEL index id 188676
        return "(FG_CONSTITUENTS(188676,0,CLOSE))=1"
    if r == 'STO':
        return "(FG_CONSTITUENTS(OMXSALLS,0,CLOSE))=1"
    return "(FG_CONSTITUENTS(187183,0,CLOSE))=1"


def _get_rate_header(headers: Dict[str, str], key: str) -> str | None:
    # Accept hyphen or underscore variants, case-insensitive
    lk = key.lower()
    for k, v in headers.items():
        kl = k.lower().replace('_', '-')
        if kl == lk:
            return v
    return None


def _write_rate_snapshot(headers: Dict[str, str]) -> None:
    """Persist a rate snapshot including all response headers and normalized fields.
    Captures both FactSet-specific keys (X-FactSet-Api-RateLimit-*) and common variants
    like X-RateLimit-* and Retry-After. This helps diagnose 429s precisely.
    """
    try:
        # Normalize a copy of headers to plain dict of strings
        hdrs = {str(k): str(v) for k, v in (headers or {}).items()}
        # Preferred FactSet headers
        limit = _get_rate_header(
            headers, 'X-FactSet-Api-RateLimit-Limit') or _get_rate_header(headers, 'X-RateLimit-Limit')
        remaining = _get_rate_header(
            headers, 'X-FactSet-Api-RateLimit-Remaining') or _get_rate_header(headers, 'X-RateLimit-Remaining')
        reset = _get_rate_header(
            headers, 'X-FactSet-Api-RateLimit-Reset') or _get_rate_header(headers, 'X-RateLimit-Reset')
        # Variants for per-second/day, if provided by upstream
        limit_second = _get_rate_header(headers, 'X-RateLimit-Limit-Second')
        remaining_second = _get_rate_header(
            headers, 'X-RateLimit-Remaining-Second')
        limit_day = _get_rate_header(headers, 'X-RateLimit-Limit-Day')
        remaining_day = _get_rate_header(headers, 'X-RateLimit-Remaining-Day')
        retry_after = _get_rate_header(headers, 'Retry-After')
        payload = {
            'ts': datetime.utcnow().isoformat() + 'Z',
            'limit': limit,
            'remaining': remaining,
            'reset': reset,
            'limit_second': limit_second,
            'remaining_second': remaining_second,
            'limit_day': limit_day,
            'remaining_day': remaining_day,
            'retry_after': retry_after,
            'headers': hdrs,
        }
        path = PROJECT_ROOT / 'logs' / 'api_rate.json'
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(payload, f)
    except Exception:
        pass


def _calc_wait_from_headers(headers: Dict[str, str] | None, attempt: int) -> float:
    """Given response headers, estimate a backoff wait in seconds."""
    import time
    if not headers:
        return min(8.0, 1.5 * (2 ** attempt))
    retry_after = _get_rate_header(headers, 'Retry-After')
    if retry_after:
        try:
            v = float(retry_after)
            if v > 0:
                return v
        except Exception:
            pass
    reset = _get_rate_header(
        headers, 'X-FactSet-Api-RateLimit-Reset') or _get_rate_header(headers, 'X-RateLimit-Reset')
    if reset:
        try:
            reset_val = float(reset)
            now = time.time()
            wait_s = max(1.0, reset_val - now)
            return min(wait_s, 30.0)
        except Exception:
            pass
    # Fallback exponential backoff with cap
    return min(8.0, 1.5 * (2 ** attempt))


def _request_with_backoff(method: str, url: str, max_retries: int = 4, **kwargs):
    """HTTP helper that retries on 429 using Retry-After or FactSet rate headers."""
    import time
    attempt = 0
    while True:
        r = requests.request(method.upper(), url, **kwargs)
        # Log basic headers for visibility if present
        try:
            limit = _get_rate_header(
                r.headers, 'X-FactSet-Api-RateLimit-Limit')
            remaining = _get_rate_header(
                r.headers, 'X-FactSet-Api-RateLimit-Remaining')
            reset = _get_rate_header(
                r.headers, 'X-FactSet-Api-RateLimit-Reset')
            if limit or remaining:
                print(
                    f"[rate] limit={limit} remaining={remaining} reset={reset}")
                _write_rate_snapshot(r.headers)
        except Exception:
            pass
        if r.status_code != 429 or attempt >= max_retries:
            return r
        # Determine wait time
        wait_s = _calc_wait_from_headers(r.headers, attempt)
        attempt += 1
        print(
            f"[rate] 429 received — backing off for {wait_s:.2f}s (attempt {attempt}/{max_retries})")
        time.sleep(wait_s)


def _build_formulas(region: str) -> Dict[str, Any]:
    r = (region or 'CPH').upper()
    # Allow runtime overrides via env; fall back to per-region defaults
    defaults = {
        'CPH': {'shares_symbol': 'OMXCALLS', 'capped_symbol': 'OMXCCAPX', 'ccy': 'DKK'},
        'HEL': {'shares_symbol': 'OMXHALLS', 'capped_symbol': 'OMXHCAPX', 'ccy': 'EUR'},
        'STO': {'shares_symbol': 'OMXSALLS', 'capped_symbol': None,         'ccy': 'SEK'},
    }
    base = defaults.get(r, defaults['CPH']).copy()
    # Env overrides (optional): FORMULA_SHARES_SYMBOL_<REG>, FORMULA_CAPPED_SYMBOL_<REG>, FORMULA_CCY_<REG>
    for key, env_key in (
        ('shares_symbol', f'FORMULA_SHARES_SYMBOL_{r}'),
        ('capped_symbol', f'FORMULA_CAPPED_SYMBOL_{r}'),
        ('ccy', f'FORMULA_CCY_{r}'),
    ):
        val = os.environ.get(env_key)
        if val:
            base[key] = val

    shares_symbol = str(base['shares_symbol']).upper()
    capped_symbol = str(base['capped_symbol']).upper(
    ) if base['capped_symbol'] else None
    ccy = str(base['ccy']).upper()

    formulas = [
        'FSYM_TICKER_EXCHANGE(0,"ID")',
        'FG_COMPANY_NAME',
        'FG_PRICE(NOW)',
        f'EXG_OMX_SHARES(0,{shares_symbol},PI,{ccy},ND)',
        # Preferred 30D average volumes (both units) — shares and millions
        'P_VOLUME_AVG(0,-1/0/0,0)',   # actual shares (ones)
        'P_VOLUME_AVG(0,-1/0/0)',     # in millions
        # Fallback: latest daily volume in shares
        'P_VOLUME(0)'
    ]
    # Include capped shares if available
    if capped_symbol:
        formulas.insert(4, f'EXG_OMX_SHARES(0,{capped_symbol},PI,{ccy},ND)')

    return {
        'formulas': formulas,
        'flatten': 'Y',
        '_shares_symbol': shares_symbol,
        '_capped_symbol': capped_symbol,
        '_ccy': ccy,
    }


def fetch_index_raw(region: str = 'CPH') -> pd.DataFrame:
    universe = _universe_expr(region)
    f = _build_formulas(region)
    # Ensure column list is always defined, even if any branch earlier fails
    cols_default = ['ticker', 'issuer', 'name', 'price', 'shares',
                    'shares_capped', 'avg_vol_30d_millions', 'mcap', 'region']

    if USE_SDK:
        # Use SDK with header capture and 429-aware backoff
        import time
        with _get_client() as api_client:
            api = CrossSectionalApi(api_client)
            req = CrossSectionalRequest(
                data=CrossSectionalRequestData(
                    universe=universe,
                    universe_exclusion=["NONEQUITY"],
                    formulas=f["formulas"],
                    flatten=f["flatten"]
                )
            )
            max_retries = 4
            attempt = 0
            raw = []
            while True:
                try:
                    # Prefer with_http_info to access headers
                    call_with_info = getattr(
                        api, 'get_cross_sectional_data_for_list_with_http_info', None)
                    if callable(call_with_info):
                        data_obj, status_code, headers = call_with_info(req)
                        try:
                            _write_rate_snapshot(headers or {})
                        except Exception:
                            pass
                        raw = (data_obj.to_dict() or {}).get('data', [])
                    else:
                        resp_obj = api.get_cross_sectional_data_for_list(
                            req).get_response_200()
                        # Try best-effort header retrieval from client internals
                        try:
                            last = getattr(api_client, 'last_response', None)
                            if last and getattr(last, 'headers', None):
                                _write_rate_snapshot(last.headers)
                        except Exception:
                            pass
                        raw = (resp_obj.to_dict() or {}).get('data', [])
                    break
                except Exception as e:
                    # Handle 429 with backoff using headers from exception when available
                    status = getattr(e, 'status', None) or getattr(
                        e, 'status_code', None)
                    headers = getattr(e, 'headers', None) or {}
                    if status == 429 and attempt < max_retries:
                        try:
                            _write_rate_snapshot(headers)
                        except Exception:
                            pass
                        wait_s = _calc_wait_from_headers(headers, attempt)
                        attempt += 1
                        print(
                            f"[rate] SDK 429 — backing off {wait_s:.2f}s (attempt {attempt}/{max_retries})")
                        time.sleep(wait_s)
                        continue
                    raise
    else:
        headers = {'Accept': 'application/json',
                   'Content-Type': 'application/json'}
        http_method = os.environ.get('FACTSET_HTTP_METHOD', 'POST').upper()
        verify_ssl = os.environ.get(
            'FACTSET_VERIFY_SSL', 'true').lower() not in ('0', 'false', 'no')
        if http_method == 'GET':
            import urllib.parse as _up
            params = {
                'universe': universe,
                'formulas': ','.join(f['formulas']),
                'flatten': f['flatten'],
                'universeExclusion': 'NONEQUITY',
            }
            url = FACTSET_FORMULA_URL + '?' + _up.urlencode(params, safe=",()")
            r = _request_with_backoff('GET', url, auth=(
                FACTSET_USERNAME, FACTSET_API_KEY), headers=headers, verify=verify_ssl)
        else:
            payload = {"data": {"universe": universe,
                                "formulas": f["formulas"], "flatten": f["flatten"],
                                "universeExclusion": ["NONEQUITY"]}}
            r = _request_with_backoff('POST', FACTSET_FORMULA_URL, auth=(
                FACTSET_USERNAME, FACTSET_API_KEY), headers=headers, json=payload, verify=verify_ssl)
        print("[formula] status:", r.status_code)
        if not r.ok:
            print("[formula] body:", r.text[:800])
        r.raise_for_status()
        raw = r.json().get('data', [])

    df = pd.DataFrame(raw)
    print("[formula] columns:", df.columns.tolist())

    rename_map: Dict[str, str] = {
        'FSYM_TICKER_EXCHANGE(0,"ID")': 'ticker',
        'FG_COMPANY_NAME': 'name',
        'FG_PRICE(NOW)': 'price',
        # legacy alias if ever nonzero
        'P_VOL_AVG(-1/0/0)': 'avg_30d_volume_millions',
        'P_VOLUME_AVG(0,-1/0/0,0)': 'avg_30d_volume_shares',
        'P_VOLUME_AVG(0,-1/0/0)': 'avg_30d_volume_millions_raw',
        'P_VOLUME(0)': 'volume_last',
        # HTTP snake-case fallbacks
        "fsym_ticker_exchange_0_id_": "ticker",
        "fg_company_name": "name",
        "fg_price_now_": "price",
        "p_vol_avg_-1_0_0_": "avg_30d_volume_millions",
        "p_volume_avg_0_-1_0_0_0_": "avg_30d_volume_shares",
        "p_volume_avg_0_-1_0_0_": "avg_30d_volume_millions_raw",
        "p_volume_0_": "volume_last",
    }

    shares_symbol = f["_shares_symbol"].upper()

    def _find_col(prefix: str, symbol: str):
        for c in df.columns:
            if isinstance(c, str) and c.startswith(prefix) and (f",{symbol}," in c or f"({symbol}," in c):
                return c
        return None

    found_shares = _find_col('EXG_OMX_SHARES', shares_symbol)
    if found_shares:
        rename_map[found_shares] = 'shares'
    capped_symbol = f.get('_capped_symbol')
    if capped_symbol:
        found_capped = _find_col('EXG_OMX_SHARES', str(capped_symbol).upper())
        if found_capped:
            rename_map[found_capped] = 'shares_capped'
    # Intentionally do not fetch or map API weights; we compute weights from MCAP only

    df = df.rename(columns=rename_map)

    # Optional: join shares from a dedicated GET fetch with a short retry to mitigate timing issues
    try:
        # Only run shares-only GET if requests is available
        if _req_mod is None:
            raise RuntimeError('requests module not available')
        shares_df = fetch_index_shares(region)
        # If shares are entirely null, perform a quick retry after a brief wait
        if shares_df['shares'].notna().sum() == 0 and shares_df['shares_capped'].notna().sum() == 0:
            import time
            time.sleep(0.75)
            shares_df = fetch_index_shares(region)
        # Merge on ticker, preferring explicit shares from shares_df
        if 'ticker' in df.columns and 'ticker' in shares_df.columns:
            df = df.merge(shares_df, on='ticker',
                          how='left', suffixes=('', '_sh'))
            for col in ('shares', 'shares_capped'):
                sh_col = f'{col}_sh'
                if sh_col in df.columns:
                    # Prefer numeric RHS when LHS is null or non-positive
                    lhs = pd.to_numeric(
                        df[col], errors='coerce') if col in df.columns else pd.Series([None] * len(df))
                    rhs = pd.to_numeric(df[sh_col], errors='coerce')
                    # Fill where lhs is NaN or <= 0 and rhs is positive
                    use_rhs = (lhs.isna() | (lhs <= 0)) & (rhs > 0)
                    df[col] = lhs.where(~use_rhs, rhs)
            # Drop helper columns
            drop_cols = [c for c in df.columns if c.endswith('_sh')]
            if drop_cols:
                df = df.drop(columns=drop_cols)
    except Exception as _merge_err:
        print('[shares-join] skipped due to:', _merge_err)
    if 'issuer' not in df.columns:
        df['issuer'] = df['name'] if 'name' in df.columns else None

        # Compute mcap if shares arrive; else keep price and set mcap None
    if 'price' in df.columns and 'shares' in df.columns:
        df['mcap'] = pd.to_numeric(
            df['price'], errors='coerce') * pd.to_numeric(df['shares'], errors='coerce')

    df['region'] = (region or 'CPH').upper()

    # Harmonize volume columns (produce a single 'avg_vol_30d_millions')
    # Priority:
    # 1) explicit millions (P_VOLUME_AVG millions or legacy P_VOL_AVG)
    # 2) explicit shares (convert to millions)
    # 3) last day volume as fallback (convert to millions)
    if 'avg_30d_volume_millions_raw' in df.columns and df['avg_30d_volume_millions_raw'].notna().any():
        df['avg_vol_30d_millions'] = pd.to_numeric(
            df['avg_30d_volume_millions_raw'], errors='coerce')
    elif 'avg_30d_volume_millions' in df.columns and df['avg_30d_volume_millions'].notna().any():
        df['avg_vol_30d_millions'] = pd.to_numeric(
            df['avg_30d_volume_millions'], errors='coerce')
    elif 'avg_30d_volume_shares' in df.columns and df['avg_30d_volume_shares'].notna().any():
        df['avg_vol_30d_millions'] = pd.to_numeric(
            df['avg_30d_volume_shares'], errors='coerce') / 1_000_000.0
    elif 'avg_30d_volume' in df.columns and df['avg_30d_volume'].notna().any():
        # historical compatibility if server returns 'avg_30d_volume' already in millions
        df['avg_vol_30d_millions'] = pd.to_numeric(
            df['avg_30d_volume'], errors='coerce')
    elif 'volume_last' in df.columns:
        df['avg_vol_30d_millions'] = pd.to_numeric(
            df['volume_last'], errors='coerce') / 1_000_000.0

    # Ensure output column selection is always initialized
    cols = cols_default
    for c in cols:
        if c not in df.columns:
            df[c] = None
    try:
        return df[cols]
    except Exception:
        return df[[c for c in cols if c in df.columns]]


def fetch_index_shares(region: str = 'CPH') -> pd.DataFrame:
    """Fetch only shares columns via GET to test numeric outputs independently, then return ticker+shares."""
    rgn = (region or 'CPH').upper()
    ccy = 'DKK' if rgn == 'CPH' else ('EUR' if rgn == 'HEL' else 'SEK')
    if rgn == 'CPH':
        sym = 'OMXCALLS'
        cap = 'OMXCCAPX'
    elif rgn == 'HEL':
        sym = 'OMXHALLS'
        cap = 'OMXHCAPX'
    else:
        sym = 'OMXSALLS'
        cap = None
    formulas = ['FSYM_TICKER_EXCHANGE(0,"ID")',
                f'EXG_OMX_SHARES(NOW,{sym},PI,{ccy},ND)']
    if cap:
        formulas.append(f'EXG_OMX_SHARES(NOW,{cap},PI,{ccy},ND)')

    universe = _universe_expr(region)
    headers = {'Accept': 'application/json',
               'Content-Type': 'application/json'}
    import urllib.parse as _up
    verify_ssl = os.environ.get(
        'FACTSET_VERIFY_SSL', 'true').lower() not in ('0', 'false', 'no')
    params = {
        'universe': universe,
        'formulas': ','.join(formulas),
        'flatten': 'Y',
        'universeExclusion': 'NONEQUITY',
    }
    url = FACTSET_FORMULA_URL + '?' + _up.urlencode(params, safe=",()")
    # Use the conditionally-imported requests module to work in both SDK and non-SDK modes
    # Use same backoff helper via requests (module available as _req_mod), but fall back if not
    try:
        r = _request_with_backoff('GET', url, auth=(FACTSET_USERNAME, FACTSET_API_KEY),
                                  headers=headers, verify=verify_ssl)
    except Exception:
        r = _req_mod.get(url, auth=(FACTSET_USERNAME, FACTSET_API_KEY),
                         headers=headers, verify=verify_ssl)
    print('[shares-only] status:', r.status_code)
    if not r.ok:
        print('[shares-only] body:', r.text[:600])
        r.raise_for_status()
    raw = r.json().get('data', [])
    df = pd.DataFrame(raw)
    ren = {'FSYM_TICKER_EXCHANGE(0,"ID")': 'ticker'}

    def _find_col(prefix: str, symbol: str | None):
        if not symbol:
            return None
        for c in df.columns:
            if isinstance(c, str) and c.startswith(prefix) and (f',{symbol},' in c or f'({symbol},' in c):
                return c
        return None
    s_unc = _find_col('EXG_OMX_SHARES', sym)
    s_cap = _find_col('EXG_OMX_SHARES', cap)
    if s_unc:
        ren[s_unc] = 'shares'
    if s_cap:
        ren[s_cap] = 'shares_capped'
    df = df.rename(columns=ren)
    for key in ('ticker', 'shares', 'shares_capped'):
        if key not in df.columns:
            df[key] = None
    return df[['ticker', 'shares', 'shares_capped']]
