# workers/indexes/supabase_client.py
import json
import math
import os

import pandas as pd
import requests

try:
    from supabase import create_client as _create_supabase_client  # type: ignore
except Exception:
    _create_supabase_client = None


SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_SERVICE_KEY = (
    os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    or os.environ.get("SUPABASE_KEY")
    or ""
)

WHITELIST_COLUMNS = {
    "ticker",
    "isin",
    "name",
    "price",
    "shares",
    "mcap",
    "mcap_capped",
    "weight",
    "capped_weight",
    "delta_pct",
    "avg_daily_volume",
    "as_of",
    "source",
}


def _table_for_index(index_id: str) -> str:
    idx = (index_id or "").strip().upper()
    mapping = {
        "KAXCAP": "index_constituents_kaxcap",
        "HELXCAP": "index_constituents_helxcap",
        "OMXSALLS": "index_constituents_omxsalls",
    }
    return mapping.get(idx, f"index_constituents_{idx.lower()}")


def _quarterly_table_for_index(index_id: str) -> str:
    idx = (index_id or "").strip().upper()
    mapping = {
        "KAXCAP": "index_quarterly_kaxcap",
        "HELXCAP": "index_quarterly_helxcap",
        "OMXSALLS": "index_quarterly_omxsalls",
    }
    return mapping.get(idx, f"index_quarterly_{idx.lower()}")


def _issuers_table_for_index(index_id: str) -> str:
    idx = (index_id or "").strip().upper()
    mapping = {
        "KAXCAP": "index_issuers_kaxcap",
        "HELXCAP": "index_issuers_helxcap",
        "OMXSALLS": "index_issuers_omxsalls",
    }
    return mapping.get(idx, f"index_issuers_{idx.lower()}")


def upsert_index_constituents(df_status: pd.DataFrame) -> None:
    if df_status.empty:
        print("No rows to upsert.")
        return
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print("Supabase env missing: SUPABASE_URL or SERVICE KEY is empty. Configure SUPABASE_SERVICE_ROLE_KEY or SUPABASE_KEY.")
        return

    if "weight" in df_status.columns:
        df_status = df_status[~df_status["weight"].isna()].copy()
        df_status = df_status[df_status["weight"].apply(lambda v: isinstance(
            v, (int, float)) and not (math.isnan(v) or math.isinf(v)))].copy()

    if df_status.empty:
        print("No valid rows to upsert after filtering.")
        return

    idx = df_status["index_id"].iloc[0]
    as_of = df_status["as_of"].iloc[0]
    table_name = _table_for_index(idx)

    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        # Upsert semantics (requires a UNIQUE constraint on (as_of, ticker))
        "Prefer": "resolution=merge-duplicates",
    }

    raw_payload = df_status.to_dict(orient="records")
    normalized_payload = []
    for row in raw_payload:
        r = dict(row)
        # Keep delta_pct so daily pages can render deltas
        # Prefer explicit daily volume in shares; support millions-based alias
        if "avg_daily_volume" not in r:
            if "avg_vol_30d" in r:
                r["avg_daily_volume"] = r.get("avg_vol_30d")
            elif "avg_vol_30d_millions" in r:
                # Convert millions to shares (big number)
                try:
                    r["avg_daily_volume"] = float(
                        r.get("avg_vol_30d_millions", 0.0)) * 1_000_000.0
                except Exception:
                    r["avg_daily_volume"] = None
        # Clean up aliases and non-persisted fields
        r.pop("avg_vol_30d", None)
        r.pop("issuer", None)
        r.pop("region", None)
        r.pop("index_id", None)
        r.pop("shares_capped", None)
        r.pop("mcap_uncapped", None)
        # Keep mcap_capped so we can surface capped totals in Daily
        # Normalize numbers
        for num_key in ("price", "shares", "mcap", "weight", "capped_weight", "delta_pct", "avg_daily_volume"):
            if num_key in r and r[num_key] is not None:
                try:
                    r[num_key] = float(r[num_key])
                    if math.isnan(r[num_key]) or math.isinf(r[num_key]):
                        r[num_key] = None
                except Exception:
                    r[num_key] = None
        # Normalize name
        if "name" in r and r["name"] is not None:
            try:
                r["name"] = str(r["name"]).strip()
            except Exception:
                r["name"] = None
        # Normalize as_of to ISO string
        if "as_of" in r and r["as_of"] is not None:
            try:
                val = r["as_of"]
                if isinstance(val, (pd.Timestamp,)):
                    r["as_of"] = val.isoformat()
                else:
                    # Support datetime/date or plain string/number
                    try:
                        r["as_of"] = pd.to_datetime(val).isoformat()
                    except Exception:
                        r["as_of"] = str(val)
            except Exception:
                pass
        r.setdefault("source", "factset")
        # Always trim payload to whitelist columns to avoid 400s
        trimmed = {k: r.get(k) for k in WHITELIST_COLUMNS}
        normalized_payload.append(trimmed)

    # Prefer conflict-safe UPSERT to avoid duplicates during concurrent runs
    upsert_url = f"{SUPABASE_URL}/rest/v1/{table_name}?on_conflict=as_of,ticker"
    try:
        resp = requests.post(upsert_url, headers=headers,
                             json=normalized_payload)
        if resp.ok:
            print(
                f"Upserted {len(normalized_payload)} rows into {table_name}.")
            return
        else:
            print("Upsert failed (", resp.status_code, "):", resp.text)
            # As a very last resort, fall back to delete+insert (not race-safe)
            try:
                delete_url = f"{SUPABASE_URL}/rest/v1/{table_name}"
                delete_params = {"as_of": f"eq.{as_of}"}
                requests.delete(delete_url, headers=headers,
                                params=delete_params)
            except Exception:
                pass
            insert_url = f"{SUPABASE_URL}/rest/v1/{table_name}"
            resp2 = requests.post(
                insert_url, headers=headers, json=normalized_payload)
            if resp2.ok:
                print(
                    f"Inserted {len(normalized_payload)} rows into {table_name} (fallback).")
                return
            print("Insert fallback failed:", resp2.status_code, resp2.text)
            resp2.raise_for_status()
    except Exception as e:
        print("Request to Supabase failed:", e)
        raise


def upsert_index_quarterly(df_pro: pd.DataFrame) -> None:
    if df_pro.empty:
        print("No quarterly rows to upsert.")
        return
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print("Supabase env missing: SUPABASE_URL or SERVICE KEY is empty. Configure SUPABASE_SERVICE_ROLE_KEY or SUPABASE_KEY.")
        return

    idx = str(df_pro.get("index_id", [None])[0] or "").upper()
    as_of = df_pro.get("as_of", [None])[0]
    table_name = _quarterly_table_for_index(idx)

    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        # Upsert semantics (requires UNIQUE (as_of, ticker))
        "Prefer": "resolution=merge-duplicates",
    }

    # Normalize payload and keep key fields used by UI/API
    raw_payload = df_pro.to_dict(orient="records")
    normalized_payload = []
    for row in raw_payload:
        r = dict(row)
        # Remove fields not present in per-index quarterly tables
        for drop_key in ("index_id", "region", "issuer"):
            if drop_key in r:
                r.pop(drop_key, None)
        # Ensure numeric
        for num_key in (
            "price",
            "shares",
            "shares_capped",
            "mcap_uncapped",
            "mcap_capped",
            "curr_weight_uncapped",
            "curr_weight_capped",
            "weight",
            "capped_weight",
            "delta_pct",
            "delta_ccy",
            "delta_vol",
            "days_to_cover",
            "avg_vol_30d",
            "avg_vol_30d_millions",
        ):
            if num_key in r and r[num_key] is not None:
                try:
                    r[num_key] = float(r[num_key])
                    if math.isnan(r[num_key]) or math.isinf(r[num_key]):
                        r[num_key] = None
                except Exception:
                    r[num_key] = None
        # Normalize as_of to ISO string
        if "as_of" in r and r["as_of"] is not None:
            try:
                r["as_of"] = pd.to_datetime(r["as_of"]).isoformat()
            except Exception:
                try:
                    r["as_of"] = str(r["as_of"])
                except Exception:
                    pass
        normalized_payload.append(r)

    # Conflict-safe UPSERT
    upsert_url = f"{SUPABASE_URL}/rest/v1/{table_name}?on_conflict=as_of,ticker"
    resp = requests.post(upsert_url, headers=headers, json=normalized_payload)
    if resp.ok:
        print(
            f"Upserted {len(normalized_payload)} rows into {table_name} (quarterly).")
        return
    print("Quarterly upsert failed:", resp.status_code, resp.text)
    # Last-resort fallback to delete+insert
    try:
        delete_url = f"{SUPABASE_URL}/rest/v1/{table_name}"
        delete_params = {"as_of": f"eq.{as_of}"}
        requests.delete(delete_url, headers=headers, params=delete_params)
        insert_url = f"{SUPABASE_URL}/rest/v1/{table_name}"
        resp2 = requests.post(insert_url, headers=headers,
                              json=normalized_payload)
        if resp2.ok:
            print(
                f"Inserted {len(normalized_payload)} rows into {table_name} (quarterly fallback).")
            return
        print("Quarterly fallback insert failed:",
              resp2.status_code, resp2.text)
        resp2.raise_for_status()
    except Exception as e:
        print("Quarterly fallback failed:", e)
        raise


def upsert_index_issuers(df_issuers: pd.DataFrame) -> None:
    if df_issuers.empty:
        print("No issuer rows to upsert.")
        return
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print("Supabase env missing: SUPABASE_URL or SERVICE KEY is empty. Configure SUPABASE_SERVICE_ROLE_KEY or SUPABASE_KEY.")
        return

    idx = str(df_issuers.get("index_id", [None])[0] or "").upper()
    as_of = df_issuers.get("as_of", [None])[0]
    table_name = _issuers_table_for_index(idx)

    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        # Upsert semantics (requires UNIQUE (as_of, issuer))
        "Prefer": "resolution=merge-duplicates",
    }

    raw_payload = df_issuers.to_dict(orient="records")
    normalized_payload = []
    for row in raw_payload:
        r = dict(row)
        # Remove fields not needed for issuer table
        for drop in ("region", "index_id"):
            r.pop(drop, None)
        # Ensure numeric
        for num_key in (
            "mcap_uncapped",
            "mcap_capped",
            "curr_weight_uncapped",
            "curr_weight_capped",
            "weight",
            "capped_weight",
            "delta_pct",
            "delta_ccy",
        ):
            if num_key in r and r[num_key] is not None:
                try:
                    r[num_key] = float(r[num_key])
                    if math.isnan(r[num_key]) or math.isinf(r[num_key]):
                        r[num_key] = None
                except Exception:
                    r[num_key] = None
        # Normalize as_of
        if "as_of" in r and r["as_of"] is not None:
            try:
                r["as_of"] = pd.to_datetime(r["as_of"]).isoformat()
            except Exception:
                try:
                    r["as_of"] = str(r["as_of"])
                except Exception:
                    pass
        normalized_payload.append(r)

    # Conflict-safe UPSERT on (as_of, issuer)
    upsert_url = f"{SUPABASE_URL}/rest/v1/{table_name}?on_conflict=as_of,issuer"
    resp = requests.post(upsert_url, headers=headers, json=normalized_payload)
    if resp.ok:
        print(
            f"Upserted {len(normalized_payload)} rows into {table_name} (issuers).")
        return
    print("Issuer upsert failed:", resp.status_code, resp.text)
    # If table does not exist yet, log and continue without raising
    try:
        if resp.status_code == 404:
            print(
                f"Issuer table {table_name} not found; skipping persistence.")
            return
    except Exception:
        pass
    # Last resort: delete+insert
    try:
        delete_url = f"{SUPABASE_URL}/rest/v1/{table_name}"
        delete_params = {"as_of": f"eq.{as_of}"}
        requests.delete(delete_url, headers=headers, params=delete_params)
        insert_url = f"{SUPABASE_URL}/rest/v1/{table_name}"
        resp2 = requests.post(insert_url, headers=headers,
                              json=normalized_payload)
        if resp2.ok:
            print(
                f"Inserted {len(normalized_payload)} rows into {table_name} (issuers fallback).")
            return
        print("Issuer fallback insert failed:", resp2.status_code, resp2.text)
        resp2.raise_for_status()
    except Exception as e:
        print("Issuer fallback failed:", e)
        raise
