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
    "weight",
    "capped_weight",
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
        "Prefer": "resolution=merge-duplicates",
    }

    raw_payload = df_status.to_dict(orient="records")
    normalized_payload = []
    for row in raw_payload:
        r = dict(row)
        if "avg_vol_30d" in r and "avg_daily_volume" not in r:
            r["avg_daily_volume"] = r["avg_vol_30d"]
        r.pop("avg_vol_30d", None)
        r.pop("issuer", None)
        r.pop("region", None)
        r.pop("index_id", None)
        r.pop("shares_capped", None)
        r.pop("mcap_uncapped", None)
        r.pop("mcap_capped", None)
        for num_key in ("price", "shares", "mcap", "weight", "capped_weight", "avg_daily_volume"):
            if num_key in r and r[num_key] is not None:
                try:
                    r[num_key] = float(r[num_key])
                    if math.isnan(r[num_key]) or math.isinf(r[num_key]):
                        r[num_key] = None
                except Exception:
                    r[num_key] = None
        r.setdefault("source", "factset")
        normalized_payload.append(r)

    if _create_supabase_client is not None:
        try:
            client = _create_supabase_client(
                SUPABASE_URL, SUPABASE_SERVICE_KEY)
            try:
                client.table(table_name).delete().eq('as_of', as_of).execute()
            except Exception as de:
                print("Warning: SDK delete failed:", de)
            try:
                res = client.table(table_name).insert(
                    normalized_payload).execute()
                try:
                    inserted = len(getattr(res, 'data', []) or [])
                except Exception:
                    inserted = len(normalized_payload)
                print(
                    f"Inserted {inserted or len(normalized_payload)} rows into {table_name} (SDK).")
                return
            except Exception as ie:
                print("SDK insert failed, falling back to raw requests:", ie)
        except Exception as e:
            print("Warning: failed to init Supabase SDK, using raw requests:", e)

    delete_url = f"{SUPABASE_URL}/rest/v1/{table_name}"
    delete_params = {"as_of": f"eq.{as_of}"}
    delete_resp = requests.delete(
        delete_url, headers=headers, params=delete_params)
    if not delete_resp.ok:
        print("Warning: delete failed:",
              delete_resp.status_code, delete_resp.text)

    insert_url = f"{SUPABASE_URL}/rest/v1/{table_name}"

    def _do_insert(rows):
        return requests.post(insert_url, headers=headers, json=rows)

    insert_resp = _do_insert(normalized_payload)
    if insert_resp.ok:
        print(f"Inserted {len(normalized_payload)} rows into {table_name}.")
        return
    print("Insert failed (first attempt):",
          insert_resp.status_code, insert_resp.text)
    trimmed_payload = [{k: v for k, v in row.items() if k in WHITELIST_COLUMNS}
                       for row in normalized_payload]
    insert_resp2 = _do_insert(trimmed_payload)
    if insert_resp2.ok:
        print(
            f"Inserted {len(trimmed_payload)} rows into {table_name} (trimmed whitelist).")
        return
    print("Insert failed (second attempt):",
          insert_resp2.status_code, insert_resp2.text)
    insert_resp2.raise_for_status()
