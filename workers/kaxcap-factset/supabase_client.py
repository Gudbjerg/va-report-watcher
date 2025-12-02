# workers/kaxcap-factset/supabase_client.py
import json
import os

import pandas as pd
import requests


SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")

# IMPORTANT: prefer service role key; fallback to SUPABASE_KEY if that's what you configured.
SUPABASE_SERVICE_KEY = (
    os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    or os.environ.get("SUPABASE_KEY")
    or ""
)

# Columns expected in per-index tables
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
    """
    Upsert rows into per-index tables for a given (index_id, as_of).
    """
    if df_status.empty:
        print("No rows to upsert.")
        return

    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print("Supabase env missing: SUPABASE_URL or SERVICE KEY is empty. Configure SUPABASE_SERVICE_ROLE_KEY or SUPABASE_KEY.")
        return

    idx = df_status["index_id"].iloc[0]
    as_of = df_status["as_of"].iloc[0]
    table_name = _table_for_index(idx)

    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        # Use header for merge-duplicates behavior (not a query param)
        "Prefer": "resolution=merge-duplicates",
    }

    # Delete any existing snapshot for this date in the per-index table
    delete_url = f"{SUPABASE_URL}/rest/v1/{table_name}"
    delete_params = {
        "as_of": f"eq.{as_of}",
    }
    delete_resp = requests.delete(
        delete_url, headers=headers, params=delete_params)
    if not delete_resp.ok:
        print("Warning: delete failed:",
              delete_resp.status_code, delete_resp.text)

    # Build normalized payload
    raw_payload = df_status.to_dict(orient="records")
    normalized_payload = []
    for row in raw_payload:
        r = dict(row)
        # Map avg_vol_30d -> avg_daily_volume (Supabase column)
        if "avg_vol_30d" in r and "avg_daily_volume" not in r:
            r["avg_daily_volume"] = r["avg_vol_30d"]
        r.pop("avg_vol_30d", None)

        # Remove fields not present in per-index tables
        r.pop("issuer", None)
        r.pop("region", None)
        r.pop("index_id", None)

        # Ensure types are numeric where expected
        # (avoid 400s due to type coercion issues)
        for num_key in ("price", "shares", "mcap", "weight", "capped_weight", "avg_daily_volume"):
            if num_key in r and r[num_key] is not None:
                try:
                    r[num_key] = float(r[num_key])
                except Exception:
                    r[num_key] = None

        # Default source lineage if not provided
        r.setdefault("source", "factset")

        normalized_payload.append(r)

    insert_url = f"{SUPABASE_URL}/rest/v1/{table_name}"
    # No query params needed; headers carry auth + Prefer resolution
    insert_params = {}

    def _do_insert(rows):
        return requests.post(
            insert_url,
            headers=headers,
            params=insert_params,
            data=json.dumps(rows),
        )

    # First attempt
    insert_resp = _do_insert(normalized_payload)
    if insert_resp.ok:
        print(f"Inserted {len(normalized_payload)} rows into {table_name}.")
        return

    print("Insert failed (first attempt):",
          insert_resp.status_code, insert_resp.text)

    # Fallback: trim to whitelist and retry
    trimmed_payload = [
        {k: v for k, v in row.items() if k in WHITELIST_COLUMNS}
        for row in normalized_payload
    ]

    insert_resp2 = _do_insert(trimmed_payload)
    if insert_resp2.ok:
        print(
            f"Inserted {len(trimmed_payload)} rows into {table_name} (trimmed whitelist).")
        return

    print("Insert failed (second attempt):",
          insert_resp2.status_code, insert_resp2.text)
    insert_resp2.raise_for_status()
