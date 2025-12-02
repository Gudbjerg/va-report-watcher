# workers/kaxcap-factset/supabase_client.py
import json
import os

import pandas as pd
import requests


SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")

# Re-use your existing env name. In your .env this is SUPABASE_KEY.
# Make sure this is the **service role key**, not the anon key.
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_KEY"]


def upsert_index_constituents(df_status: pd.DataFrame) -> None:
    """
    Upsert rows into public.index_constituents for (index_id, as_of).

    Implementation: delete existing rows for that (index_id, as_of),
    then insert the new snapshot.
    """
    if df_status.empty:
        print("No rows to upsert.")
        return

    idx = df_status["index_id"].iloc[0]
    as_of = df_status["as_of"].iloc[0]

    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
    }

    # 1) delete any old snapshot for this index + date
    delete_url = f"{SUPABASE_URL}/rest/v1/index_constituents"
    delete_params = {
        "index_id": f"eq.{idx}",
        "as_of": f"eq.{as_of}",
    }
    delete_resp = requests.delete(
        delete_url, headers=headers, params=delete_params)
    if not delete_resp.ok:
        print("Warning: delete failed:",
              delete_resp.status_code, delete_resp.text)

    # 2) insert new rows with graceful fallback if optional columns don't exist
    insert_url = f"{SUPABASE_URL}/rest/v1/index_constituents"
    # Map calculated columns to Supabase schema where needed
    df = df_status.copy()
    # Supabase column is avg_daily_volume; map from avg_vol_30d if present
    if "avg_vol_30d" in df.columns and "avg_daily_volume" not in df.columns:
        df["avg_daily_volume"] = df["avg_vol_30d"]
    payload = df.to_dict(orient="records")
    insert_params = {"prefer": "resolution=merge-duplicates"}

    def _do_insert(rows):
        return requests.post(
            insert_url,
            headers=headers,
            params=insert_params,
            data=json.dumps(rows),
        )

    insert_resp = _do_insert(payload)
    if not insert_resp.ok:
        # Log full error body to aid debugging in Render
        print("Supabase insert failed:", insert_resp.status_code)
        try:
            print("Error body:", insert_resp.text)
        except Exception:
            pass

        # Retry with a conservative trimmed schema of known columns
        allowed = {
            "index_id",
            "ticker",
            "name",
            "price",
            "shares",
            "mcap",
            "weight",
            "capped_weight",
            "avg_daily_volume",
            "as_of",
        }
        trimmed = [{k: v for k, v in row.items() if k in allowed}
                   for row in payload]
        print("Retrying insert with trimmed payload (known columns only)…")
        insert_resp2 = _do_insert(trimmed)
        if not insert_resp2.ok:
            print("Trimmed insert still failed:", insert_resp2.status_code)
            try:
                print("Error body:", insert_resp2.text)
            except Exception:
                pass
            insert_resp2.raise_for_status()
        print(
            f"Inserted {len(trimmed)} rows into index_constituents (trimmed schema).")
    else:
        print(f"Inserted {len(payload)} rows into index_constituents.")
