# workers/kaxcap-factset/main.py
from datetime import date
import argparse
import os

from factset_client import fetch_kaxcap_raw
from kaxcap_calcs import build_status
from supabase_client import upsert_index_constituents


def run_update() -> None:
    parser = argparse.ArgumentParser(
        description="Run index worker for a region")
    parser.add_argument("--region", default=os.getenv("REGION", "CPH"),
                        choices=["CPH", "HEL", "STO"], help="Exchange region/universe")
    parser.add_argument("--index-id", dest="index_id", default=os.getenv(
        "INDEX_ID", "KAXCAP"), help="Index ID to tag rows with")
    parser.add_argument("--as-of", dest="as_of",
                        default=date.today().isoformat(), help="As-of date (YYYY-MM-DD)")
    parser.add_argument("--quarterly", dest="quarterly",
                        action="store_true", help="Use quarterly exception methodology")
    args = parser.parse_args()

    as_of = date.fromisoformat(args.as_of)
    print(
        f"Running {args.index_id} update for {as_of} in {args.region} (quarterly={args.quarterly})…")

    df_raw = fetch_kaxcap_raw(args.region)
    print(f"Fetched {len(df_raw)} rows from Formula API")

    df_status = build_status(df_raw, as_of=as_of, index_id=args.index_id,
                             region=args.region, quarterly=args.quarterly)
    upsert_index_constituents(df_status)

    print(f"{args.index_id} status upsert complete.")


if __name__ == "__main__":
    run_update()
