# workers/indexes/main.py
from datetime import date
import argparse
import os

from factset_client import fetch_index_raw
from index_calcs import build_status, build_issuer_status, _compute_issuer_mcaps, _apply_daily_capping, _distribute_to_constituents
from supabase_client import upsert_index_constituents, upsert_index_quarterly, upsert_index_issuers


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
    parser.add_argument("--aum", dest="aum", default=os.getenv("AUM", "0"),
                        help="Fund AUM in index currency (e.g., DKK/EUR)")
    args = parser.parse_args()

    as_of = date.fromisoformat(args.as_of)
    print(
        f"Running {args.index_id} update for {as_of} in {args.region} (quarterly={args.quarterly})…")

    df_raw = fetch_index_raw(args.region)
    print(f"Fetched {len(df_raw)} rows from Formula API")

    # Always compute DAILY status for constituents (never write quarterly logic into daily table)
    df_status = build_status(df_raw, as_of=as_of, index_id=args.index_id,
                             region=args.region, quarterly=False)
    # Lightweight preview of computed weights (helps verify capping logic)
    try:
        preview_cols = ["ticker", "name", "weight", "capped_weight"]
        ranked = df_status.sort_values("weight", ascending=False).head(10)
        print("Computed top weights (preview):")
        print(ranked[preview_cols].to_string(index=False))
    except Exception:
        pass
    upsert_index_constituents(df_status)

    print(f"{args.index_id} status upsert complete.")

    # Persist issuer-level snapshot (daily methodology); delta_vol/DTC intentionally blank
    try:
        df_issuers = build_issuer_status(
            df_raw, as_of=as_of, index_id=args.index_id, region=args.region, aum_ccy=None, quarterly=False
        )
        upsert_index_issuers(df_issuers)
    except Exception as e:
        print("Issuer upsert error (non-fatal):", e)

    try:
        aum_val = float(args.aum)
    except Exception:
        aum_val = 0.0
    if args.quarterly:
        from index_calcs import build_quarterly_proforma
        df_pro = build_quarterly_proforma(
            df_raw, as_of=as_of, index_id=args.index_id, region=args.region, aum_ccy=aum_val)
        # Persist quarterly snapshot
        try:
            upsert_index_quarterly(df_pro)
        except Exception as e:
            print("Quarterly upsert error:", e)
        # Optionally persist issuer-level snapshot with AUM for delta_ccy (still blank delta_vol/DTC)
        try:
            df_issuers_q = build_issuer_status(
                df_raw, as_of=as_of, index_id=args.index_id, region=args.region, aum_ccy=aum_val, quarterly=True
            )
            upsert_index_issuers(df_issuers_q)
        except Exception as e:
            print("Issuer (quarterly) upsert error (non-fatal):", e)
        try:
            # Daily status preview: apply daily capping weights and show top 10 by uncapped mcap
            try:
                d = df_status.copy()
                issuers, _ = _compute_issuer_mcaps(d)
                daily_weights = _apply_daily_capping(
                    issuers, {"region": args.region})
                d_daily = _distribute_to_constituents(d, daily_weights)
                ranked = d_daily.sort_values("mcap_uncapped", ascending=False)
                print("Daily Status — Top 25 by uncapped mcap:")
                print(ranked[["ticker", "name", "mcap_uncapped", "weight"]].head(
                    25).to_string(index=False))
            except Exception:
                pass
            print("Quarterly proforma preview:")
            # Show both current uncapped and capped weights alongside target
            print(df_pro[["ticker", "name", "curr_weight_uncapped", "curr_weight_capped", "weight", "delta_pct",
                          "delta_ccy", "delta_vol", "days_to_cover"]].head(12).to_string(index=False))
        except Exception as e:
            print("Proforma preview error:", e)


if __name__ == "__main__":
    run_update()
