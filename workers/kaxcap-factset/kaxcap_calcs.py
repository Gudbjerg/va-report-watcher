# workers/kaxcap-factset/kaxcap_calcs.py
from datetime import date, datetime, timezone
from typing import Dict, Tuple

import pandas as pd


Params = Dict[str, float]


def _params_for_region(region: str) -> Params:
    r = (region or "CPH").upper()
    if r == "STO":
        return {"cap": 0.045, "exceptionCap": 0.09, "exceptionAggregateLimit": 0.36}
    # Default CPH/HEL
    return {"cap": 0.045, "exceptionCap": 0.07, "exceptionAggregateLimit": 0.36}


def _normalize_input(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["ticker"] = d["ticker"].astype(str).str.upper()
    # if name missing, fall back to issuer
    if "name" not in d.columns:
        d["name"] = d.get("issuer", "").astype(str)
    else:
        d["name"] = d["name"].astype(str)
    d["price"] = pd.to_numeric(d["price"], errors="coerce").fillna(0.0)
    d["shares"] = pd.to_numeric(d["shares"], errors="coerce").fillna(0.0)
    # handle both possible volume names
    vol_source = "avg_vol_30d" if "avg_vol_30d" in d.columns else "avg_30d_volume"
    d["avg_vol_30d"] = pd.to_numeric(
        d.get(vol_source, 0.0), errors="coerce").fillna(0.0)
    d["mcap"] = d["shares"] * d["price"]
    return d


def _compute_issuer_mcaps(d: pd.DataFrame) -> Tuple[pd.DataFrame, float]:
    # issuer proxy uses name if issuer column not available
    issuers = (
        d.assign(issuer=d.get("issuer", d["name"].str.upper()))
        .groupby("issuer", as_index=False)["mcap"]
        .sum()
    )
    issuers = issuers.sort_values(
        "mcap", ascending=False).reset_index(drop=True)
    total = issuers["mcap"].sum()
    issuers["initWeight"] = issuers["mcap"] / total if total else 0.0
    return issuers, total


def _apply_quarterly_exceptions(issuers: pd.DataFrame, params: Params) -> Dict[str, float]:
    cap = params["cap"]
    exc = params["exceptionCap"]
    limit = params["exceptionAggregateLimit"]

    max_exceptions = int(limit // exc) if exc > 0 else 0
    fixed = {}
    for i, row in issuers.head(max_exceptions).iterrows():
        fixed[row["issuer"]] = exc

    sum_fixed = sum(fixed.values())
    remaining = max(0.0, 1.0 - sum_fixed)
    free = issuers[~issuers["issuer"].isin(fixed.keys())]
    free_total = free["initWeight"].sum() or 1.0

    final = {}
    for _, row in issuers.iterrows():
        issuer = row["issuer"]
        if issuer in fixed:
            final[issuer] = fixed[issuer]
        else:
            final[issuer] = (row["initWeight"] / free_total) * remaining
    return final


def _apply_daily_capping(issuers: pd.DataFrame, params: Params) -> Dict[str, float]:
    cap = params["cap"]
    exc = params["exceptionCap"]
    init = issuers[["issuer", "initWeight"]].copy()
    fixed: Dict[str, float] = {}

    def tentative_weights() -> Dict[str, float]:
        sum_fixed = sum(fixed.values())
        remaining = max(0.0, 1.0 - sum_fixed)
        free = init[~init["issuer"].isin(fixed.keys())]
        free_total = free["initWeight"].sum() or 1.0
        out = {}
        for _, row in init.iterrows():
            issuer = row["issuer"]
            if issuer in fixed:
                out[issuer] = fixed[issuer]
            else:
                out[issuer] = (row["initWeight"] / free_total) * remaining
        return out

    while True:
        # Stage 1: fix any issuer with original weight > 10% to exception cap
        added = False
        for _, row in init.iterrows():
            if row["issuer"] not in fixed and row["initWeight"] > 0.10:
                fixed[row["issuer"]] = exc
                added = True

        t = tentative_weights()
        over5 = {k: v for k, v in t.items() if v > 0.05}
        agg = sum(over5.values())

        if agg <= 0.40:
            return t

        # Stage 2: choose issuer among >5% with the lowest tentative weight, not already fixed to exc
        candidates = sorted(over5.items(), key=lambda kv: kv[1])
        chosen = None
        for issuer, wt in candidates:
            if fixed.get(issuer) != exc:
                chosen = issuer
                break
        if chosen is None:
            return t  # all >5% already exception-capped
        fixed[chosen] = cap
        # loop continues until aggregate <= 40%


def _distribute_to_constituents(d: pd.DataFrame, final_issuer_w: Dict[str, float]) -> pd.DataFrame:
    # Map issuer -> total mcap
    d = d.copy()
    d["issuer"] = d.get("issuer", d["name"].str.upper())
    totals = d.groupby("issuer")["mcap"].sum().rename("issuer_mcap")
    d = d.join(totals, on="issuer")
    d["issuer_share"] = d.apply(lambda r: (
        r["mcap"] / r["issuer_mcap"]) if r["issuer_mcap"] else 0.0, axis=1)
    d["newWeight"] = d.apply(lambda r: (final_issuer_w.get(
        r["issuer"], 0.0) * r["issuer_share"]), axis=1)
    return d


def build_status(df_raw: pd.DataFrame, as_of: date, index_id: str, region: str, quarterly: bool = False) -> pd.DataFrame:
    """
    Compute final capped (or uncapped) weights per Nasdaq methodology for the given region.
    Returns a DataFrame ready to upsert into public.index_constituents with columns:
    index_id, ticker, name, price, shares, mcap, weight, capped_weight, avg_vol_30d, as_of
    """
    d = _normalize_input(df_raw)
    params = _params_for_region(region)

    issuers, _ = _compute_issuer_mcaps(d)
    if quarterly:
        final_issuer_w = _apply_quarterly_exceptions(issuers, params)
    else:
        final_issuer_w = _apply_daily_capping(issuers, params)

    out = _distribute_to_constituents(d, final_issuer_w)

    # Prepare output for DB
    out["index_id"] = index_id

    # Represent as_of as a full ISO timestamp suitable for timestamptz
    as_of_dt = datetime.combine(as_of, datetime.min.time(), tzinfo=timezone.utc)
    out["as_of"] = as_of_dt.isoformat()

    out["weight"] = out["newWeight"].astype("float64")
    out["capped_weight"] = None
    out["region"] = (region or "").upper()

    cols = [
        "index_id",
        "ticker",
        "issuer",
        "name",
        "price",
        "shares",
        "mcap",
        "weight",
        "capped_weight",
        "avg_vol_30d",
        "as_of",
        "region",
    ]
    return out[cols].copy()
