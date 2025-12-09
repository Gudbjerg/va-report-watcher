# workers/indexes/index_calcs.py
from datetime import date, datetime, timezone
from typing import Dict, Tuple

import pandas as pd


Params = Dict[str, float]


def _params_for_region(region: str) -> Params:
    r = (region or "CPH").upper()
    if r == "STO":
        return {"cap": 0.045, "exceptionCap": 0.09, "exceptionAggregateLimit": 0.36}
    return {"cap": 0.045, "exceptionCap": 0.07, "exceptionAggregateLimit": 0.36}


def _normalize_input(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["ticker"] = d["ticker"].astype(str).str.upper()
    if "name" not in d.columns:
        d["name"] = d.get("issuer", "").astype(str)
    else:
        d["name"] = d["name"].astype(str)
    d["price"] = pd.to_numeric(d["price"], errors="coerce").fillna(0.0)
    d["shares"] = pd.to_numeric(d["shares"], errors="coerce").fillna(0.0)
    d["shares_capped"] = pd.to_numeric(
        d.get("shares_capped", 0.0), errors="coerce").fillna(0.0)
    # Use average daily volume in millions when provided
    vol_source = "avg_vol_30d_millions" if "avg_vol_30d_millions" in d.columns else (
        "avg_vol_30d" if "avg_vol_30d" in d.columns else "avg_30d_volume")
    d["avg_vol_30d_millions"] = pd.to_numeric(
        d.get(vol_source, 0.0), errors="coerce").fillna(0.0)
    d["mcap_uncapped"] = d["shares"] * d["price"]
    d["mcap_capped"] = d["shares_capped"] * d["price"]

    if (d["mcap_uncapped"].sum() == 0.0) and ("omx_weight" in d.columns):
        w = pd.to_numeric(d["omx_weight"], errors="coerce").fillna(0.0)
        if w.sum() > 0:
            d["mcap_uncapped"] = w
            d["mcap_capped"] = w
            d.loc[:, "shares"] = 1.0

    d["mcap"] = d["mcap_uncapped"]
    return d


def _compute_issuer_mcaps(d: pd.DataFrame) -> Tuple[pd.DataFrame, float]:
    dd = d.assign(issuer=d.get("issuer", d["name"].str.upper()))
    issuers = (
        dd.groupby("issuer", as_index=False)[["mcap_uncapped", "mcap_capped"]]
        .sum()
    )
    issuers = issuers.sort_values(
        "mcap_uncapped", ascending=False).reset_index(drop=True)
    total_uncapped = issuers["mcap_uncapped"].sum()
    total_capped = issuers["mcap_capped"].sum()
    issuers["initWeight_uncapped"] = issuers["mcap_uncapped"] / \
        (total_uncapped if total_uncapped else 1.0)
    issuers["initWeight_capped"] = issuers["mcap_capped"] / \
        (total_capped if total_capped else 1.0)
    return issuers, total_uncapped


def _apply_quarterly_exceptions(issuers: pd.DataFrame, params: Params) -> Dict[str, float]:
    region = params.get("region", "CPH").upper()
    top_cap = 0.07 if region in ("CPH", "HEL") else 0.045
    other_cap = 0.045
    top_n = 5 if region in ("CPH", "HEL") else 0

    init = issuers[["issuer", "initWeight_uncapped"]].copy().sort_values(
        "initWeight_uncapped", ascending=False).reset_index(drop=True)
    fixed: Dict[str, float] = {}
    for _, row in init.head(top_n).iterrows():
        fixed[row["issuer"]] = top_cap

    def tentative_weights() -> Dict[str, float]:
        sum_fixed = sum(fixed.values())
        remaining = max(0.0, 1.0 - sum_fixed)
        free = init[~init["issuer"].isin(fixed.keys())]
        free_total = free["initWeight_uncapped"].sum() or 1.0
        out: Dict[str, float] = {}
        for _, row in init.iterrows():
            issuer = row["issuer"]
            out[issuer] = fixed.get(
                issuer, (row["initWeight_uncapped"] / free_total) * remaining)
        return out

    while True:
        t = tentative_weights()
        violators = [k for k, v in t.items(
        ) if k not in fixed and v > other_cap]
        if not violators:
            return t
        violators.sort(key=lambda k: t[k], reverse=True)
        fixed[violators[0]] = other_cap


def _apply_daily_capping(issuers: pd.DataFrame, params: Params) -> Dict[str, float]:
    cap = params["cap"]
    exc = params["exceptionCap"]
    init = issuers[["issuer", "initWeight_uncapped"]].copy()
    fixed: Dict[str, float] = {}

    def tentative_weights() -> Dict[str, float]:
        sum_fixed = sum(fixed.values())
        remaining = max(0.0, 1.0 - sum_fixed)
        free = init[~init["issuer"].isin(fixed.keys())]
        free_total = free["initWeight_uncapped"].sum() or 1.0
        out: Dict[str, float] = {}
        for _, row in init.iterrows():
            issuer = row["issuer"]
            out[issuer] = fixed.get(
                issuer, (row["initWeight_uncapped"] / free_total) * remaining)
        return out

    while True:
        added = False
        for _, row in init.iterrows():
            if row["issuer"] not in fixed and row["initWeight_uncapped"] > 0.10:
                fixed[row["issuer"]] = exc
                added = True
        t = tentative_weights()
        over5 = {k: v for k, v in t.items() if v > 0.05}
        agg = sum(over5.values())
        if agg <= 0.40:
            return t
        candidates = sorted(over5.items(), key=lambda kv: kv[1])
        chosen = None
        for issuer, wt in candidates:
            if fixed.get(issuer) != exc:
                chosen = issuer
                break
        if chosen is None:
            return t
        fixed[chosen] = cap


def _distribute_to_constituents(d: pd.DataFrame, final_issuer_w: Dict[str, float]) -> pd.DataFrame:
    d = d.copy()
    d["issuer"] = d.get("issuer", d["name"].str.upper())
    totals_uncapped = d.groupby(
        "issuer")["mcap_uncapped"].sum().rename("issuer_mcap_uncapped")
    totals_capped = d.groupby(
        "issuer")["mcap_capped"].sum().rename("issuer_mcap_capped")
    d = d.join(totals_uncapped, on="issuer").join(totals_capped, on="issuer")
    d["issuer_share_uncapped"] = d.apply(lambda r: (
        (r["mcap_uncapped"] / r["issuer_mcap_uncapped"]) if r.get("issuer_mcap_uncapped") else 0.0), axis=1)
    d["issuer_share_capped"] = d.apply(lambda r: (
        (r["mcap_capped"] / r["issuer_mcap_capped"]) if r.get("issuer_mcap_capped") else 0.0), axis=1)
    d["weight"] = d.apply(lambda r: (final_issuer_w.get(
        r["issuer"], 0.0) * r["issuer_share_uncapped"]), axis=1)
    # If API provided current capped weights, prefer them for capped_weight; else compute pro-rata
    if "omx_weight_capped" in d.columns:
        d["capped_weight"] = pd.to_numeric(
            d["omx_weight_capped"], errors="coerce").fillna(0.0)
    else:
        d["capped_weight"] = d.apply(lambda r: (final_issuer_w.get(
            r["issuer"], 0.0) * r["issuer_share_capped"]), axis=1)
    return d


def build_status(df_raw: pd.DataFrame, as_of: date, index_id: str, region: str, quarterly: bool = False) -> pd.DataFrame:
    d = _normalize_input(df_raw)
    params = _params_for_region(region)
    params["region"] = (region or "CPH").upper()
    issuers, _ = _compute_issuer_mcaps(d)
    final_issuer_w = _apply_quarterly_exceptions(
        issuers, params) if quarterly else _apply_daily_capping(issuers, params)
    out = _distribute_to_constituents(d, final_issuer_w)
    out["index_id"] = index_id
    as_of_dt = datetime.combine(
        as_of, datetime.min.time(), tzinfo=timezone.utc)
    out["as_of"] = as_of_dt.isoformat()
    out["weight"] = out["weight"].astype("float64")
    out["capped_weight"] = out["capped_weight"].astype("float64")
    out["region"] = (region or "").upper()
    cols = [
        "index_id", "ticker", "issuer", "name", "price", "shares", "shares_capped",
        "mcap", "mcap_uncapped", "mcap_capped", "weight", "capped_weight", "avg_vol_30d", "as_of", "region",
    ]
    return out[cols].copy()


def build_quarterly_proforma(df_raw: pd.DataFrame, as_of: date, index_id: str, region: str, aum_ccy: float) -> pd.DataFrame:
    d = _normalize_input(df_raw)
    params = _params_for_region(region)
    params["region"] = (region or "CPH").upper()
    issuers, _ = _compute_issuer_mcaps(d)
    final_issuer_w = _apply_quarterly_exceptions(issuers, params)
    dd = _distribute_to_constituents(d, final_issuer_w)
    # Current weights for ordering vs delta:
    # - Use UNCAPPED weight for ordering and preview context
    # - Use CAPPED current weight for delta calculations (user requirement)
    total_mcap_uncapped = d["mcap_uncapped"].sum() or 1.0
    # Uncapped current weight (for ordering/context)
    if "omx_weight" in d.columns:
        d["curr_weight_uncapped"] = (pd.to_numeric(
            d["omx_weight"], errors="coerce").fillna(0.0) / 100.0)
    else:
        d["curr_weight_uncapped"] = d["mcap_uncapped"] / total_mcap_uncapped
    # Capped current weight (for delta)
    if "omx_weight_capped" in d.columns:
        d["curr_weight_capped"] = (pd.to_numeric(
            d["omx_weight_capped"], errors="coerce").fillna(0.0) / 100.0)
    else:
        # If capped OMX weight is unavailable, fall back to uncapped
        d["curr_weight_capped"] = d["curr_weight_uncapped"]
    out = dd.join(d[["ticker", "curr_weight_uncapped", "curr_weight_capped"]].set_index(
        "ticker"), on="ticker")
    # Delta should reflect movement from current CAPPED weight to target weight
    out["delta_pct"] = (out["weight"] - out["curr_weight_capped"])
    out["delta_ccy"] = out["delta_pct"] * float(aum_ccy or 0.0)
    # Convert millions to actual shares for delta volume
    out["delta_vol"] = out.apply(lambda r: (
        (r["delta_ccy"] / (r["avg_vol_30d_millions"] * 1_000_000)) if r.get("avg_vol_30d_millions", 0.0) else 0.0), axis=1)
    # Use absolute Days to Cover for clarity
    out["days_to_cover"] = out.apply(lambda r: (
        abs((r["delta_vol"] / r["price"])) if r["price"] else 0.0), axis=1)
    out["index_id"] = index_id
    as_of_dt = datetime.combine(
        as_of, datetime.min.time(), tzinfo=timezone.utc)
    out["as_of"] = as_of_dt.isoformat()
    out["region"] = (region or "").upper()
    cols = [
        "index_id", "ticker", "issuer", "name", "price", "shares", "shares_capped",
        "mcap_uncapped", "mcap_capped", "curr_weight_uncapped", "curr_weight_capped", "weight", "capped_weight",
        "delta_pct", "delta_ccy", "delta_vol", "days_to_cover", "avg_vol_30d_millions", "as_of", "region",
    ]
    # Sort by largest uncapped mcap for preview consistency
    out_sorted = out.sort_values("mcap_uncapped", ascending=False)
    return out_sorted[cols].copy()
