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
    # Average daily volume: source is in millions; convert to shares
    vol_source = "avg_vol_30d_millions" if "avg_vol_30d_millions" in d.columns else (
        "avg_vol_30d" if "avg_vol_30d" in d.columns else "avg_30d_volume")
    d["avg_vol_30d_millions"] = pd.to_numeric(
        d.get(vol_source, 0.0), errors="coerce").fillna(0.0)
    # Big number in shares for calculations
    d["avg_vol_30d"] = d["avg_vol_30d_millions"] * 1_000_000.0
    d["mcap_uncapped"] = d["shares"] * d["price"]
    d["mcap_capped"] = d["shares_capped"] * d["price"]

    # If no capped shares available (e.g., STO), fall back to uncapped so
    # 'capped_weight' equals 'weight' and daily pages remain meaningful.
    try:
        if (d["mcap_capped"].sum() == 0.0) and (d["mcap_uncapped"].sum() > 0.0):
            d["shares_capped"] = d["shares"]
            d["mcap_capped"] = d["mcap_uncapped"]
    except Exception:
        pass

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
    # Proposed capped weight for distribution based on final issuer weights
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

    # Current capped weights (from API if available; else from mcap_capped)
    total_mcap_capped = (d["mcap_capped"].sum() or 1.0)
    if "omx_weight_capped" in d.columns:
        d["curr_weight_capped"] = (pd.to_numeric(
            d["omx_weight_capped"], errors="coerce").fillna(0.0) / 100.0)
    else:
        d["curr_weight_capped"] = d["mcap_capped"] / total_mcap_capped
    # Attach current capped to output to compute delta
    out = out.join(d[["ticker", "curr_weight_capped"]
                     ].set_index("ticker"), on="ticker")
    # Delta = proposed capped_weight - current capped weight
    out["delta_pct"] = pd.to_numeric(out.get("capped_weight", 0.0), errors="coerce").fillna(
        0.0) - pd.to_numeric(out.get("curr_weight_capped", 0.0), errors="coerce").fillna(0.0)

    out["index_id"] = index_id
    as_of_dt = datetime.combine(
        as_of, datetime.min.time(), tzinfo=timezone.utc)
    out["as_of"] = as_of_dt.isoformat()
    out["weight"] = out["weight"].astype("float64")
    out["capped_weight"] = out["capped_weight"].astype("float64")
    out["region"] = (region or "").upper()
    # Round key numeric columns: price/shares to 2 decimals; weights to 4 decimals
    for c in ("price", "shares", "shares_capped", "avg_vol_30d"):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
            out[c] = out[c].round(2)
    for c in ("weight", "capped_weight"):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
            out[c] = out[c].round(4)

    # Derive issuer-level flags for daily rule breaches (10% and 40% aggregate >5%)
    issuers_flags: Dict[str, str] = {}
    try:
        init_map = {row["issuer"]: float(row["initWeight_uncapped"]) for _, row in issuers[[
            "issuer", "initWeight_uncapped"]].iterrows()}
        exc = params.get("exceptionCap", 0.07 if params.get(
            "region", "CPH").upper() in ("CPH", "HEL") else 0.09)
        # Current capped weights per issuer
        d_curr = d.assign(issuer=d.get(
            "issuer", d.get("name", "").str.upper()))
        curr_by_issuer = d_curr.groupby("issuer", as_index=False)[
            "curr_weight_capped"].sum()
        # 10% breach flag (init uncapped > 10%)
        for _, row in curr_by_issuer.iterrows():
            issuer = row["issuer"]
            curr_w = float(row["curr_weight_capped"])
            init_w = float(init_map.get(issuer, 0.0))
            if init_w > 0.10 and curr_w > exc + 1e-9:
                issuers_flags[issuer] = (issuers_flags.get(
                    issuer, "") + (", " if issuers_flags.get(issuer) else "") + "10% breach")
        # 40% aggregate >5% breach
        over5 = [(row["issuer"], float(row["curr_weight_capped"]))
                 for _, row in curr_by_issuer.iterrows() if float(row["curr_weight_capped"]) > 0.05]
        agg = sum([w for _, w in over5])
        if agg > 0.40 + 1e-9:
            candidates = sorted(over5, key=lambda kv: kv[1])
            chosen = None
            for issuer, w in candidates:
                if not (init_map.get(issuer, 0.0) > 0.10):
                    chosen = issuer
                    break
            if chosen:
                issuers_flags[chosen] = (issuers_flags.get(
                    chosen, "") + (", " if issuers_flags.get(chosen) else "") + "40% breach — cut to 4.5%")
    except Exception:
        pass

    # Attach flags to each constituent row based on issuer
    out["flags"] = out.apply(
        lambda r: issuers_flags.get(r.get("issuer"), ""), axis=1)
    cols = [
        "index_id", "ticker", "issuer", "name", "price", "shares", "shares_capped",
        "mcap", "mcap_uncapped", "mcap_capped", "weight", "capped_weight", "delta_pct", "avg_vol_30d", "as_of", "region", "flags",
    ]
    return out[cols].copy()


def build_quarterly_proforma(df_raw: pd.DataFrame, as_of: date, index_id: str, region: str, aum_ccy: float) -> pd.DataFrame:
    d = _normalize_input(df_raw)
    params = _params_for_region(region)
    params["region"] = (region or "CPH").upper()
    # Region-default AUMs (big-number currency units)
    region_upper = params["region"]
    default_aum = 110_000_000_000.0 if region_upper == "CPH" else (
        22_000_000_000.0 if region_upper == "HEL" else float(aum_ccy or 0.0))
    aum = float(aum_ccy) if aum_ccy else default_aum
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
    out["delta_ccy"] = out["delta_pct"] * aum
    # Correct methodology:
    # delta_vol_millions = (delta_ccy / price) converted to millions of shares
    out["delta_vol"] = out.apply(lambda r: (
        (((r["delta_ccy"] / r["price"]) / 1_000_000.0) if r.get("price", 0.0) else 0.0)), axis=1)
    # DTC = abs(delta_vol_millions) / avg_vol_30d_millions
    out["days_to_cover"] = out.apply(lambda r: (
        (abs(r["delta_vol"]) / (r["avg_vol_30d_millions"] if r.get("avg_vol_30d_millions", 0.0) else 1.0))), axis=1)
    out["index_id"] = index_id
    as_of_dt = datetime.combine(
        as_of, datetime.min.time(), tzinfo=timezone.utc)
    out["as_of"] = as_of_dt.isoformat()
    out["region"] = (region or "").upper()
    # Round numeric columns: weights to 4 decimals, others to 2
    for c in ("price", "shares", "shares_capped", "delta_ccy", "delta_vol", "days_to_cover", "avg_vol_30d", "avg_vol_30d_millions"):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
            out[c] = out[c].round(2)
    for c in ("curr_weight_uncapped", "curr_weight_capped", "weight", "capped_weight", "delta_pct"):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
            out[c] = out[c].round(4)
    cols = [
        "index_id", "ticker", "issuer", "name", "price", "shares", "shares_capped",
        "mcap_uncapped", "mcap_capped", "curr_weight_uncapped", "curr_weight_capped", "weight", "capped_weight",
        "delta_pct", "delta_ccy", "delta_vol", "days_to_cover", "avg_vol_30d", "as_of", "region",
    ]
    # Sort by largest uncapped mcap for preview consistency
    out_sorted = out.sort_values("mcap_uncapped", ascending=False)
    return out_sorted[cols].copy()
