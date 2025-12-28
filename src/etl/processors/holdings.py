"""Holdings estimation processor."""
import pandas as pd
from typing import Optional


def build_foreign_master(twse: pd.DataFrame, tpex: pd.DataFrame) -> pd.DataFrame:
    """Build consolidated foreign holdings master with forward-fill."""
    all_df = pd.concat([twse, tpex], ignore_index=True)
    if all_df.empty:
        return all_df

    all_df = all_df.sort_values(["code", "date"])
    all_df["date"] = pd.to_datetime(all_df["date"]).dt.date

    all_df = (
        all_df.set_index(["code", "date"])
        .sort_index()
        .groupby(level=0)
        .ffill()
        .reset_index()
    )
    return all_df


def build_estimated_holdings(
    flows: pd.DataFrame,
    foreign_master: pd.DataFrame,
    baseline: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Build institutional holdings estimation with baseline correction.

    Args:
        flows: DataFrame with daily institutional flows
        foreign_master: DataFrame with foreign holdings data
        baseline: Optional DataFrame with baseline calibration points

    Returns:
        DataFrame with estimated holdings ratios
    """
    flows = flows.copy()
    flows["date"] = pd.to_datetime(flows["date"]).dt.date
    foreign_master = foreign_master.copy()
    foreign_master["date"] = pd.to_datetime(foreign_master["date"]).dt.date

    # Merge flows with foreign holdings
    merged = flows.merge(
        foreign_master[["date", "code", "market", "total_shares", "foreign_ratio"]],
        on=["date", "code", "market"],
        how="left",
    )

    # Handle baseline data
    if baseline is not None and not baseline.empty and "date" in baseline.columns:
        base = baseline.copy()
        base["date"] = pd.to_datetime(base["date"], format="%Y-%m-%d", errors="coerce")
        base = base.dropna(subset=["date"])
        if not base.empty:
            base["date"] = base["date"].dt.date
            merged = merged.merge(
                base[["date", "code", "trust_shares_base", "dealer_shares_base"]],
                on=["date", "code"],
                how="left",
            )
        else:
            merged["trust_shares_base"] = pd.NA
            merged["dealer_shares_base"] = pd.NA
    else:
        merged["trust_shares_base"] = pd.NA
        merged["dealer_shares_base"] = pd.NA

    merged = merged.sort_values(["code", "date"])
    merged["total_shares"] = pd.to_numeric(merged["total_shares"], errors="coerce").fillna(0.0)

    def accumulate(group: pd.DataFrame) -> pd.DataFrame:
        g = group.copy()
        g["trust_net"] = g["trust_net"].astype(float)
        g["dealer_net"] = g["dealer_net"].astype(float)

        g["trust_cum"] = g["trust_net"].cumsum()
        g["dealer_cum"] = g["dealer_net"].cumsum()

        base_trust = pd.to_numeric(g["trust_shares_base"], errors="coerce").fillna(0.0)
        base_dealer = pd.to_numeric(g["dealer_shares_base"], errors="coerce").fillna(0.0)

        base_trust_ff = base_trust.ffill().fillna(0.0)
        base_dealer_ff = base_dealer.ffill().fillna(0.0)

        trust_cum_at_base = g["trust_cum"].where(g["trust_shares_base"].notna()).ffill().fillna(0.0)
        dealer_cum_at_base = g["dealer_cum"].where(g["dealer_shares_base"].notna()).ffill().fillna(0.0)

        g["trust_shares_est"] = base_trust_ff + (g["trust_cum"] - trust_cum_at_base)
        g["dealer_shares_est"] = base_dealer_ff + (g["dealer_cum"] - dealer_cum_at_base)

        # Fallback to pure cumsum if no baseline
        mask_no_base = (base_trust_ff == 0.0) & (base_dealer_ff == 0.0)
        if mask_no_base.all():
            g["trust_shares_est"] = g["trust_cum"]
            g["dealer_shares_est"] = g["dealer_cum"]

        return g

    merged = merged.groupby("code", group_keys=False).apply(accumulate)

    # Calculate ratios
    denom = merged["total_shares"].astype("float64")
    valid = denom > 0.0

    merged["trust_ratio_est"] = 0.0
    merged["dealer_ratio_est"] = 0.0

    merged.loc[valid, "trust_ratio_est"] = (
        merged.loc[valid, "trust_shares_est"].astype(float) / denom[valid] * 100.0
    )
    merged.loc[valid, "dealer_ratio_est"] = (
        merged.loc[valid, "dealer_shares_est"].astype(float) / denom[valid] * 100.0
    )

    # Convert foreign_ratio from Decimal to float for arithmetic operations
    merged["foreign_ratio"] = merged["foreign_ratio"].fillna(0.0).astype(float)
    merged["three_inst_ratio_est"] = (
        merged["foreign_ratio"] + merged["trust_ratio_est"] + merged["dealer_ratio_est"]
    )

    return merged
