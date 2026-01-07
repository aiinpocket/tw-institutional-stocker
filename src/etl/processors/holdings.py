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

    # Vectorized operations - much faster than groupby().apply()
    merged["trust_net"] = merged["trust_net"].astype(float)
    merged["dealer_net"] = merged["dealer_net"].astype(float)

    # Vectorized cumsum
    merged["trust_cum"] = merged.groupby("code")["trust_net"].cumsum()
    merged["dealer_cum"] = merged.groupby("code")["dealer_net"].cumsum()

    # Process baseline values using transform (faster than apply)
    merged["_base_trust"] = pd.to_numeric(merged["trust_shares_base"], errors="coerce").fillna(0.0)
    merged["_base_dealer"] = pd.to_numeric(merged["dealer_shares_base"], errors="coerce").fillna(0.0)
    merged["_base_trust_ff"] = merged.groupby("code")["_base_trust"].transform(lambda x: x.ffill().fillna(0.0))
    merged["_base_dealer_ff"] = merged.groupby("code")["_base_dealer"].transform(lambda x: x.ffill().fillna(0.0))

    # Get cumsum at baseline points
    merged["_trust_cum_at_base"] = merged["trust_cum"].where(merged["trust_shares_base"].notna())
    merged["_dealer_cum_at_base"] = merged["dealer_cum"].where(merged["dealer_shares_base"].notna())
    merged["_trust_cum_at_base"] = merged.groupby("code")["_trust_cum_at_base"].transform(lambda x: x.ffill().fillna(0.0))
    merged["_dealer_cum_at_base"] = merged.groupby("code")["_dealer_cum_at_base"].transform(lambda x: x.ffill().fillna(0.0))

    # Calculate estimated shares
    merged["trust_shares_est"] = merged["_base_trust_ff"] + (merged["trust_cum"] - merged["_trust_cum_at_base"])
    merged["dealer_shares_est"] = merged["_base_dealer_ff"] + (merged["dealer_cum"] - merged["_dealer_cum_at_base"])

    # Fallback to pure cumsum where no baseline exists
    no_baseline_mask = (merged["_base_trust_ff"] == 0.0) & (merged["_base_dealer_ff"] == 0.0)
    merged.loc[no_baseline_mask, "trust_shares_est"] = merged.loc[no_baseline_mask, "trust_cum"]
    merged.loc[no_baseline_mask, "dealer_shares_est"] = merged.loc[no_baseline_mask, "dealer_cum"]

    # Clean up temp columns
    merged = merged.drop(columns=["_base_trust", "_base_dealer", "_base_trust_ff", "_base_dealer_ff",
                                   "_trust_cum_at_base", "_dealer_cum_at_base"])

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
