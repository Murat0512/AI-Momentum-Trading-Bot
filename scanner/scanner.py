import numpy as np
import pandas as pd
from typing import Dict, List

from config.settings import CONFIG


class MomentumScanner:
    def __init__(self):
        self.config = CONFIG.scanner

    def calculate_demand_score(self, row: pd.Series) -> float:
        """
        Calculates a composite score (0.0 to 1.0) based on config weights.
        Assumes 'row' contains normalized indicators.
        """
        score = (
            (float(row.get("rvol_norm", 0.0)) * self.config.weight_rvol)
            + (float(row.get("gap_norm", 0.0)) * self.config.weight_gap)
            + (float(row.get("range_norm", 0.0)) * self.config.weight_intraday_range)
            + (float(row.get("vol_spike_z", 0.0)) * self.config.weight_volume_spike_z)
            + (
                float(row.get("flow_momentum", 0.0))
                * self.config.weight_dollar_flow_momentum
            )
        )
        return float(np.clip(score, 0.0, 1.0))

    def filter_universe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Applies hard filters defined in ScannerConfig.
        """
        if df is None or df.empty:
            return pd.DataFrame()

        required_cols = {
            "price",
            "dollar_volume",
            "spread_pct",
            "gap_pct",
            "intraday_range_pct",
            "rvol_norm",
            "gap_norm",
            "range_norm",
            "vol_spike_z",
            "flow_momentum",
        }
        missing = required_cols.difference(df.columns)
        if missing:
            raise ValueError(
                f"MomentumScanner.filter_universe missing required columns: {sorted(missing)}"
            )

        mask = (
            (df["price"] >= self.config.min_price)
            & (df["price"] <= self.config.max_price)
            & (df["dollar_volume"] >= self.config.min_dollar_volume)
            & (df["spread_pct"] <= self.config.max_spread_pct)
        )

        catalyst_mask = (df["gap_pct"] >= self.config.min_gap_pct) | (
            df["intraday_range_pct"] >= self.config.min_intraday_range_pct
        )

        filtered_df = df[mask & catalyst_mask].copy()

        if filtered_df.empty:
            return filtered_df

        filtered_df["demand_score"] = filtered_df.apply(
            self.calculate_demand_score, axis=1
        )
        return filtered_df.sort_values(by="demand_score", ascending=False).head(
            self.config.top_n
        )
