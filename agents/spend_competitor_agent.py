"""Spend Competitor Agent.

This agent performs deterministic competitor analysis inside the supplied spend file. It
finds peer suppliers in the same L2 category and ranks them by total spend.
"""

from __future__ import annotations

from typing import Dict

import pandas as pd

from .common import spend_fmt


class SpendCompetitorAgent:
    """Identify internal/in-file competitor or substitute suppliers by category."""

    name = "Spend-Internal Competitor Agent"

    def competitors_within_spend(self, dataframe: pd.DataFrame) -> Dict[int, str]:
        """Return a row-indexed mapping of same-L2 in-file peer suppliers."""
        result: Dict[int, str] = {}
        grouped = (
            dataframe.groupby(["L2", "Cleansed Vendor Name"], dropna=False)["Total Spend"]
            .sum()
            .reset_index()
        )
        for idx, row in dataframe.iterrows():
            l2 = row["L2"]
            vendor = row["Cleansed Vendor Name"]
            pool = grouped[(grouped["L2"] == l2) & (grouped["Cleansed Vendor Name"] != vendor)].copy()
            pool = pool.sort_values("Total Spend", ascending=False).head(5)
            if pool.empty:
                result[idx] = "No direct in-file peer found in same L2 category"
            else:
                result[idx] = "; ".join(
                    f"{peer['Cleansed Vendor Name']} ({spend_fmt(peer['Total Spend'])})"
                    for _, peer in pool.iterrows()
                )
        return result
