from typing import Any

import pandas as pd

from dashboard.hot_path import to_native


def scan_all(table: Any) -> list[dict[str, Any]]:
    """Full Scan — unlike hot_path's Query-by-key pattern, these are small,
    self-cleaning 5-min windowed tables (Phase 26), not a table sized for
    per-key lookups. Paginates through every page, not just the first."""
    items: list[dict[str, Any]] = []
    response = table.scan()
    items.extend(response.get("Items", []))
    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items.extend(response.get("Items", []))
    return items


def windowed_metric_df(table: Any) -> pd.DataFrame:
    """Every row of one Phase 26 windowed-metric table, Decimal-converted
    and sorted by window_start ascending (oldest first) so a line chart
    reads left-to-right in time order. Empty DataFrame if the table has no
    rows yet."""
    items = [to_native(item) for item in scan_all(table)]
    df = pd.DataFrame(items)
    if df.empty:
        return df
    return df.sort_values("window_start").reset_index(drop=True)
