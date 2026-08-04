"""Cross-check that transform/seeds/seasonality.csv matches
services/market-ingestor/src/market_ingestor/seasonality.py exactly (spec 05
§7, AC-08) — two hand-kept-in-sync copies of the same seasonality table, same
contract-test spirit as the JSON Schema fixtures above, just checking two
independently-maintained artifacts instead of a schema against fixtures.
Loads seasonality.py by file path rather than as a package import so this
suite doesn't need market-ingestor's own dependencies (boto3, etc.) declared
here just for one dependency-free pure function."""

import csv
import importlib.util
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SEASONALITY_CSV = REPO_ROOT / "transform" / "seeds" / "seasonality.csv"
SEASONALITY_PY = (
    REPO_ROOT
    / "services"
    / "market-ingestor"
    / "src"
    / "market_ingestor"
    / "seasonality.py"
)


def _load_seasonality_module():
    spec = importlib.util.spec_from_file_location("seasonality", SEASONALITY_PY)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_csv_multipliers() -> dict[int, float]:
    with SEASONALITY_CSV.open(newline="") as f:
        return {
            int(row["month"]): float(row["multiplier"]) for row in csv.DictReader(f)
        }


def test_seasonality_csv_matches_seasonality_py_for_every_month():
    seasonality = _load_seasonality_module()
    csv_multipliers = _load_csv_multipliers()

    assert set(csv_multipliers) == set(range(1, 13))
    for month in range(1, 13):
        expected = seasonality.seasonal_multiplier(date(2026, month, 15))
        assert csv_multipliers[month] == expected, (
            f"month {month}: csv says {csv_multipliers[month]}, "
            f"seasonality.py says {expected}"
        )
