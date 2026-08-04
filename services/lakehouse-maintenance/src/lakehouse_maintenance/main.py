import time

from common import configure_logging, get_logger
from lakehouse_shared import build_catalog
from pyiceberg.catalog import Catalog

from lakehouse_maintenance.maintenance import compact_once
from lakehouse_maintenance.settings import MaintenanceSettings


def run_forever(catalog: Catalog, settings: MaintenanceSettings) -> None:
    logger = get_logger(__name__)
    while True:
        started = time.monotonic()
        rows = compact_once(catalog, settings)
        logger.info("compaction_tick", rows_compacted=rows)

        elapsed = time.monotonic() - started
        # Run, then sleep the *remainder* of the interval (spec 05 §10) — a
        # fixed sleep would let a slow tick stack on top of the next one.
        time.sleep(max(0.0, settings.compaction_interval_seconds - elapsed))


def main() -> None:
    settings = MaintenanceSettings()
    configure_logging(settings.log_level)

    catalog = build_catalog(settings)
    run_forever(catalog, settings)


if __name__ == "__main__":
    main()
