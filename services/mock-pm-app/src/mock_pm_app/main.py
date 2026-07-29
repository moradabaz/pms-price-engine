import random
from datetime import date

import psycopg
from common import configure_logging, get_logger

from mock_pm_app.data import build_apartment_pool
from mock_pm_app.generator import run_forever
from mock_pm_app.migrations import ensure_apartment_market_segments_schema
from mock_pm_app.seed import (
    already_seeded,
    already_seeded_segments,
    seed,
    seed_apartment_market_segments,
)
from mock_pm_app.settings import MockAppSettings


def main() -> None:
    settings = MockAppSettings()
    configure_logging(settings.log_level)
    logger = get_logger(__name__)

    with psycopg.connect(settings.postgres_dsn, autocommit=False) as conn:
        # Self-healing migration: makes apartment_market_segments exist even
        # on a volume from before this table did — see migrations.py's own
        # header for why this can't rely on docker-entrypoint-initdb.d alone.
        ensure_apartment_market_segments_schema(conn)
        logger.info("apartment_market_segments_schema_ensured")

        apartments = build_apartment_pool(settings.seed_apartments)

        if already_seeded(conn):
            logger.info("seed_skipped", reason="payment_lines already has rows")
        else:
            rows_inserted = seed(
                conn, settings, apartments, random.Random(), date.today()
            )
            logger.info("seed_complete", rows_inserted=rows_inserted)

        if already_seeded_segments(conn):
            logger.info(
                "segment_seed_skipped",
                reason="apartment_market_segments already has rows",
            )
        else:
            segments_inserted = seed_apartment_market_segments(conn, apartments)
            logger.info("segment_seed_complete", rows_inserted=segments_inserted)

        logger.info("generator_starting")
        run_forever(conn, settings, apartments)


if __name__ == "__main__":
    main()
