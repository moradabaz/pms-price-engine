import random
from datetime import date

import psycopg
from common import configure_logging, get_logger

from mock_pm_app.data import build_apartment_pool
from mock_pm_app.generator import run_forever
from mock_pm_app.seed import already_seeded, seed
from mock_pm_app.settings import MockAppSettings


def main() -> None:
    settings = MockAppSettings()
    configure_logging(settings.log_level)
    logger = get_logger(__name__)

    conn = psycopg.connect(settings.postgres_dsn, autocommit=False)
    apartments = build_apartment_pool(settings.seed_apartments)

    if already_seeded(conn):
        logger.info("seed_skipped", reason="payment_lines already has rows")
    else:
        rows_inserted = seed(conn, settings, apartments, random.Random(), date.today())
        logger.info("seed_complete", rows_inserted=rows_inserted)

    logger.info("generator_starting")
    run_forever(conn, settings, apartments)


if __name__ == "__main__":
    main()
