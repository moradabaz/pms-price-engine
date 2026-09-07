import random
from datetime import date

import psycopg
from common import configure_logging, get_logger

from mock_pm_app.data import (
    build_apartment_pool,
    build_owner_contracts,
    build_owner_pool,
)
from mock_pm_app.generator import run_forever
from mock_pm_app.migrations import (
    ensure_apartment_market_segments_schema,
    ensure_manual_overrides_schema,
    ensure_owner_contracts_schema,
    ensure_owners_schema,
)
from mock_pm_app.seed import (
    already_seeded,
    already_seeded_owner_contracts,
    already_seeded_owners,
    already_seeded_segments,
    seed,
    seed_apartment_market_segments,
    seed_owner_contracts,
    seed_owners,
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
        ensure_owners_schema(conn)
        logger.info("owners_schema_ensured")
        ensure_owner_contracts_schema(conn)
        logger.info("owner_contracts_schema_ensured")
        # Phase 14 (ADR-0011 backlog #9): schema only, no seed call — this
        # table starts empty and stays empty unless a human inserts a row
        # (spec 14 §1).
        ensure_manual_overrides_schema(conn)
        logger.info("manual_overrides_schema_ensured")

        rng = random.Random()
        apartments = build_apartment_pool(settings.seed_apartments, rng)
        owners = build_owner_pool()

        if already_seeded(conn):
            logger.info("seed_skipped", reason="payment_lines already has rows")
        else:
            rows_inserted = seed(conn, settings, apartments, rng, date.today())
            logger.info("seed_complete", rows_inserted=rows_inserted)

        if already_seeded_segments(conn):
            logger.info(
                "segment_seed_skipped",
                reason="apartment_market_segments already has rows",
            )
        else:
            segments_inserted = seed_apartment_market_segments(conn, apartments)
            logger.info("segment_seed_complete", rows_inserted=segments_inserted)

        if already_seeded_owners(conn):
            logger.info("owner_seed_skipped", reason="owners already has rows")
        else:
            owners_inserted = seed_owners(conn, owners)
            logger.info("owner_seed_complete", rows_inserted=owners_inserted)

        if already_seeded_owner_contracts(conn):
            logger.info(
                "owner_contract_seed_skipped",
                reason="owner_contracts already has rows",
            )
        else:
            contracts = build_owner_contracts(apartments, owners, rng)
            contracts_inserted = seed_owner_contracts(conn, contracts)
            logger.info(
                "owner_contract_seed_complete", rows_inserted=contracts_inserted
            )

        logger.info("generator_starting")
        run_forever(conn, settings, apartments)


if __name__ == "__main__":
    main()
