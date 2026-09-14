import random
from datetime import date

import psycopg
from common import configure_logging, get_logger

from mock_pm_app.data import (
    build_apartment_pool,
    build_bookings,
    build_company_cost_occurrences,
    build_owner_contracts,
    build_owner_pool,
    build_pricing_strategies,
)
from mock_pm_app.generator import run_forever
from mock_pm_app.migrations import (
    ensure_apartment_market_segments_schema,
    ensure_bookings_schema,
    ensure_company_cost_occurrences_schema,
    ensure_cost_allocation_rules_schema,
    ensure_cost_definitions_schema,
    ensure_manual_overrides_schema,
    ensure_owner_contracts_schema,
    ensure_owners_schema,
    ensure_pricing_strategies_schema,
    migrate_apartment_market_segments_to_pricing_strategies,
    migrate_owner_contracts_to_cost_definitions,
    migrate_payment_lines_to_cost_definitions,
)
from mock_pm_app.seed import (
    already_seeded,
    already_seeded_bookings,
    already_seeded_company_cost_occurrences,
    already_seeded_owner_contracts,
    already_seeded_owners,
    already_seeded_pricing_strategies,
    already_seeded_segments,
    ensure_weighted_allocation_config,
    resolve_cost_definition_ids,
    seed,
    seed_apartment_market_segments,
    seed_bookings,
    seed_company_cost_occurrences,
    seed_owner_contracts,
    seed_owners,
    seed_pricing_strategies,
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
        # Phase 25 (ADR-0018 §2): must run after apartment_market_segments
        # exists (pricing_strategies.apartment_id FKs to it).
        ensure_pricing_strategies_schema(conn)
        logger.info("pricing_strategies_schema_ensured")
        # Phase 25 (ADR-0018 §1): breaking change, backfills any existing
        # apartment_market_segments target_margin/competitiveness_discount
        # into a version=1 pricing_strategies row each, then drops both
        # columns. Must run after both tables above exist. Harmless no-op
        # once already migrated (checked via information_schema, not a
        # flag) or on a fresh install (the columns never existed).
        migrate_apartment_market_segments_to_pricing_strategies(conn)
        logger.info("apartment_market_segments_migrated_to_pricing_strategies")
        ensure_owners_schema(conn)
        logger.info("owners_schema_ensured")
        ensure_owner_contracts_schema(conn)
        logger.info("owner_contracts_schema_ensured")
        # Phase 14 (ADR-0011 backlog #9): schema only, no seed call — this
        # table starts empty and stays empty unless a human inserts a row
        # (spec 14 §1).
        ensure_manual_overrides_schema(conn)
        logger.info("manual_overrides_schema_ensured")
        ensure_bookings_schema(conn)
        logger.info("bookings_schema_ensured")

        # Phase 19 (ADR-0011 backlog #13, ADR-0012): cost_definitions/
        # cost_allocation_rules must exist before resolve_cost_definition_ids
        # and the payment_lines migration (both reference/insert into them).
        ensure_cost_definitions_schema(conn)
        logger.info("cost_definitions_schema_ensured")
        ensure_cost_allocation_rules_schema(conn)
        logger.info("cost_allocation_rules_schema_ensured")

        # Phase 20 (ADR-0013 §4): breaking change, backfills existing
        # owner_contracts rows into a CostDefinition each, then drops
        # commission_base/commission_pct. Must run after both tables above
        # exist. Harmless no-op once already migrated.
        migrate_owner_contracts_to_cost_definitions(conn)
        logger.info("owner_contracts_migrated_to_cost_definitions")

        rng = random.Random()
        apartments = build_apartment_pool(settings.seed_apartments, rng)
        owners = build_owner_pool()

        # Runs every startup (not seed-once) — see resolve_cost_definition_ids'
        # own docstring for why. Must run before the payment_lines migration
        # below, so the canonical specs exist first and the migration's
        # backfill can reuse them where the shape matches.
        concept_to_cost_definition_id = resolve_cost_definition_ids(conn)
        logger.info(
            "cost_definitions_resolved", concepts=len(concept_to_cost_definition_id)
        )
        # office_rent (scope=company) needs an explicit weight_config —
        # Flink's fan-out never discovers the apartment roster itself
        # (spec 19 §5). Fills it in once apartments are known; a no-op once
        # already set.
        ensure_weighted_allocation_config(
            conn, concept_to_cost_definition_id["office_rent"], apartments
        )
        logger.info("office_rent_weight_config_ensured")

        # ADR-0012: breaking change, backfills existing rows, then drops
        # concept/cost_type/is_shared/allocation_ratio. Harmless no-op once
        # already migrated (checked via information_schema, not a flag).
        migrate_payment_lines_to_cost_definitions(conn)
        logger.info("payment_lines_migrated_to_cost_definitions")

        ensure_company_cost_occurrences_schema(conn)
        logger.info("company_cost_occurrences_schema_ensured")

        if already_seeded(conn):
            logger.info("seed_skipped", reason="payment_lines already has rows")
        else:
            rows_inserted = seed(
                conn,
                settings,
                apartments,
                rng,
                date.today(),
                concept_to_cost_definition_id,
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

        if already_seeded_pricing_strategies(conn):
            logger.info(
                "pricing_strategy_seed_skipped",
                reason="pricing_strategies already has rows",
            )
        else:
            strategies = build_pricing_strategies(apartments)
            strategies_inserted = seed_pricing_strategies(conn, strategies)
            logger.info(
                "pricing_strategy_seed_complete", rows_inserted=strategies_inserted
            )

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

        if already_seeded_bookings(conn):
            logger.info("booking_seed_skipped", reason="bookings already has rows")
        else:
            bookings = build_bookings(apartments, rng, date.today())
            bookings_inserted = seed_bookings(conn, bookings)
            logger.info("booking_seed_complete", rows_inserted=bookings_inserted)

        if already_seeded_company_cost_occurrences(conn):
            logger.info(
                "company_cost_occurrence_seed_skipped",
                reason="company_cost_occurrences already has rows",
            )
        else:
            occurrences = build_company_cost_occurrences(
                concept_to_cost_definition_id["office_rent"], date.today()
            )
            occurrences_inserted = seed_company_cost_occurrences(conn, occurrences)
            logger.info(
                "company_cost_occurrence_seed_complete",
                rows_inserted=occurrences_inserted,
            )

        logger.info("generator_starting")
        run_forever(conn, settings, apartments, concept_to_cost_definition_id)


if __name__ == "__main__":
    main()
