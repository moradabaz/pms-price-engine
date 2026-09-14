import json
import random
from datetime import date, timedelta
from typing import Any

from mock_pm_app.bookings_rows import insert_booking
from mock_pm_app.data import (
    CONCEPT_PROFILES,
    COST_DEFINITION_SPECS,
    Apartment,
    Booking,
    CompanyCostOccurrenceSeed,
    CostDefinitionSpec,
    Owner,
    OwnerContract,
    PricingStrategy,
)
from mock_pm_app.rows import build_historical_row, insert_row
from mock_pm_app.settings import MockAppSettings


def already_seeded(conn: Any) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM payment_lines")
        (count,) = cur.fetchone()
    return bool(count > 0)


def already_seeded_segments(conn: Any) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM apartment_market_segments")
        (count,) = cur.fetchone()
    return bool(count > 0)


def already_seeded_pricing_strategies(conn: Any) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM pricing_strategies")
        (count,) = cur.fetchone()
    return bool(count > 0)


def already_seeded_owners(conn: Any) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM owners")
        (count,) = cur.fetchone()
    return bool(count > 0)


def already_seeded_owner_contracts(conn: Any) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM owner_contracts")
        (count,) = cur.fetchone()
    return bool(count > 0)


def already_seeded_bookings(conn: Any) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM bookings")
        (count,) = cur.fetchone()
    return bool(count > 0)


def already_seeded_company_cost_occurrences(conn: Any) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM company_cost_occurrences")
        (count,) = cur.fetchone()
    return bool(count > 0)


def seed_apartment_market_segments(conn: Any, apartments: list[Apartment]) -> int:
    # Decision C.1: seed once, deterministically, from mock-pm-app's own
    # apartment pool. Phase 8's four Bonus/Malus attributes are set here —
    # generated per apartment (data.py's build_apartment_pool). Phase 25
    # (ADR-0018 §1): target_margin/competitiveness_discount moved to their
    # own pricing_strategies table (see seed_pricing_strategies below) — no
    # longer this function's concern at all.
    with conn.cursor() as cur:
        for apartment in apartments:
            cur.execute(
                """
                INSERT INTO apartment_market_segments
                    (apartment_id, apartment_reference, city, neighborhood,
                     property_type, bedrooms, quality_tier, rating,
                     has_view, has_parking)
                VALUES (%(apartment_id)s, %(apartment_reference)s, %(city)s,
                        %(neighborhood)s, %(property_type)s, %(bedrooms)s,
                        %(quality_tier)s, %(rating)s, %(has_view)s, %(has_parking)s)
                ON CONFLICT (apartment_id) DO NOTHING
                """,
                {
                    "apartment_id": apartment.apartment_id,
                    "apartment_reference": apartment.apartment_reference,
                    "city": apartment.city,
                    "neighborhood": apartment.neighborhood,
                    "property_type": apartment.property_type,
                    "bedrooms": apartment.bedrooms,
                    "quality_tier": apartment.quality_tier,
                    "rating": apartment.rating,
                    "has_view": apartment.has_view,
                    "has_parking": apartment.has_parking,
                },
            )
    conn.commit()
    return len(apartments)


def seed_pricing_strategies(conn: Any, strategies: list[PricingStrategy]) -> int:
    # Phase 25 (ADR-0018 §2): one version=1 row per apartment, from
    # data.py's build_pricing_strategies() — the seed-time equivalent of the
    # backfill migration's own version=1 row for a pre-existing deployment.
    # ON CONFLICT DO NOTHING on (apartment_id, version): safe to call again
    # against a volume that already has rows (a no-op, not a duplicate).
    with conn.cursor() as cur:
        for strategy in strategies:
            cur.execute(
                """
                INSERT INTO pricing_strategies
                    (apartment_id, version, target_margin,
                     competitiveness_discount, floor_policy_default)
                VALUES (%(apartment_id)s, %(version)s, %(target_margin)s,
                        %(competitiveness_discount)s, %(floor_policy_default)s)
                ON CONFLICT (apartment_id, version) DO NOTHING
                """,
                {
                    "apartment_id": strategy.apartment_id,
                    "version": strategy.version,
                    "target_margin": strategy.target_margin,
                    "competitiveness_discount": strategy.competitiveness_discount,
                    "floor_policy_default": strategy.floor_policy_default,
                },
            )
    conn.commit()
    return len(strategies)


def new_strategy_version(conn: Any, apartment_id: str, **changes: Any) -> int:
    """Inserts the next PricingStrategy version for one apartment — never an
    UPDATE (Phase 25, ADR-0018 §2: insert-only is what makes 'future-only,
    no replay' concrete at the data layer, not just a documented intention).
    `changes` overrides target_margin/competitiveness_discount/
    floor_policy_default from the current highest version; any field not
    passed carries over unchanged. Returns the new version number."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT target_margin, competitiveness_discount, floor_policy_default,
                   version
            FROM pricing_strategies
            WHERE apartment_id = %(apartment_id)s
            ORDER BY version DESC
            LIMIT 1
            """,
            {"apartment_id": apartment_id},
        )
        row = cur.fetchone()
        if row is None:
            raise ValueError(
                f"no existing pricing_strategies row for apartment_id={apartment_id!r}"
            )
        (
            target_margin,
            competitiveness_discount,
            floor_policy_default,
            current_version,
        ) = row
        new_version = int(current_version) + 1
        cur.execute(
            """
            INSERT INTO pricing_strategies
                (apartment_id, version, target_margin,
                 competitiveness_discount, floor_policy_default)
            VALUES (%(apartment_id)s, %(version)s, %(target_margin)s,
                    %(competitiveness_discount)s, %(floor_policy_default)s)
            """,
            {
                "apartment_id": apartment_id,
                "version": new_version,
                "target_margin": changes.get("target_margin", target_margin),
                "competitiveness_discount": changes.get(
                    "competitiveness_discount", competitiveness_discount
                ),
                "floor_policy_default": changes.get(
                    "floor_policy_default", floor_policy_default
                ),
            },
        )
    conn.commit()
    return new_version


def seed_owners(conn: Any, owners: list[Owner]) -> int:
    # Phase 11 (ADR-0011 backlog #5): seeded once, deterministically, from
    # data.py's build_owner_pool() — same shape as seed_apartment_market_segments.
    with conn.cursor() as cur:
        for owner in owners:
            cur.execute(
                """
                INSERT INTO owners (owner_id, owner_name)
                VALUES (%(owner_id)s, %(owner_name)s)
                ON CONFLICT (owner_id) DO NOTHING
                """,
                {"owner_id": owner.owner_id, "owner_name": owner.owner_name},
            )
    conn.commit()
    return len(owners)


def resolve_owner_commission_cost_definition_id(
    conn: Any, revenue_base: str, rate: float
) -> str:
    """Phase 20 (ADR-0013 §4): the same upsert-by-shape pattern
    resolve_cost_definition_ids() established, specialized for
    concept='owner_commission' — one CostDefinition per distinct
    (revenue_base, rate) pair, since (unlike the other 13 concepts) this one
    genuinely varies per apartment/contract, not just once globally. Reuses
    an existing row when the exact same (revenue_base, rate) pair is already
    present. Returns the cost_definition_id."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT cost_definition_id FROM cost_definitions
            WHERE concept = 'owner_commission'
              AND revenue_base = %(revenue_base)s AND rate = %(rate)s
            """,
            {"revenue_base": revenue_base, "rate": rate},
        )
        existing = cur.fetchone()
        if existing is not None:
            return str(existing[0])

        cur.execute(
            """
            INSERT INTO cost_definitions
                (concept, scope, behavior, trigger, calculation_base,
                 recurrence, revenue_base, rate)
            VALUES ('owner_commission', 'booking', 'variable', 'revenue',
                    'pct_adjusted_revenue', 'per_booking', %(revenue_base)s,
                    %(rate)s)
            RETURNING cost_definition_id
            """,
            {"revenue_base": revenue_base, "rate": rate},
        )
        (cost_definition_id,) = cur.fetchone()

        cur.execute(
            """
            INSERT INTO cost_allocation_rules (cost_definition_id, method)
            VALUES (%(cost_definition_id)s, 'direct')
            """,
            {"cost_definition_id": cost_definition_id},
        )
    conn.commit()
    return str(cost_definition_id)


def seed_owner_contracts(conn: Any, contracts: list[OwnerContract]) -> int:
    # Phase 11 (ADR-0011 backlog #5): one contract per apartment, from
    # data.py's build_owner_contracts() — must run after both
    # seed_apartment_market_segments (FK to apartment_id) and seed_owners
    # (FK to owner_id). Phase 20 (ADR-0013 §4): commission_base/commission_pct
    # are resolved into a CostDefinition first, then only cost_definition_id
    # is stored on the contract row itself.
    with conn.cursor() as cur:
        for contract in contracts:
            cost_definition_id = resolve_owner_commission_cost_definition_id(
                conn, contract.commission_base, contract.commission_pct
            )
            cur.execute(
                """
                INSERT INTO owner_contracts
                    (apartment_id, owner_id, cost_definition_id)
                VALUES (%(apartment_id)s, %(owner_id)s, %(cost_definition_id)s)
                ON CONFLICT (apartment_id) DO NOTHING
                """,
                {
                    "apartment_id": contract.apartment_id,
                    "owner_id": contract.owner_id,
                    "cost_definition_id": cost_definition_id,
                },
            )
    conn.commit()
    return len(contracts)


def resolve_cost_definition_ids(
    conn: Any, specs: list[CostDefinitionSpec] = COST_DEFINITION_SPECS
) -> dict[str, str]:
    """Phase 19 (ADR-0011 backlog #13): upsert-by-shape, not seed-once. Every
    mock-pm-app startup needs this map (seeding new historical rows, and
    run_forever's ongoing live inserts both need a cost_definition_id per
    concept) — unlike every other already_seeded_*-guarded table here, a
    plain "skip if already seeded" would return nothing on a restart where
    seeding was already done. For each spec: reuse the existing
    CostDefinition if one with the exact same shape already exists
    (matches on every dimension, not just concept — the payment_lines
    backfill migration may have already created a legacy definition for the
    same concept with a different recurrence/behavior, which must stay
    distinct, not be silently reused here); otherwise insert it (plus its
    CostAllocationRule) fresh. Returns {concept: cost_definition_id}."""
    concept_to_id: dict[str, str] = {}
    with conn.cursor() as cur:
        for spec in specs:
            cur.execute(
                """
                SELECT cost_definition_id FROM cost_definitions
                WHERE concept = %(concept)s AND scope = %(scope)s
                  AND behavior = %(behavior)s AND trigger = %(trigger)s
                  AND calculation_base = %(calculation_base)s
                  AND recurrence = %(recurrence)s
                  AND revenue_base IS NOT DISTINCT FROM %(revenue_base)s
                  AND rate IS NOT DISTINCT FROM %(rate)s
                """,
                {
                    "concept": spec.concept,
                    "scope": spec.scope,
                    "behavior": spec.behavior,
                    "trigger": spec.trigger,
                    "calculation_base": spec.calculation_base,
                    "recurrence": spec.recurrence,
                    "revenue_base": spec.revenue_base,
                    "rate": spec.rate,
                },
            )
            existing = cur.fetchone()
            if existing is not None:
                concept_to_id[spec.concept] = str(existing[0])
                continue

            cur.execute(
                """
                INSERT INTO cost_definitions
                    (concept, scope, behavior, trigger, calculation_base,
                     recurrence, revenue_base, rate)
                VALUES (%(concept)s, %(scope)s, %(behavior)s, %(trigger)s,
                        %(calculation_base)s, %(recurrence)s, %(revenue_base)s,
                        %(rate)s)
                RETURNING cost_definition_id
                """,
                {
                    "concept": spec.concept,
                    "scope": spec.scope,
                    "behavior": spec.behavior,
                    "trigger": spec.trigger,
                    "calculation_base": spec.calculation_base,
                    "recurrence": spec.recurrence,
                    "revenue_base": spec.revenue_base,
                    "rate": spec.rate,
                },
            )
            (cost_definition_id,) = cur.fetchone()
            concept_to_id[spec.concept] = str(cost_definition_id)

            cur.execute(
                """
                INSERT INTO cost_allocation_rules
                    (cost_definition_id, method, weight_config)
                VALUES (%(cost_definition_id)s, %(method)s, %(weight_config)s)
                """,
                {
                    "cost_definition_id": cost_definition_id,
                    "method": spec.allocation_method,
                    "weight_config": spec.weight_config,
                },
            )
    conn.commit()
    return concept_to_id


def ensure_weighted_allocation_config(
    conn: Any, cost_definition_id: str, apartments: list[Apartment]
) -> None:
    """Phase 19: Flink's company-cost fan-out (stage_company_cost_enrichment.py)
    never discovers the apartment roster itself — weight_config must always
    be explicit for a 'weighted' allocation rule (ADR-0012's own "never
    assume Total/N automatically" principle, applied literally: an equal
    split is still an explicit choice, computed here once apartments are
    known, not inferred at read time). Only fills weight_config when it is
    still NULL — a human who later hand-edits a real weighting is never
    silently overwritten on the next restart."""
    weight_config = json.dumps(
        {
            apartment.apartment_id: round(1.0 / len(apartments), 6)
            for apartment in apartments
        }
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE cost_allocation_rules
            SET weight_config = %(weight_config)s
            WHERE cost_definition_id = %(cost_definition_id)s
              AND weight_config IS NULL
            """,
            {
                "weight_config": weight_config,
                "cost_definition_id": cost_definition_id,
            },
        )
    conn.commit()


def seed_company_cost_occurrences(
    conn: Any, occurrences: list[CompanyCostOccurrenceSeed]
) -> int:
    with conn.cursor() as cur:
        for occurrence in occurrences:
            cur.execute(
                """
                INSERT INTO company_cost_occurrences
                    (cost_definition_id, billing_period_start, billing_period_end,
                     amount_gross, description)
                VALUES (%(cost_definition_id)s, %(billing_period_start)s,
                        %(billing_period_end)s, %(amount_gross)s, %(description)s)
                """,
                {
                    "cost_definition_id": occurrence.cost_definition_id,
                    "billing_period_start": occurrence.billing_period_start,
                    "billing_period_end": occurrence.billing_period_end,
                    "amount_gross": occurrence.amount_gross,
                    "description": occurrence.description,
                },
            )
    conn.commit()
    return len(occurrences)


def _month_bounds(months_ago: int, today: date) -> tuple[date, date]:
    year = today.year
    month = today.month - months_ago
    while month <= 0:
        month += 12
        year -= 1
    start = date(year, month, 1)
    next_month = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
    end = next_month - timedelta(days=1)
    return start, end


def seed(
    conn: Any,
    settings: MockAppSettings,
    apartments: list[Apartment],
    rng: random.Random,
    today: date,
    concept_to_cost_definition_id: dict[str, str],
) -> int:
    rows: list[dict[str, Any]] = []
    for apartment in apartments:
        for months_ago in range(1, settings.seed_months + 1):
            period_start, period_end = _month_bounds(months_ago, today)
            profiles = rng.sample(
                CONCEPT_PROFILES, k=rng.randint(3, len(CONCEPT_PROFILES))
            )
            for profile in profiles:
                rows.append(
                    build_historical_row(
                        apartment,
                        profile,
                        concept_to_cost_definition_id[profile.concept],
                        period_start,
                        period_end,
                        rng,
                    )
                )

    with conn.cursor() as cur:
        for row in rows:
            insert_row(cur, row)
    conn.commit()
    return len(rows)


def seed_bookings(conn: Any, bookings: list[Booking]) -> int:
    # Phase 18 (ADR-0011 backlog #13 prerequisite): one seed pass, no
    # ON CONFLICT guard needed — booking_id is server-generated
    # (gen_random_uuid()), so a re-run would only be skipped by
    # already_seeded_bookings() above, same as payment_lines' own seed().
    with conn.cursor() as cur:
        for booking in bookings:
            insert_booking(cur, booking)
    conn.commit()
    return len(bookings)
