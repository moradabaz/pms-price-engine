from datetime import UTC, date, datetime

from flink_jobs.models import CostAggregate
from flink_jobs.stage_price_decision import PriceDecisionFunction
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from shared_schemas.market_price import (
    MarketArea,
    MarketContext,
    MarketPrice,
    Pricing,
    PropertyProfile,
)


def _cost(apartment_id: str) -> CostAggregate:
    return CostAggregate(
        apartment_id=apartment_id,
        apartment_reference=apartment_id,
        city="Barcelona",
        neighborhood="Eixample",
        property_type="studio",
        bedrooms=0,
        fixed_cost_eur=0.0,
        variable_cost_eur=100.0,
        one_time_cost_eur=0.0,
        total_monthly_cost_eur=3000.0,
        available_days=30,
        cost_lines_count=1,
        billing_period_start=date(2026, 6, 1),
        billing_period_end=date(2026, 6, 30),
        target_margin=0.05,
        competitiveness_discount=0.05,
        commission_pct=0.15,
        updated_at=datetime.now(UTC),
    )


def _market(target_date: str) -> MarketPrice:
    return MarketPrice(
        event_id="00000000-0000-0000-0000-000000000001",
        market_area=MarketArea(
            city="Barcelona", neighborhood="Eixample", country_code="ES"
        ),
        property_profile=PropertyProfile(type="studio", bedrooms=0),
        target_date=target_date,
        pricing=Pricing(avg_nightly_rate=150.0),
        market_context=MarketContext(sample_size=20, data_source="mock"),
        collected_at=datetime.now(UTC),
    )


def test_stage_b_fan_out_shape_on_a_real_minicluster():
    """MiniCluster smoke test (spec §13): runs Stage B's real connect()/keyBy
    wiring, not the in-memory doubles the other tests use. Asserts the exact
    cost x market cross product AC-03/AC-04 require. Each (apartment, night)
    pair is emitted exactly once regardless of arrival interleaving — whichever
    side of the pair lands second triggers its emission — so this holds
    without needing to control the two streams' relative ordering."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    apartments = ["apt-A", "apt-B"]
    dates = ["2026-09-01", "2026-09-02", "2026-09-03"]

    # from_collection bridges elements through a Java-side pickle decoder that
    # only understands basic types — plain strings in, our dataclasses/pydantic
    # models built by a .map() UDF instead (same split KafkaSource + .map()
    # uses in job.py), never handed to from_collection directly.
    cost_stream = (
        env.from_collection(apartments, type_info=Types.STRING())
        .map(_cost, output_type=Types.PICKLED_BYTE_ARRAY())
        .key_by(lambda ca: ca.segment_key)
    )

    market_stream = (
        env.from_collection(dates, type_info=Types.STRING())
        .map(_market, output_type=Types.PICKLED_BYTE_ARRAY())
        .key_by(
            lambda mp: (
                mp.market_area.city,
                mp.market_area.neighborhood,
                mp.property_profile.type,
                mp.property_profile.bedrooms,
            )
        )
    )

    price_decisions = cost_stream.connect(market_stream).process(
        PriceDecisionFunction()
    )

    results = list(price_decisions.execute_and_collect())

    assert len(results) == len(apartments) * len(dates)
    pairs = {(d.apartment_id, str(d.target_date)) for d in results}
    assert pairs == {(apt, d) for apt in apartments for d in dates}
