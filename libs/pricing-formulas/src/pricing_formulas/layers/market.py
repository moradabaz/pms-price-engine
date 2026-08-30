def market_reference_price(
    property_reference_price_eur: float, competitiveness_discount: float
) -> float:
    """Market layer (ADR-0011 backlog #7, spec 13 §3) — the property
    reference price, discounted to stay competitive. Pure extraction of
    engine.py's former inline multiplication; unrounded, rounded once by the
    caller. No DecisionComponent: a discount setting isn't itself a decision
    to explain (spec 13 §D). This is the intended landing spot for backlog
    #11's real market data (occupancy_rate/sample_size), currently dead
    pass-through data unused by pricing. Returns the market reference
    price."""
    return property_reference_price_eur * (1 - competitiveness_discount)
