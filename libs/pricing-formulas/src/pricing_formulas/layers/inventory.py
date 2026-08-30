def inventory_layer() -> float:
    """Inventory layer stub (ADR-0011 backlog #7, spec 13 §E) — always the
    neutral factor. No anchor exists anywhere in this project today (no
    calendar/availability data source); backlog #11 will need a new
    broadcast CDC source, chained after Stage A2 the same way owner_contracts
    was, before this layer can do anything real. Never emits a
    DecisionComponent (spec 13 §E). Returns 1.0."""
    return 1.0
