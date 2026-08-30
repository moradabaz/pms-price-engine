def performance_layer() -> float:
    """Performance layer stub (ADR-0011 backlog #7, spec 13 §E) — always the
    neutral factor, until backlog #11 supplies a real occupancy/demand
    signal to act on. No parameters: adding them ahead of real logic to
    consume them would be speculative (YAGNI); backlog #11 adds both
    together, the same pattern Phase 11 followed for commission_netting_eur.
    Never emits a DecisionComponent — a permanently-neutral stub can't
    explain anything (spec 13 §E). Returns 1.0."""
    return 1.0
