# ADR-0002 — PyFlink over Java/Scala Flink

**Date:** 2026-07-01
**Status:** Accepted

## Context

Apache Flink is the stream processing engine for this project (cost aggregation, multi-stream join, pricing rule engine). Flink's most mature, best-documented, and most widely deployed-in-production API surface is Java/Scala. The Python API (PyFlink) is a thinner layer on top of the JVM engine — it is fully capable for DataStream and Table API workloads, but has historically lagged Java in feature availability, has an extra JVM↔Python serialization hop for user-defined functions, and has a smaller body of production war stories to learn from.

The rest of this repository — the market-ingestor service, the shared schemas, the dashboard — is Python. The question was whether to break that consistency for the stream processing layer, where Java is the ecosystem's default choice, or to stay in a single language across the whole stack.

## Decision

Use **PyFlink** for all Flink jobs (cost aggregation, the payment/market stream join, and the pricing rule engine).

## Rationale

- **Single-language stack.** Every other component (market-ingestor, shared-schemas, dashboard, contract tests) is Python. A Java/Scala Flink job would force this project's only developer to context-switch languages, build tools (uv vs. Maven/sbt), and dependency ecosystems for one component. That cost is not justified by a PoC with one maintainer.
- **The learning goal is stream processing concepts, not the JVM.** This project's stated purpose is to learn CDC, stateful joins, watermarking, and windowing — none of which are Java-specific. PyFlink exposes the same DataStream/Table API concepts; the JVM-vs-Python choice is orthogonal to that learning goal.
- **The pricing rule engine is business logic, not a performance-critical hot loop.** `streaming/flink-jobs/src/flink_jobs/pricing/` is a small, testable set of deterministic rules operating on aggregated, already-low-volume data (per-apartment, not per-event at high throughput). PyFlink's Python UDF serialization overhead is a real cost at high event rates; it is not a meaningful cost here (~100 apartments, low-frequency pricing decisions).
- **Pydantic models from `libs/shared-schemas` can be reused directly** inside PyFlink UDFs for validation, instead of maintaining a parallel Java POJO/Avro representation of the same three event schemas.

## Consequences

- Flink jobs pay a JVM↔Python serialization cost per record for any Python UDF (map/join/process functions). At this project's volume (dozens of apartments, cost lines arriving at human timescales, not IoT-scale throughput) this is not expected to be a bottleneck; if it becomes one, the mitigation is to push the hot path into Table API/SQL (which executes on the JVM directly) rather than switching languages.
- PyFlink's connector ecosystem must cover both sources needed here: a Kafka source (`payment-events.v1`) and a Kinesis source (`market-price-events`). Both are supported in PyFlink 2.x DataStream API (confirmed before adopting this ADR); this is a hard dependency of the decision and should be re-verified if the Flink version pinned in `streaming/flink-jobs/pyproject.toml` changes.
- Debugging PyFlink jobs means debugging across a Python/JVM boundary (stack traces from UDF failures surface through Py4J). This is a known ergonomic cost of the PyFlink API, accepted here in exchange for stack consistency.
- If a future phase needs sub-second, high-throughput processing (outside this PoC's scope), revisit this decision — Java/Scala Flink remains the better choice at that scale.
