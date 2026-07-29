# Flink operational checklist — things this project's design conversation almost missed

**Phase:** 4 (Flink processing)
**Component:** `streaming/flink-jobs/` (not implemented yet)
**Date written:** 2026-07-29
**Status:** Instructional, not an incident. Nothing here has failed live — this is a checklist of genuine Flink/PyFlink operational sharp edges that came up while writing `specs/phases/04-flink-processing/spec.md`, several of which the design conversation (`docs/phase-4-streaming-design-decisions.md`) did not originally account for. Unlike `error-handling/anticipated-risks-flink-processing.md` (risks specific to *this job's* design choices), this file is closer to a general Flink manual — things worth knowing about the engine itself, cross-referenced to where they matter in this project. Treat each item as a checklist entry to verify during implementation (task: "Implementar `streaming/flink-jobs/`"), not as settled fact until verified against the actual running job.

---

## 1. Set `max parallelism` explicitly, before the first checkpoint ever taken

**The gotcha:** Flink partitions keyed state into a fixed number of *key groups*, determined by `max parallelism` — not by however many parallel subtasks the job happens to run with today. If `max parallelism` is never set explicitly, Flink derives a default from the job's *initial* operator parallelism (roughly `clamp(nextPowerOfTwo(parallelism × 1.5), 128, 32768)`). **This derived value becomes a hard ceiling the moment a checkpoint/savepoint is taken** — rescaling the job's actual parallelism later is only possible up to that ceiling; going beyond it requires the State Processor API to manually rewrite the key-group layout, not a normal savepoint restore.

**Why this matters here specifically:** Decision H already accepts that Stage A tops out at Kafka's 6 partitions and Stage B at Kinesis's 4 shards *today*. If a future hardening pass (or the README's ~100-apartment target scale) ever needs more parallelism than that, an unset `max parallelism` derived from `parallelism=6` could compute to something as low as 128 — probably fine, but this is exactly the kind of implicit default this project has already been burned by twice (Debezium's default topic name, default partition key — both "Fase 2" `error-handling/` write-ups). Don't let a third instance of "silently accepted the tool's default" happen here.

**What to do:** set `env.set_max_parallelism(...)` explicitly at job-graph construction time in `streaming/flink-jobs/`, to a generous value (128 is Flink's own commonly recommended default) — a deliberate decision recorded in the job's setup code, not whatever the framework happens to compute from today's parallelism.

---

## 2. The Broadcast State Pattern gives ordering *within* the broadcast side, not *across* the broadcast and keyed sides

**The gotcha:** Flink guarantees every parallel subtask of a `KeyedBroadcastProcessFunction` sees broadcast elements in the same relative order as every other subtask (so broadcast state stays consistent across parallelism) — but it makes **no guarantee about the relative timing** between an element arriving on the broadcast side (`processBroadcastElement`) and an element arriving on the keyed side (`processElement`) at the same wall-clock moment. Two events that are "simultaneous" from an external point of view can be delivered to the operator in either order.

**Why this matters here:** this is precisely the "Stage A's broadcast-not-yet-arrived race" already named in `specs/phases/04-flink-processing/spec.md` §6 and §14 — an apartment's first cost event can genuinely arrive before its `apartment_market_segments` broadcast entry does, with no ordering fix available at the Flink level. The spec already accepts this as a narrow, unfixed startup race (log-and-skip, no buffering) — this entry exists so that decision is understood as **a fundamental property of the Broadcast State Pattern**, not a bug to chase down later. Don't spend implementation time trying to "fix" an ordering guarantee Flink was never going to provide.

---

## 3. A `KeyedProcessFunction`'s timers are scoped to the operator's key — not to whatever sub-entity you're actually tracking inside a `MapState`

**The gotcha:** `ctx.timerService().registerProcessingTimeTimer(t)` associates the timer with the **current key of the element being processed** (i.e., the operator's `keyBy` key) — there is no built-in notion of "one timer per entry in my `MapState`." If an operator is keyed by `X` but holds a `MapState<Y, ...>` internally, you cannot register independent timers per `Y` without your own bookkeeping; two different `Y`s registering a timer for the exact same timestamp collapse into a single `onTimer` callback for that key.

**Why this matters here:** Stage B is keyed by `segment`, but Decision E.1's dead-man's-switch watchdog needs independent 48h timers per `apartment_id` and per `target_date` *within* a segment. `specs/phases/04-flink-processing/spec.md` §7 resolves this with a deadline-map-plus-scan-on-fire pattern specifically because of this gotcha — worth calling out on its own here because it's easy to read the pre-spec doc's original pseudocode ("register a timer for that key") and implement a version that silently only fires once per segment instead of once per apartment/date, with no error or warning to reveal the bug.

---

## 4. PyFlink's Python `ProcessFunction`s run out-of-process — every state access has real RPC/serialization cost

**The gotcha:** Unlike Java Flink, where a `ProcessFunction`'s state access is an in-JVM-heap (or RocksDB-native) call, PyFlink's DataStream API executes Python user code in a **separate Python process**, communicating with the JVM task manager over Apache Beam's portability framework (gRPC). Every `MapState.get()`/`.put()`/`.entries()` call from Python code crosses that process boundary — this is not free the way it would be in native Java Flink, and debugging (stack traces spanning JVM ↔ Python) is meaningfully harder than a pure-Python or pure-Java stack trace.

**Why this matters here:** Decision B (this project's decision to use PyFlink over Java Flink, ADR-0002) accepted this trade-off deliberately for a single-language stack — this entry doesn't reopen that decision, but flags a concrete consequence: §6's "evict on every market event received" full scan of `nights_in_segment` (bounded to ~60 entries) is cheap in absolute terms, but each entry read pays PyFlink's cross-process overhead, not a bare Python dict's. At this project's tiny scale (100 apartments, 18 segments) this is very unlikely to matter — but if this job is ever pointed at real production volume, "how many `MapState` accesses does one event trigger" becomes a real performance question, not a hypothetical one.

---

## 5. RocksDB incremental checkpoints: the first one is always full, and state serializer changes can break checkpoint compatibility across deployments

**The gotcha:** With `EmbeddedRocksDBStateBackend` and incremental checkpointing enabled (Decision F), the *first* checkpoint after the job starts is always a full checkpoint (there's no prior checkpoint to diff against) — subsequent ones upload only the changed SST files. Separately and more importantly: if the **type or shape** of anything stored in state changes between deployments of this job (e.g., adding a field to `CostAggregate`, or changing which Python type backs a `MapState` value), restoring an old checkpoint against the new code is not automatically safe — Flink's state schema evolution support is solid for its own typed state descriptors (POJOs with Flink's type system, Avro) but far less proven for arbitrary Python objects serialized however PyFlink's default mechanism happens to serialize them.

**Why this matters here:** this job's `implementación` task hasn't yet chosen exactly how `CostAggregate`/`MarketSnapshot`/the timer deadline maps get serialized into RocksDB. This is a decision to make deliberately during implementation, not an incidental consequence of "whatever type hints the code happens to use" — worth explicitly picking a serialization approach (e.g., a stable, versioned JSON encoding of state values, rather than relying on implicit pickling) specifically *because* this job's state will need to survive code changes across a project that's still actively evolving `price_decision.v1`'s own contract (ADR-0007 already changed it once).

---

## 6. Checkpoint barrier alignment can stall under backpressure — and this job mixes two structurally different sources

**The gotcha:** By default, Flink's checkpointing aligns barriers across all of an operator's inputs before taking a consistent snapshot — an operator with a slow or backpressured input can stall checkpointing for the *whole* job, not just that one input, until the barrier catches up. Flink 1.11+ offers **unaligned checkpoints** as a mitigation (checkpoints proceed by buffering in-flight data instead of waiting for alignment), at the cost of larger checkpoint sizes.

**Why this matters here:** Stage B's `.connect()` combines a re-keyed Kafka-derived stream (Stage A's output) with the raw Kinesis stream directly — two sources with different connector implementations, different natural throughput characteristics, and (per Phase 3's own findings) an already-observed shard imbalance on the Kinesis side (30/30/10/20). If one side ever backpressures meaningfully more than the other, checkpoint alignment is the first place it would show up as a symptom (rising checkpoint duration, not a data-correctness bug) — worth knowing to look at checkpoint duration/alignment-time metrics specifically if checkpointing ever seems to be silently slow, rather than assuming a state-size problem.

---

## 7. Confirmed: PyFlink 2.3's Kinesis connector is `FlinkKinesisConsumer` only — no modern `KinesisSource`

**Update (2026-07-29): verified against the actual installed `apache-flink==2.3.0`, not assumed.** `pyflink.datastream.connectors.kinesis` exposes only the legacy `FlinkKinesisConsumer` (the older `SourceFunction`-based API) for reading — there is no `KinesisSource`/`KinesisSourceBuilder` counterpart to Kafka's modern `KafkaSource`. This is exactly the Python/Java parity gap this entry originally warned about, now confirmed rather than speculative. `job.py` uses `FlinkKinesisConsumer` accordingly.

**Why this matters here:** confirms this project's practice of checking a tool's actual installed behavior instead of assuming docs/parity hold (same category as Debezium's `CustomConverter` gap, LocalStack's DynamoDB Streams support). Also: `KafkaSource`/`FlinkKinesisConsumer` objects require real connector JARs on the classpath to even construct — attempting to build one without `pipeline.jars` configured fails immediately with a clear Java-class-not-found error, confirmed by trying it directly.

---

## 9. No native custom-Sink API in PyFlink's DataStream — `SinkFunction` only wraps existing Java sinks

**Confirmed 2026-07-29:** `pyflink.datastream.functions.SinkFunction` takes a Java class name or `JavaObject` in its constructor — it is not a base class meant for custom Python write logic the way `MapFunction`/`ProcessFunction` are. There is no Python-native custom `Sink`/`SinkWriter` class in this PyFlink version either.

**The practical pattern used here** (`dynamodb_sink.py` + `job.py`): implement the custom write (boto3 `put_item`) as a `MapFunction` that performs the write as a side effect and passes its input through unchanged, then terminate the stream with `SinkFunction("org.apache.flink.streaming.api.functions.sink.DiscardingSink")` — a no-op Java sink that exists only so the job graph has a formal terminal operator (Flink requires at least one). Consistent with Decision G: no 2PC/transactional sink needed since `put_item` is already idempotent by `decision_id`.

---

## 8. State TTL was not used here on purpose — know why, so it isn't "fixed" into the design later

**The gotcha:** Flink's built-in State TTL (`StateTtlConfig`) can automatically expire `MapState` entries after a configurable idle time, with a choice of cleanup strategies (background RocksDB compaction filter, incremental cleanup on access, full-snapshot cleanup). It looks, at a glance, like it could replace this job's hand-rolled eviction logic (§6) and dead-man's-switch timers (§7) — **it cannot, for one specific reason**: State TTL silently drops expired entries, with no callback, side-output, or event emitted when expiry happens. Decision E.1's entire point is the *opposite* — silence itself must produce a visible `data_stale` signal, not a quiet deletion.

**Why this matters here:** this is worth writing down explicitly so that a future pass at "simplifying" this job's state management doesn't reach for State TTL as an apparent shortcut and accidentally delete the freshness watchdog's actual value in the process. State TTL remains a legitimate tool for the *other* eviction rules in §6 (e.g., `nights_in_segment`'s "date has passed" cleanup could, in principle, use TTL instead of a manual scan) — but only where losing the "why did this get evicted" signal is actually acceptable, which it is there and is not for the watchdog.

---

## 10. Timer deadlines must be stored at millisecond precision, matching the timer service — not as full-precision `datetime`

**Found via testing, 2026-07-29, not review.** Item 3's deadline-map pattern initially stored `datetime` objects (microsecond precision) in the deadline `MapState`, while `register_processing_time_timer()` takes an epoch-millisecond `int`. Converting the stored `datetime` to millis for registration, then back to a `datetime` on `on_timer`'s firing, loses precision on the round trip — the reconstructed value almost never exactly equals the originally stored one, so `expired_keys`'s equality check silently never matched. The watchdog would have fired on schedule but never actually flagged anything as expired.

**Fix:** store deadlines as the same epoch-millisecond `int` used to register the timer (`watchdog.next_deadline_millis`) — no `datetime` round-trip at all, so the equality check in `on_timer` compares the exact same representation on both sides.

**Why this matters generally:** any Flink timer-adjacent state that needs to compare against a fired timer's `timestamp` must be stored in that same unit and precision — mixing a higher-precision Python representation with Flink's millisecond timer resolution is an easy, silent mismatch that unit tests catch immediately but a live cluster would only reveal as "the watchdog just never seems to fire."

---

## What to do with this list

Before or during `streaming/flink-jobs/` implementation, treat each item above as something to actively verify against the real PyFlink version and the real running job — not settled by having been written down here. Items 1, 3, 9, and 10 are design decisions/fixes already applied in the implementation (max parallelism, the deadline-map timer pattern, the MapFunction+DiscardingSink sink shape, millis-precision deadlines). Items 4–7 are things to keep an eye on once the job is actually running under load, not blockers to starting implementation.
