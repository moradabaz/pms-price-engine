# Incident: four bugs getting the full pipeline (Stage A + Stage B + sink) to actually run

**Phase:** 4 | **Date:** 2026-07-30 | **Component:** `streaming/flink-jobs/`, `infra/docker-compose.yml`

Found in sequence submitting the job after the Kinesis→Kafka bridge was wired in. Each one only visible once the job actually ran on the cluster — none caught by review or unit tests.

## 1. `NoOffsetForPartitionException` — no fallback for a brand-new consumer group

`payment-events.v1` used `KafkaOffsetsInitializer.committed_offsets()` with no argument — its default is `KafkaOffsetResetStrategy.NONE`, which throws instead of picking a start position the first time a consumer group reads a topic. **Fix:** `committed_offsets(KafkaOffsetResetStrategy.EARLIEST)` — mirrors Debezium's own `snapshot.mode: initial` (full history on first run, resume from committed offset after).

## 2. `DiscardingSink` moved package in Flink 2.x

Same root cause as the `RichParallelSourceFunction` removal (see the Flink-2.x-Kinesis incident) but this class was *moved*, not deleted: `org.apache.flink.streaming.api.functions.sink.DiscardingSink` → `...sink.legacy.DiscardingSink`. Found by scanning `flink-dist-2.3.0.jar` with Python's `zipfile` for any class named `DiscardingSink`, since `jar`/`unzip` aren't in this image.

## 3. DynamoDB item double-wrapped in `{"M": ...}`

`DynamoDbSinkFunction` built the item with a helper that wraps every dict in `{"M": {...}}` — correct for a *nested* attribute value, wrong for `put_item`'s top-level `Item` argument, which is the bare attribute map. Result: `ParamValidationError: Unknown parameter in Item.M: "decision_id"...` for every field. **Fix:** unwrap one level — `_python_to_dynamodb(data)["M"]`, not `_python_to_dynamodb(data)`.

## 4. `NoCredentialsError` on the TaskManager only

`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` were set on `flink-jobmanager`'s environment but not `flink-taskmanager`'s. The DynamoDB sink's `boto3` client is created in `open()`, which runs on the **TaskManager** (where the operator actually executes), not the JobManager that merely submits the job — so only the JobManager had credentials. **Fix:** same env vars added to `flink-taskmanager` too.

## 5. LocalStack lost the `pms-iceberg` S3 bucket despite `PERSISTENCE: 1`

Checkpointing failed with `NoSuchBucket` even though the bucket was created successfully days earlier (confirmed in LocalStack's own logs). Never fully root-caused — LocalStack's persistence didn't survive whatever happened to the container between then and now. **Fix applied:** recreated the bucket manually (`aws s3 mb`). Not investigated further; flagging in case it recurs.

## Lesson

All four only appeared once a real job actually reached that code path on a real cluster — `docker build` succeeding, or the job graph constructing without error, said nothing about whether it would run. Same lesson as the earlier PyFlink Docker build incidents, one layer up: a job that *submits* successfully hasn't proven anything about what happens once its operators start executing on a TaskManager.
