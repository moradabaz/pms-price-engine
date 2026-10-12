# Incident: `init-aws.sh` silently stopped provisioning new resources on every LocalStack restart

**Phase:** 5 | **Date:** 2026-08-04 | **Component:** `infra/localstack/init-aws.sh`, `infra/docker-compose.yml` (`localstack` service)

## What happened

Added three new resources to `init-aws.sh` for Phase 5 (`stream_checkpoints` table, `pms-lakehouse` bucket, a Glue database). Restarting the `localstack` container (`docker compose up -d localstack` against an existing `localstack_data` volume) never created any of them — LocalStack's own log showed the script erroring out on the very first line:

```
An error occurred (ResourceInUseException) when calling the CreateStream operation: Stream market-price-events already exists
ERROR ... Script /etc/localstack/init/ready.d/init-aws.sh returned a non-zero exit code 255
```

## Root cause

`infra/docker-compose.yml`'s `localstack` service sets `PERSISTENCE: 1`, and `init-aws.sh` starts with `set -euo pipefail`. Those two facts don't compose: `PERSISTENCE: 1` means every resource created by a *previous* run of this script still exists after a restart (the whole point of the setting — Kinesis stream data, DynamoDB tables, S3 buckets all survive). But every `awslocal ... create-*` command in the script was written assuming a clean slate, with no existence check — the very first one (`kinesis create-stream`) fails with `ResourceInUseException` on any restart after the first, and `set -e` aborts the entire script right there. Every command after it — including all three new Phase 5 resources — silently never ran. Nothing in LocalStack's healthcheck (`awslocal kinesis list-streams`) catches this, because that check only verifies Kinesis is reachable, not that the init script actually completed.

## How it was solved

Made every resource-creation step idempotent with an existence-check-then-create pattern:

```bash
awslocal dynamodb describe-table --table-name stream_checkpoints --region "$REGION" >/dev/null 2>&1 || \
awslocal dynamodb create-table --table-name stream_checkpoints ...
```

(`describe-table`/`describe-stream`/`s3api head-bucket` for the resources that have one; `get-database` was going to be used for Glue's database too, but see the related write-up on Glue's actual Community-tier availability — that step was removed entirely, not just made idempotent.) Verified live by restarting the container twice in a row and confirming `"Bootstrap complete."` printed both times with no errors, and every expected resource (bucket, tables) existed via direct `aws --endpoint-url` calls afterward.

## What to learn from this

`set -e` and a persistent volume are a standing trap for any init/bootstrap script: `set -e` assumes "any command failing means something is wrong," but a persistent volume means "this command failing because the resource already exists" is the *expected*, correct outcome on every run after the first. Writing a bootstrap script against a persistent backing store without making every step idempotent isn't a script that "usually works" — it's a script that works exactly once, on a genuinely empty volume, and silently does less and less each time it's asked to do more (new steps appended after the first failure point never execute) until someone notices resources are missing. The failure mode is especially dangerous because it's *quiet*: the container still reports healthy, the original resources still work, and only the newest additions are missing.

## Situations where you can hit this

Any bootstrap/init script for a service with a persistent data volume (LocalStack, a database's `docker-entrypoint-initdb.d` scripts re-run against an existing data directory, a Kafka topic-creation script re-run against a broker that kept its logs, Terraform-adjacent shell scripts predating real Terraform state) — if the script uses `set -e` (or any language's equivalent of "stop on first error") and doesn't explicitly handle "this already exists," adding a new step to the end of the script is silently a no-op on every environment that isn't starting from zero. Always test a bootstrap script's idempotency by running it twice against the same persisted state before trusting it, not just once against a fresh volume.
