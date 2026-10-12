# Portfolio Demo — EC2 On-Demand Runbook

How to turn the public portfolio demo on before an interview and off afterward, without needing
an AI agent to do it. Companion to [`MANUAL.md`](./MANUAL.md) (which covers running the stack
*locally*) and to the Terraform/scripts under `infra/terraform/` and `infra/aws/`.

1. [What this is](#1-what-this-is)
2. [One-time setup](#2-one-time-setup-already-done)
3. [Turning the demo on](#3-turning-the-demo-on)
4. [Verifying it's actually live](#4-verifying-its-actually-live)
5. [Turning the demo off](#5-turning-the-demo-off)
6. [Troubleshooting](#6-troubleshooting)
7. [Costs](#7-costs)

---

## 1. What this is

A single EC2 instance (`m5.xlarge`, Elastic IP, locked-down security group) running the exact
same `docker-compose.yml` stack as local dev, reachable at **https://pms-price-engine.duckdns.org**
via Caddy (automatic HTTPS). It is **off by default** — started only before a demo/interview, and
stopped right after. See `infra/terraform/ec2.tf` for the resources and
`infra/aws/start-demo.sh` / `stop-demo.sh` for the lifecycle scripts.

## 2. One-time setup (already done)

These only need redoing if the instance is ever destroyed and recreated:

- `aws configure --profile personal` — a dedicated AWS CLI profile for the personal account
  (`940596268510`), never `default`. `infra/terraform/main.tf`'s provider refuses to run without
  both `aws_profile` and `expected_aws_account_id` set, specifically so this can't accidentally
  target a different AWS account.
- `terraform apply` from `infra/terraform/` (with `terraform.tfvars` holding the profile, account
  id, SSH key name, allowed SSH CIDR, and S3 bucket name — gitignored, never committed).
- DuckDNS subdomain (`pms-price-engine.duckdns.org`) pointed at the instance's Elastic IP
  (`terraform output demo_elastic_ip`) — a one-time manual step on duckdns.org, since the IP
  doesn't change across stop/start cycles.
- An SSH key pair imported into the AWS account (`pms-price-engine-demo`), with the **private**
  half generated locally via `ssh-keygen` and never uploaded anywhere (only the public half was
  given to AWS via `import-key-pair`).

## 3. Turning the demo on

From the repo root, on your own machine:

```bash
# Unlock the SSH key if it has a passphrase (only needed once per shell session/reboot)
ssh-add ~/.ssh/pms-price-engine-demo

AWS_PROFILE=personal \
DEMO_INSTANCE_ID=i-0eb3b63810d737bd2 \
DEMO_SSH_KEY_PATH=$HOME/.ssh/pms-price-engine-demo \
DEMO_DOMAIN=pms-price-engine.duckdns.org \
  ./infra/aws/start-demo.sh
```

This single command, with no further input needed:

1. Starts the stopped EC2 instance and waits for it to be reachable over SSH.
2. Copies over the two build artifacts that are deliberately gitignored (`uv.lock`, the Debezium
   date-converter jar) — see `infra/aws/start-demo.sh`'s header comment for why they're not in
   git.
3. Runs `git pull` on the instance, then `docker compose up -d --build`.
4. Waits for Kafka Connect and the Flink cluster to answer, then **idempotently**: creates every
   Kafka topic the pipeline needs, registers the Debezium connector, and submits both Flink jobs
   (`pms-price-engine`, `market-pulse-job`) — skipping any step whose result already exists, so
   re-running this script on an already-running demo is always safe.
5. Writes the real `Caddyfile` (substituting the actual domain) and (re)starts Caddy.

Takes roughly 3-5 minutes end to end, most of it Docker image builds.

**Important:** the windowed metrics (`market_pulse_5min`, `bookings_created_5min`,
`bookings_cancelled_5min`) are 5-minute tumbling windows. The Market Pulse Job panel will show
"not reachable" or an empty table for the first ~5 minutes after start — that's expected, not a
bug. Start the demo at least 10 minutes before you actually need to show it.

## 4. Verifying it's actually live

```bash
curl -s -o /dev/null -w "%{http_code}\n" https://pms-price-engine.duckdns.org/
# expect: 200
```

Or just open the URL in a browser. The dashboard's own "Job health" section (top of the Market
Pulse Job tab) shows live job state and the latest checkpoint — if that's green and advancing,
the pipeline is healthy end to end.

If you changed networks (new public IP) since the last `terraform apply`, SSH to the instance
will be refused by the security group — see [Troubleshooting](#6-troubleshooting).

## 5. Turning the demo off

```bash
AWS_PROFILE=personal \
DEMO_INSTANCE_ID=i-0eb3b63810d737bd2 \
DEMO_SSH_KEY_PATH=$HOME/.ssh/pms-price-engine-demo \
  ./infra/aws/stop-demo.sh
```

Tears down `docker compose`, stops Caddy, then **stops** (not terminates) the EC2 instance. The
EBS volume and Elastic IP survive a stop, so the next `start-demo.sh` doesn't need to rebuild
anything from scratch beyond Docker images already cached on disk.

Always do this after a demo — the instance costs money per hour while running.

## 6. Troubleshooting

**`ssh: Permission denied (publickey)`** — the private key has a passphrase and isn't loaded into
your SSH agent. Run `ssh-add ~/.ssh/pms-price-engine-demo` first.

**SSH connection times out / refused** — your current public IP doesn't match the security
group's allowed CIDR (it's locked to one IP, not `0.0.0.0/0`, on purpose). Get your current IP
(`curl -s ifconfig.me`) and update `ssh_allowed_cidr` in `terraform.tfvars`, then
`terraform apply -target=aws_security_group.demo`.

**Browser shows a blank page or certificate warning right after `start-demo.sh`** — Caddy needs
DNS to already resolve to the instance's Elastic IP before it can get its first Let's Encrypt
certificate. If the Elastic IP ever changes (it shouldn't, across stop/start — only if the
instance is destroyed and recreated), update the IP in the DuckDNS panel first.

**Dashboard loads but the Market Pulse Job panel is empty / "not reachable"** — see the 5-minute
window note in [§3](#3-turning-the-demo-on). Wait it out, or check
`docker exec pms_flink_jobmanager flink list` on the instance to confirm both jobs are `RUNNING`.

**`start-demo.sh` fails on "Missing .../uv.lock"** — run `uv lock` locally first (it should
already exist from normal local dev; this only fires if it was deleted).

## 7. Costs (personal AWS account only)

| Resource | Cost while demo is OFF | Cost while demo is ON |
|---|---|---|
| EC2 `m5.xlarge` | $0 | ~$0.19/hour |
| Elastic IP | ~$0.005/hour (reserved, not attached to a running instance) | $0 (free while attached to a running instance) |
| EBS gp3 50GB | ~$4/month, flat | same |
| DuckDNS | $0 | $0 |

Forgetting to run `stop-demo.sh` is the only way this gets expensive — there's no auto-shutoff.

**Why not also destroy the EBS volume + Elastic IP between demos** (saving that ~$7-8/month
fixed cost entirely)? Considered and deliberately rejected: it would mean `terraform destroy`/
`apply` on every cycle, a new Elastic IP each time (so DuckDNS would need updating automatically
before Caddy could get a certificate), and losing the Docker image build cache (turning the
~3-5 minute start into something closer to 10+ minutes, downloading everything from scratch). For
a demo that's switched on a handful of times a year, the fixed cost buys a fast, simple,
always-the-same-URL start — worth more than the few dollars saved.
