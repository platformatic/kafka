# Regression Suite

This directory contains the code-side regression suite. It is intentionally outside
`test/` so regular `npm test` and `npm run test:ci` do not run these heavier tests
by accident.

The suite assumes Kafka services from `docker-compose.yml` are already running.

## Commands

Start dependencies:

```sh
npm run test:docker:up
```

Run the default regression suite. This intentionally runs integrity tests only so
local/default regression runs do not spend time in performance benchmarks:

```sh
npm run test:regression
```

Run only integrity regression tests:

```sh
npm run test:integrity
```

Run only producer/consumer performance regression tests:

```sh
npm run test:performance
```

Run all regression tests, including performance tests:

```sh
npm run test:regression:all
```

Run the standalone benchmark/baseline harness:

```sh
npm run regression:baseline:update
```

Run regular tests plus all regression tests:

```sh
npm run test:all
```

## Test Groups

Correctness and integrity:

- `integrity/invariants.test.ts`: shared duplicate/gap/resource helper behavior
- `integrity/metadata-recovery.test.ts`: bootstrap broker failure and metadata refresh recovery
- `integrity/recovery-delivery.test.ts`: reconnect recovery without gaps or duplicates
- `integrity/rebalance-offsets.test.ts`: rebalance race while committed offsets refresh
- `integrity/stale-epoch.test.ts`: committed-offset startup with the consumer group protocol
- `integrity/offset-commit-correctness.test.ts`: manual and timed autocommit offset correctness
- `integrity/tombstone-control-batch.test.ts`: tombstones are delivered and transaction control batches are filtered

Backpressure, stalls, and load:

- `integrity/backpressure.test.ts`: pipeline backpressure progress and bounded buffering
- `integrity/batch-stall.test.ts`: long-running batch consumer stall detection
- `integrity/partition-fairness.test.ts`: hot partitions do not starve colder partitions
- `integrity/deserialization-load.test.ts`: JSON deserialization under load without loss or duplicates
- `integrity/resource-stability.test.ts`: bounded resource sampling during sustained consume

Producer and transaction reliability:

- `integrity/producer-reliability.test.ts`: producer stream `acks=0` backpressure and idempotent transaction commit

Authentication and compatibility:

- `integrity/auth-reauth.test.ts`: OAUTHBEARER reauthentication and SCRAM smoke coverage
- `integrity/gssapi-auth.test.ts`: GSSAPI/Kerberos auth path
- `integrity/compatibility.test.ts`: reduced compatibility lane driven by environment brokers
- `integrity/schema-registry-load.test.ts`: schema-registry deserialization under sustained load

Performance:

- `performance/producer-performance.test.ts`: single-message and batched producer throughput regression checks
- `performance/consumer-performance.test.ts`: stream consumer throughput regression checks
- `integrity/baseline-tools.test.ts`: baseline store, benchmark harness, and median aggregation coverage

## Performance Behavior

Performance tests run multiple samples and compare the median result against a stored
baseline when one exists. This reduces noise compared with a single measurement.

By default, local runs use a small message count so the tests act as performance
smoke tests. On the dedicated Linux regression runner, use larger message counts,
more samples, warmups, and required baselines.

Recommended runner settings:

```sh
REGRESSION_REQUIRE_BASELINE=1 \
REGRESSION_PERFORMANCE_MESSAGES=5000 \
REGRESSION_PERFORMANCE_SAMPLES=5 \
REGRESSION_PERFORMANCE_WARMUPS=1 \
REGRESSION_THROUGHPUT_FAIL_RATIO=0.85 \
REGRESSION_LATENCY_FAIL_RATIO=1.2 \
REGRESSION_MEMORY_FAIL_RATIO=1.25 \
npm run test:performance
```

## Environment Variables

- `REGRESSION_LANE`: baseline lane name. Defaults to `local`.
- `REGRESSION_BENCHMARK_MESSAGES`: standalone benchmark message count. Defaults to `1000`.
- `REGRESSION_PERFORMANCE_MESSAGES`: performance test message count. Defaults to `100`.
- `REGRESSION_PERFORMANCE_SAMPLES`: measured samples per performance test. Defaults to `3`.
- `REGRESSION_PERFORMANCE_WARMUPS`: unmeasured warmup runs before samples. Defaults to `0`.
- `REGRESSION_THROUGHPUT_FAIL_RATIO`: minimum allowed throughput ratio vs baseline. Defaults to `0.85`.
- `REGRESSION_LATENCY_FAIL_RATIO`: maximum allowed duration ratio vs baseline. Defaults to `1.2`.
- `REGRESSION_MEMORY_FAIL_RATIO`: optional maximum RSS ratio vs baseline.
- `REGRESSION_REQUIRE_BASELINE`: set to `1` to fail when no baseline exists for a performance result.
- `REGRESSION_BASELINE_DIR`: local baseline directory for file-based baselines.
- `REGRESSION_BASELINE_URL`: HTTP/object-store-compatible baseline endpoint.
- `REGRESSION_BASELINE_TOKEN`: bearer token for `REGRESSION_BASELINE_URL`.
- `REGRESSION_COMPAT_BOOTSTRAP_SERVERS`: comma-separated Kafka brokers for compatibility lanes.
- `KAFKA_SINGLE_PORT`, `KAFKA_SASL_PORT`, `KAFKA_KERBEROS_PORT`: published broker ports, defaulting to
  `9001`, `9002`, and `9003`. Compose and the regression/test helpers read the same variables.
- `KAFKA_CLUSTER_1_PORT`, `KAFKA_CLUSTER_2_PORT`, `KAFKA_CLUSTER_3_PORT`: cluster ports, defaulting to
  `9011`, `9012`, and `9013`.
- `COMPOSE_FILE`, `COMPOSE_PROJECT_NAME`: select the stack for lifecycle commands and broker diagnostics.

### Running modern and legacy stacks together

Compose uses distinct default projects (`kafka-modern`, `kafka-legacy`, and `kafka-redpanda`) and generates
project-scoped container, network, and volume names. Published ports still need to be distinct. In a separate
shell, configure the legacy stack and its compatibility tests together:

```sh
export COMPOSE_FILE=docker-compose.legacy.yml
export KAFKA_SINGLE_PORT=29001 KAFKA_SASL_PORT=29002 KAFKA_SASL_PLAIN_PORT=39002
export KAFKA_CLUSTER_1_PORT=29011 KAFKA_CLUSTER_2_PORT=29012 KAFKA_CLUSTER_3_PORT=29013
docker compose up -d --wait
pnpm run test:compat
bash scripts/collect-kafka-diagnostics.sh local-legacy
docker compose down --volumes --remove-orphans
```

The advertised Kafka listeners use those same ports, so metadata directs clients back to the selected stack.
Use `docker compose exec -T <service> ...` for diagnostics rather than assuming a global container name.
For multiple copies of the same stack, also set a distinct `COMPOSE_PROJECT_NAME` and change every published
port. Redpanda accepts `REDPANDA_PORT`; its smoke test also accepts `REDPANDA_BOOTSTRAP_SERVERS`.

## Artifacts

Generated JSON artifacts are written under `regression/artifacts` and ignored by git.

CI runs also generate `regression-summary.md`, a concise record of the tested
commit, broker image and Kafka version, Node.js version, regression lane, runner, and workflow URL.
The same content is published in the GitHub Actions job summary.

Performance artifacts contain:

- aggregate median duration and throughput
- raw per-sample `runs`
- bytes/sec when available
- resource samples for RSS, heap, CPU, and event loop delay

## Notifications

The aggregate regression job posts functional and performance failures to Slack using the `regression`
Environment's `SLACK_WEBHOOK_URL` secret. `.github/workflows/regression-runner-health-check.yml` independently
checks for queued performance jobs after two hours, ignoring expected matrix waiting while another performance
job is running. Configure the repository secret `SLACK_WEBHOOK_URL` for this runner-health notification.

The regression workflow serializes the performance matrix on the dedicated runner to avoid benchmark
interference. Functional jobs run independently on GitHub-hosted runners.

The queue watchdog uses the `regression-runner-health-check` GitHub Environment wait timer instead
of sleeping on a runner. Configure that Environment in the repository settings with a 120-minute
wait timer and no required reviewers. The `requested` workflow run creates the delayed watchdog. It inspects
the dedicated jobs rather than the workflow status: hosted jobs can start while the performance runner is offline.

## CI Version Coverage

The regression workflow covers every modern Confluent Kafka version used by the CI matrix: `7.5.0`,
`7.6.0`, `7.7.0`, `7.8.0`, `7.9.0`, `8.0.0`, `8.1.0`, and `8.2.0`, split into two job groups:

- **GitHub-hosted:** integrity and memory suites for each modern version, API compatibility against Apache
  Kafka `1.1.0`, and the Redpanda and Azure Event Hubs smoke tests. Modern API compatibility also runs in
  the regular CI workflow. Event Hubs retains per-run Azure provisioning and verified cleanup.
- **Dedicated runner:** one performance-regression job and separate protocol Produce and Fetch jobs per
  modern version, plus separate unpinned Produce and Fetch sanity jobs for Kafka `1.1.0`. The matrix uses
  `max-parallel: 1`, and performance test files also run sequentially.

Each Docker job starts a fresh project once, waits for readiness, runs its workload, collects container state
and broker logs, then removes the project and volumes. The suite scripts only run tests and generate reports;
they never change the cluster lifecycle. Separate Produce/Fetch jobs avoid carrying broker state between sweeps
without an intermediate restart. Legacy jobs never tear down a modern stack or reuse ZooKeeper from another job.

Every lane writes a report and functional lanes keep going after an individual suite fails. The aggregate
workflow report waits for both job groups and shows performance results separately. A failure in either group
fails the workflow and its release gate. Baseline lane identities are preserved; reports and protocol artifacts
are namespaced by version and workload and are not compared across broker versions. Re-establish baselines
when comparing against the previous mixed-workload setup, since isolation and scheduling have changed.

## Release Gate

The release workflow requires the latest regression for its pre-bump source commit to have completed successfully.
It downloads that run's `regression-summary.md`, attaches it to the draft GitHub release, and only then publishes the
release. A missing, pending, failed, or artifact-less regression blocks the release before the version is changed.

## Baselines

Baselines can be stored locally with `REGRESSION_BASELINE_DIR` or remotely with
`REGRESSION_BASELINE_URL`. Remote storage uses `GET` and `PUT` for
`<REGRESSION_BASELINE_URL>/<REGRESSION_LANE>.json`.

For real regression detection over time, run performance tests on a dedicated,
stable runner with pinned Node.js and Kafka versions, persisted baselines, and
`REGRESSION_REQUIRE_BASELINE=1`.
