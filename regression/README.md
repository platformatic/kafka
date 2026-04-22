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

## Artifacts

Generated JSON artifacts are written under `regression/artifacts` and ignored by git.

Performance artifacts contain:

- aggregate median duration and throughput
- raw per-sample `runs`
- bytes/sec when available
- resource samples for RSS, heap, CPU, and event loop delay

## Baselines

Baselines can be stored locally with `REGRESSION_BASELINE_DIR` or remotely with
`REGRESSION_BASELINE_URL`. Remote storage uses `GET` and `PUT` for
`<REGRESSION_BASELINE_URL>/<REGRESSION_LANE>.json`.

For real regression detection over time, run performance tests on a dedicated,
stable runner with pinned Node.js and Kafka versions, persisted baselines, and
`REGRESSION_REQUIRE_BASELINE=1`.
