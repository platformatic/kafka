# Performance And Reliability Regression Plan

## Goal

Run a dedicated regression workflow on every pull request merge so the project can detect:

1. produce and consume performance regressions
2. functional test regressions
3. CPU and memory instability, including memory leaks
4. broken backpressure on producer and consumer paths

## What Already Exists In This Repo

- CI already starts Kafka in GitHub Actions with `docker compose up --build --force-recreate -d --wait` in `.github/workflows/ci.yml`.
- The repo already contains producer and consumer benchmark scripts in `benchmarks/producer-single.ts`, `benchmarks/producer-batch.ts`, and `benchmarks/consumer.ts`.
- The repo already contains targeted backpressure tests:
  - `test/clients/consumer/messages-stream-backpressure.test.ts`
  - `test/clients/consumer/messages-stream-backpressure-memory.test.ts`
  - `test/memory/messages-stream-backpressure.memory-test.ts`
- There is already reconnect coverage in `test/clients/consumer/messages-stream.test.ts`.
- The repo already has a 3-broker cluster in `docker-compose.yml`, which is important for realistic rebalance and memory tests.

## Additional Scenarios To Include

Issue history and current tests show that the plan should include more than the four initial categories.

Add explicit regression coverage for:

1. rebalance races causing stale offsets or duplicate consumption
   - closed issue `#223`
2. stale epoch and leader epoch handling during startup and fetch
   - open issue `#267`
   - closed issue `#248`
3. metadata refresh resilience when bootstrap brokers fail but discovered brokers are healthy
   - closed issue `#232`
4. automatic recovery after transient network failures or stream disconnects
   - existing test coverage in `messages-stream.test.ts`
   - related closed issue `#206`
5. SASL reauthentication deadlocks
   - closed issue `#226`
6. batch consumption stalls
   - closed issue `#228`
7. deserialization failures under heavy load
   - closed issue `#227`
8. consumer backpressure regressions causing unbounded buffering, stalls, or memory growth
   - closed issue `#260`
9. `minBytes` and fetch tuning regressions that create excessive bandwidth or CPU load
   - closed issue `#99`
10. per-partition backpressure fairness

- open issue `#128`

11. offset commit correctness across manual commit, timed autocommit, and close paths
12. message loss or duplicate delivery during failure or recovery scenarios
13. idempotent and transactional producer reliability
14. shutdown and cleanup stability, including leaked timers or pending handles
15. authentication-path reliability across SASL/OAUTHBEARER, SCRAM, and GSSAPI
16. schema-registry and deserializer integration under load
17. cross-version compatibility on oldest and newest supported Kafka versions
18. control-batch and tombstone correctness

## Proposed Workflow Layout

Create a separate workflow, for example `.github/workflows/regression.yml`.

Trigger it on:

- `push` to `main`
- `workflow_dispatch`

Run it only:

1. when code is merged to `main`
2. when manually triggered via GitHub Actions

Scope of each run:

- execute correctness, backpressure, memory, performance, compatibility, and issue-driven regression coverage as configured for this workflow
- compare results against the stored baseline for the same lane

## Infrastructure Strategy

Run this workflow on a custom self-hosted GitHub Actions runner hosted on a dedicated Linux server.

Reason:

- performance and resource measurements are too noisy on shared GitHub-hosted runners
- a dedicated Linux host gives more stable CPU, memory, I/O, network, and Docker behavior
- the regression baseline is only meaningful if the execution environment is kept stable over time

Recommended order:

1. provision a dedicated Linux server and register it as a self-hosted GitHub Actions runner
2. run the regression workflow only on that runner
3. keep Docker, Node.js, Kafka image versions, host sizing, and kernel settings stable over time
4. pin Node.js and Kafka versions for regression comparisons

Cluster strategy:

- correctness smoke jobs can use the existing single-broker services where suitable
- rebalance, leak, and backpressure soak jobs should use the existing 3-broker cluster
- SASL reauthentication jobs should use `broker-sasl`

Version strategy:

- keep the main performance comparison on a single pinned Kafka and Node.js version
- add at least one oldest-supported Kafka lane and one newest-supported Kafka lane for correctness and protocol regressions

Runner requirement:

- label the runner specifically for regression jobs, for example `self-hosted`, `linux`, `x64`, `kafka-regression`
- avoid sharing that runner with unrelated CI workloads
- keep background services on the host to a minimum
- allow only one workflow instance at a time on that dedicated runner
- queue later runs until the current run completes
- configure GitHub Actions concurrency so runs are serialized instead of cancelled

## Required Jobs

### 1. Correctness Regression Job

Purpose:

- fail immediately if the functional suite regresses

Actions:

1. install dependencies
2. start Kafka with Docker Compose
3. run `pnpm run ci`
4. upload test reports and logs as artifacts

Additional assertions to make explicit in this job family:

- no message gaps in recovery scenarios
- no duplicate delivery in recovery scenarios
- committed offsets advance to the expected next offset
- close and teardown complete without hung handles

Notes:

- this job largely exists already in `.github/workflows/ci.yml`
- keep it as the hard gate for merges

### 2. Backpressure Regression Job

Purpose:

- detect stalled pipelines, unbounded buffering, and producer-side unwritable-node regressions

Actions:

1. run the current targeted consumer backpressure tests
2. add a dedicated producer backpressure regression suite around `acks=0`, stream writes, and unwritable nodes
3. add a per-partition fairness scenario so one hot partition cannot starve others
4. publish metrics artifact with:
   - max `readableLength`
   - processed messages per second while backpressured
   - stall count / no-progress intervals
   - partition-level fairness during mixed-load consumption

Pass criteria:

- no stalls
- no unbounded growth in buffered messages
- no partition starvation beyond agreed thresholds

### 3. Memory And Resource Stability Job

Purpose:

- detect memory leaks and resource drift under sustained pressure

Actions:

1. run `npm run test:memory`
2. add process-level sampling during test execution:
   - Node heap used
   - RSS
   - CPU percent
   - event loop delay
3. persist the time series as artifacts
4. compare the last samples with the baseline envelope from `main`

Pass criteria:

- no monotonic leak pattern
- no envelope drift above agreed memory threshold
- CPU remains within an acceptable band for the same workload

Implementation note:

- because memory tests are currently excluded from CI, this job should run in this dedicated regression workflow on merge to `main` and on manual dispatch, not on every PR update

### 4. Performance Benchmark Job

Purpose:

- detect throughput and latency regressions for produce and consume paths

Actions:

1. create a benchmark harness around the existing scripts in `benchmarks/`
2. run at least these scenarios:
   - single-message produce
   - batched produce
   - stream/evented consume
3. normalize output into machine-readable JSON
4. store benchmark results in S3 or a compatible object store and optionally expose a short summary in GitHub Actions
5. compare against the latest `main` baseline, not against raw historical averages

Metrics to track:

- messages/sec
- p50, p95, p99 latency
- bytes/sec
- CPU per 100k messages
- RSS / heap delta during benchmark window

Pass criteria:

- hard fail only on severe regression
- warn on moderate degradation

Recommended initial thresholds:

- fail if throughput degrades by more than 15%
- fail if latency worsens by more than 20%
- warn if throughput degrades by more than 8%

### 5. Compatibility And Auth Regression Job

Purpose:

- detect version-sensitive protocol regressions and authentication hangs that may not show up in the main pinned lane

Actions:

1. run a reduced correctness suite against the oldest supported Kafka version
2. run the same reduced suite against the newest supported Kafka version
3. run auth-focused smoke coverage for:
   - SASL/OAUTHBEARER
   - SCRAM
   - GSSAPI when practical in this workflow
4. upload broker and client logs for any failed auth or protocol lane

Pass criteria:

- no auth deadlocks
- no protocol-version-specific fetch, commit, or rebalance regressions
- no version-specific startup failures

## Special Regression Suites To Add

The current suite is strong, but the following focused tests should be added so known incidents stay covered:

1. committed-offset startup with `groupProtocol: 'consumer'` under member-epoch churn
   - covers `STALE_MEMBER_EPOCH` issue `#267`
2. leader epoch refresh during long-running fetch loops
   - covers `FENCED_LEADER_EPOCH` issue `#248`
3. rebalance while offsets are being refreshed
   - covers issue `#223`
4. metadata refresh after bootstrap broker loss
   - covers issue `#232`
5. SASL reauth under async token refresh
   - covers issue `#226`
6. heavy-load deserialization with large batches and mixed payload sizes
   - covers issue `#227`
7. consumer backpressure under `for await` and pipeline-based consumption
   - covers issue `#260`
8. bandwidth amplification regression when `minBytes` is set
   - covers issue `#99`
9. long-running batch consumer stall detection
   - covers issue `#228`
10. offset commit correctness under manual commit, timed autocommit, and stream close
11. message delivery invariants under reconnect, rebalance, stale epoch, and backpressure
12. idempotent producer retry behavior and transactional correctness
13. shutdown under load with no leaked timers or pending handles
14. schema-registry consumption and deserialization under sustained load
15. auth-path regression coverage for OAUTHBEARER, SCRAM, and GSSAPI
16. tombstone and control-batch correctness

## Core Invariants

Every recovery or stress regression should assert the same core correctness properties, not only that the test finishes.

Required invariants:

1. no message loss
2. no duplicate delivery unless the scenario explicitly allows at-least-once duplicates
3. committed offsets move forward exactly as expected
4. consumers and producers remain closable after the scenario ends
5. no hidden resource leak remains after shutdown

## Baseline Storage

The workflow needs a stable baseline source.

Recommended approach:

1. on every successful run, store JSON results, logs, and resource samples in S3 or a compatible object store
2. keep a lightweight `regression-baseline` object containing the latest accepted benchmark and resource metrics for each lane
3. let later runs download those stored results and compare against the latest accepted baseline
4. keep GitHub Actions artifacts only as short-lived convenience attachments for the current run

Stored objects should be organized by:

- workflow name
- commit SHA
- branch
- runner label
- Kafka version
- Node.js version
- timestamp

This storage strategy supports:

- historical retrieval
- baseline comparison
- regression triage
- re-running comparisons without depending on GitHub artifact retention windows

## Noise Control

Performance results in GitHub-hosted environments can be noisy. Reduce noise by:

1. pinning runner image, Node.js version, and Kafka version
2. using a fixed topic layout, message sizes, partition counts, and warmup duration
3. running each benchmark scenario multiple times and comparing medians
4. separating correctness failures from performance warnings
5. avoiding version matrices in the main performance comparison job
6. separating the main perf lane from compatibility and auth lanes

This plan explicitly covers the main production-like failure classes already seen in repository history:

- rebalance offset races
- stale epoch handling
- metadata refresh resilience
- consumer backpressure regressions with buffering or stalls
- offset commit correctness
- duplicate/loss invariants during recovery
- idempotent and transactional producer reliability
- shutdown and cleanup stability
- auth-path reliability
- schema-registry load behavior
- cross-version compatibility
- tombstone and control-batch correctness
- reauthentication deadlocks
- heavy-load deserialization failures
- long-running batch-consumer stalls

## Suggested Rollout

### Phase 1

1. keep existing CI as the correctness gate
2. keep this regression plan document as the implementation reference
3. create `regression.yml` with:
   - correctness smoke
   - existing backpressure tests
   - existing memory test on merge to `main`
   - explicit invariant checks for duplicates, gaps, and committed offsets

### Phase 2

1. convert `benchmarks/*` into JSON-producing scripts
2. add baseline download and comparison logic against the stored S3-compatible results
3. publish GitHub Actions job summaries with trend deltas
4. add reduced oldest/newest Kafka compatibility lanes

### Phase 3

1. add the missing issue-driven regression tests listed above
2. expand this workflow with the remaining issue-driven scenarios
3. add auth-heavy and schema-registry-heavy soak coverage to this workflow

## Concrete Deliverables

1. `.github/workflows/regression.yml`
2. benchmark result schema such as `artifacts/regression/*.json`
3. baseline compare script
4. resource sampler script
5. new regression tests for issues `#223`, `#226`, `#227`, `#228`, `#232`, `#248`, `#260`, `#267`, and open enhancement `#128`
6. invariant helpers for duplicate, gap, and committed-offset assertions
7. reduced compatibility suite for oldest and newest supported Kafka versions
8. auth-focused regression suite
9. schema-registry load regression suite
10. shutdown and leaked-handle regression suite
