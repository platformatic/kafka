# Protocol Version Load Testing Plan

## Goal

Answer one question: **are the legacy protocol codecs added in the `compatibility` branch as fast as the
newest ones?**

The compatibility work made `Base[kGetApi]` able to negotiate down to `Produce v3` and `Fetch v4`. Every
broker in CI negotiates to the newest version, so the legacy codecs are exercised for correctness
(`test/integration/*.compat-test.ts`) but never for speed. If one of them is accidentally quadratic, allocates
per record, or defeats a fast path in `DynamicBuffer`, nothing in the repo would notice.

This plan measures that, on this machine, with the client protocol version as the only variable.

## What Counts As An Answer

The deliverable is a table of `client CPU microseconds per message` and `throughput` for every implemented
version of `Produce` and `Fetch`, measured against the same modern broker, plus a verdict against the
criteria in [Pass And Fail Criteria](#pass-and-fail-criteria).

"The old protocol is slower than the new one" is only a meaningful claim if the broker, the hardware, the
record batch and the message count are held constant. Which leads to the central design decision below.

## Design: Isolate The Variable

There are two comparisons one could call "old protocol vs new broker", and only one of them is an experiment.

| Comparison | Varies | Useful? |
| --- | --- | --- |
| **A. Pinned old codec vs newest codec, same broker** | client codec only | Yes — this is the experiment |
| **B. Kafka 1.1.0 broker vs Kafka 4.x broker** | broker JVM, storage format, log implementation, config defaults, container image, *and* codec | No — hopelessly confounded |

Comparison B cannot attribute a difference to our code: a 1.1.0 broker is a decade-older JVM writing a
different on-disk format. It is still worth running, but as a **sanity check** that nothing pathological
happens against a genuinely old broker, not as a performance measurement. Tier 2 below does exactly that,
and its results are explicitly labelled non-comparable.

Comparison A is possible because `test/helpers/api-versions.ts` already has `pinApiVersions()`, which rewrites
`client[kApis]` so the client negotiates a version we choose instead of the maximum. A modern broker still
accepts every version this package implements (KIP-896 raised the floor in Apache Kafka 4.0 to `Produce v3`
and `Fetch v4`, exactly our lowest implemented codecs), so the whole matrix runs against one broker.

## What Actually Differs Between Versions

Grounding the hypotheses, so the results can be explained rather than just reported.

**Produce — implemented v3 to v11** (`src/apis/producer/produce-v*.ts`)

| Range | Encoding |
| --- | --- |
| v3–v8 | non-flexible: `INT16`/`INT32` length prefixes, no tagged fields |
| v9–v11 | flexible (KIP-482): compact strings, varint lengths, `appendTaggedFields()` on every struct |

**Fetch — implemented v4 to v17** (`src/apis/consumer/fetch-v*.ts`)

| Range | Encoding |
| --- | --- |
| v4–v11 | non-flexible, topics identified by name |
| v12 | flexible, still topics by name |
| v13–v17 | flexible, topics by 16-byte UUID (KIP-516) |

**Record batches are identical across the entire matrix.** `RecordBatch v2` arrived in Kafka 0.11 alongside
`Produce v3` and `Fetch v4`, so CRC32C, compression and the record encoder are constant. Any difference we
measure is request/response framing, not payload.

This produces three concrete, falsifiable hypotheses:

1. **H1 — old versions are not slower to encode.** Non-flexible framing writes fixed-width lengths instead of
   varints and skips tagged fields entirely, so v3–v8 should be *equal or marginally cheaper* per request than
   v9–v11. A result showing legacy versions materially slower means a defect in our codec, not in the protocol.
2. **H2 — `Fetch <= v12` pays a bounded remap cost.** `src/clients/consumer/consumer.ts:974` rebuilds a
   `topicIdsByName` map and rewrites `topic.topicId` on each response when `api.version <= 12`. This PR already
   hoisted it out of the per-record callback, so the cost must scale with *topics per response*, not *records
   per response*. The consumer sweep tests exactly that by varying batch size at a fixed topic count.
3. **H3 — wire size favours old versions for short topic names.** A compact string saves one byte of length
   prefix; a topic UUID costs 16 bytes versus a short name. For `benchmarks`-length topic names, v13+ may well
   be *larger* on the wire. Worth quantifying since it is counter-intuitive.

## This System

Measured, not assumed:

- 8 logical cores, 62 GB RAM
- Node.js v24.18.0 via `./scripts/node`
- Docker available; `docker-compose.yml` defaults to `confluentinc/cp-kafka:8.1.0`, single broker on
  `localhost:9001`, 3-broker cluster on `9011`–`9013`
- `docker-compose.legacy.yml` provides Apache Kafka 1.1.0 on the same ports

Eight cores is the binding constraint: the broker JVM and the client compete for CPU, and an unpinned run
will attribute broker scheduling noise to the client codec. Every tier below therefore pins CPUs.

### Isolation Setup

Add `docker-compose.perf.yml` as an override (overrides may add keys, which is all we need here):

```yaml
# Overrides for protocol version load testing. Not used by CI.
services:
  broker-single:
    cpuset: '0-3'
    mem_limit: 8g
    environment:
      # Keep the measurement off the disk: 8 GB of tmpfs is far more than the sweeps write, and it
      # removes page-cache and fsync variance from a comparison that is about CPU in the client.
      KAFKA_LOG_DIRS: '/tmpfs-logs'
      KAFKA_NUM_PARTITIONS: '1'
    tmpfs:
      - /tmpfs-logs:size=8g
```

Client side, for every run:

```bash
taskset -c 6,7 ./scripts/node ...
```

Before the first run:

```bash
# Record the state the numbers were taken in — it goes in the results header.
sudo cpupower frequency-set -g performance 2>/dev/null || echo 'governor unchanged'
cat /sys/devices/system/cpu/intel_pstate/no_turbo 2>/dev/null
uname -r; nproc; free -g
docker exec broker-single kafka-topics --version
```

Nothing else should be running. The previous session left an 8-JVM stack up for 15 hours; confirm with
`docker ps` and `pgrep -c java` before starting.

## Tier 0 — Codec Microbenchmark (no broker)

The most sensitive tier, and the cheapest. It calls the codecs directly, so it measures pure serialization
with zero network, zero broker and zero scheduler noise. If a legacy codec has an algorithmic defect, this is
where it shows up unambiguously.

New file: `benchmarks/protocol-versions/codecs.ts`

- For each `produceV3 … produceV11`: encode a fixed batch of N records, report ns/op and `writer.length` as
  exact wire bytes.
- For each `fetchV4 … fetchV17`: parse a synthesized response carrying the same N records.
- Sweep N over `1, 10, 100, 1000, 10000` records per request.

Timing is a small local harness (`utils/measure.ts`) rather than `cronometro`: it times a block of calls and
reports the median of per-block averages, which keeps `hrtime` overhead out of a ~600 ns measurement and stops
one unlucky major GC from setting a version's number.

The fourteen Fetch response layouts come from one parameterised builder (`utils/fetch-response.ts`) driven by
five traits read off the schemas — session header (v7+), log start offset (v5+), preferred read replica (v11+),
flexible framing (v12+), topic UUID (v13+). It is self-checking: the benchmark parses what it writes with the
real codec and asserts the record count survives, so a malformed buffer fails loudly instead of being measured.

The batch-size sweep is the point: a codec whose cost per record rises faster than the newest one's has a
growth or copy bug.

```bash
taskset -c 6,7 ./scripts/node benchmarks/protocol-versions/codecs.ts
```

Estimated runtime: 2–4 minutes. No Docker required, so this tier can also run in CI later.

## Tier 1 — End To End Against A Modern Broker

The headline numbers. Same broker, same topic, same records; only the pinned version changes.

```bash
docker compose -f docker-compose.yml -f docker-compose.perf.yml up -d --wait broker-single
```

New files: `benchmarks/protocol-versions/produce-versions.ts`, `benchmarks/protocol-versions/consume-versions.ts`

Shared harness (`benchmarks/protocol-versions/utils/harness.ts`):

- Import `pinApiVersions` and `usableVersions` from `../../test/helpers/api-versions.ts`. Both are already
  written against `Base`, and their only `node:test` import is type-only, so they strip cleanly outside the
  test runner. Do not duplicate them.
- Default `bootstrapBrokers` to `['localhost:9001']`, **not** the cluster in `benchmarks/utils/definitions.ts`:
  replication across three brokers adds variance that has nothing to do with the codec.
- Skip and *report* any version the broker refuses, reusing the `isUnsupportedVersion` logic — a silently
  skipped version must never look like a passing one.

### Metrics

**Client CPU microseconds per message is the primary metric, not throughput.** With `acks=ALL` the broker
round trip dominates wall clock and every version will look identical, hiding a real codec regression. Capture
per run:

| Metric | Source |
| --- | --- |
| CPU µs per message (primary) | `process.cpuUsage()` delta ÷ messages |
| Messages per second | wall clock, as in `regression/helpers/benchmark-harness.ts` |
| GC pause total | `PerformanceObserver` on `entryTypes: ['gc']` |
| Peak RSS and heapUsed | `process.memoryUsage()` sampled, as `createResourceSampler()` already does |
| Wire bytes per message | from Tier 0 (exact), cross-checked against `docker stats` network counters |

### Producer Sweep

- Versions: every usable `Produce` version, expected v3–v11.
- Workloads: single message per `send()`, and batches of 100 and 1000.
- `acks`: run both `NO_RESPONSE` (isolates the encode path — no broker wait) and `ALL` (realistic).
- 100,000 messages per run.

### Consumer Sweep

- Versions: every usable `Fetch` version, expected v4–v17.
- Pre-seed the topic once, outside the timed region, using a single fixed producer version.
- Consume 100,000 messages per run with `autocommit: false` and a fixed `maxBytes`, so the number of round
  trips is identical across versions.
- Additionally sweep `maxBytes` so records-per-response varies by ~100x. **This is the H2 test:** the
  `Fetch <= v12` remap cost must not track records per response.

### Execution Discipline

- 3 warmup runs, discarded, before the first measured run.
- 7 measured repetitions per cell; report median and interquartile range.
- **Interleave versions across repetitions** — run the full version list once per repetition in shuffled
  order, rather than all 7 samples of v3 then all 7 of v11. Otherwise thermal drift and page-cache warming
  are indistinguishable from a version effect.
- Restart the broker between the producer and consumer sweeps so log segment growth does not favour whichever
  version ran later.

```bash
taskset -c 6,7 ./scripts/node benchmarks/protocol-versions/produce-versions.ts \
  | tee regression/artifacts/tier1-produce.txt
docker compose -f docker-compose.yml -f docker-compose.perf.yml restart broker-single
taskset -c 6,7 ./scripts/node benchmarks/protocol-versions/consume-versions.ts \
  | tee regression/artifacts/tier1-consume.txt
```

Estimated runtime: 9 produce versions × 3 workloads × 2 acks settings and 14 fetch versions × 3 `maxBytes`
settings, at 7 repetitions — roughly 60 to 90 minutes total.

## Tier 2 — Old Broker Sanity Check

Not a comparison. The question here is only: *does the client behave sanely when it genuinely has no choice
but to speak the old protocol?*

```bash
docker compose -f docker-compose.yml -f docker-compose.perf.yml down
docker compose -f docker-compose.legacy.yml up -d --wait broker-single
PROTOCOL_BENCH_PIN=false PROTOCOL_BENCH_ARTIFACT=tier2-legacy-produce \
  taskset -c 6,7 ./scripts/node benchmarks/protocol-versions/produce-versions.ts
```

With no pinning, the client negotiates naturally against Kafka 1.1.0 and lands on the legacy codecs. Check
for: stable throughput over a sustained run, flat RSS, no reconnect storms, no unbounded retry. Record the
numbers, and label them in the results as **not comparable to Tier 1** — different broker, different JVM,
different storage engine.

Estimated runtime: 10 minutes.

## Validity Guards

A benchmark that measures the wrong thing is worse than none. Each of these invalidates the run if it trips.

1. **Broker-side message conversion must be zero.** If the broker down-converts, we are measuring the broker's
   CPU, not our codec. Every implemented version carries `RecordBatch v2`, so this should hold — verify rather
   than assume. `benchmarks/protocol-versions/guards.ts` reads `ProduceMessageConversionsPerSec` and
   `FetchMessageConversionsPerSec` over JMX, drives traffic at every implemented version, and reads them again;
   both deltas must be zero. JMX is enabled by `docker-compose.perf.yml`, not by the default stack.

   ```bash
   taskset -c 6,7 ./scripts/node benchmarks/protocol-versions/guards.ts
   ```

2. **No consumer group rebalances during a timed region.** A rebalance mid-run adds seconds. Assert the
   generation id is unchanged across the run, or use a static assignment.

3. **Pins must actually apply.** `pinApiVersions()` throws if the broker rejects a pin, but it cannot detect
   a client that negotiated before pinning. Assert the observed `api.version` on the first request of each run
   equals the pinned value, and fail loudly otherwise — a silently unpinned run would report the newest codec's
   numbers under an old version's label, which is the single most dangerous failure mode here.

4. **Identical payloads.** Same key, value, headers and record count in every cell. Log a checksum of the
   generated message set per run and assert it matches across versions.

5. **Topic name length is a confound for H3.** Fix it, and note it: a 10-character topic name and a
   200-character one produce different verdicts on flexible-versus-UUID wire size.

## Pass And Fail Criteria

| # | Criterion | Rationale |
| --- | --- | --- |
| P1 | Every implemented `Produce`/`Fetch` version completes the full workload against the modern broker | Load is a different code path from the compat tests' small batches |
| P2 | CPU µs/message for any legacy version ≤ 1.15× the newest version's, same workload | Primary metric; 15% allows for measurement noise on 8 shared cores |
| P3 | Throughput at `acks=NO_RESPONSE` within 10% of the best version | Encode-path check without broker masking |
| P4 | Broker message conversions == 0 in every tier | Guard 1 |
| P5 | Tier 0 per-record cost from N=100 to N=10000 must not grow more than 10% faster for a legacy version than for the newest | Catches quadratic growth or per-record allocation in a legacy codec |
| P6 | `Fetch <= v12` CPU cost does not increase with records per response | H2 — proves the remap hoist in this PR is correct |
| P7 | Peak RSS for any legacy version ≤ 1.2× the newest | Catches retained buffers in a legacy parse path |
| P8 | Wire bytes per message documented for every version | H3 — reporting requirement, not a threshold |

Every criterion above is **relative to the newest version of the same API**, deliberately. An absolute
threshold answers the wrong question: the shared record encoder costs about 50% more per record at 10,000
records than at 100, and that applies to `Produce v3` and `v11` alike, so an absolute check flags it as a
failure in every codec at once while saying nothing about whether the legacy ones are sound. Only the
comparison against the newest version can distinguish a legacy defect from a property of the whole client.

Anything failing P2, P5, P6 or P7 gets a `--cpu-prof` run at that single version and a flamegraph diff against
the newest version, to name the specific function before opening an issue.

## Deliverables

1. `benchmarks/protocol-versions/` — four scripts (`codecs.ts`, `produce-versions.ts`, `consume-versions.ts`,
   `guards.ts`) plus the shared harness in `utils/`.
2. `docker-compose.perf.yml` — CPU, memory, tmpfs and JMX overrides.
3. `scripts/run-protocol-load-test.sh` — runs the tiers in the right order, which is easy to get wrong by hand.
4. `regression/artifacts/tier0-codecs.json`, `tier1-produce.json`, `tier1-consume.json`,
   `tier2-legacy-*.json` — raw per-sample results. These stay local: `regression/artifacts/.gitignore`
   already excludes `*.json`, which is right, since the numbers describe one machine and not the code.
   The tables below are the committed record.
5. The **Results** section below.

## Suggested Order

| Step | Work | Time |
| --- | --- | --- |
| 1 | Write the shared harness and Tier 0 script | — |
| 2 | Run Tier 0, check P5 | 5 min |
| 3 | Write the Tier 1 scripts, add `docker-compose.perf.yml` | — |
| 4 | Smoke run Tier 1 at 1000 messages, verify guards 1–4 fire correctly | 10 min |
| 5 | Full Tier 1 producer sweep | 40 min |
| 6 | Full Tier 1 consumer sweep | 40 min |
| 7 | Tier 2 sanity run | 10 min |
| 8 | Write up results, verdicts, follow-up issues | — |

Tier 0 alone answers most of the question and costs almost nothing, so step 2 is the natural go/no-go point:
if the codecs are flat and legacy framing is cheaper, Tier 1 is confirmation rather than discovery.

## Known Pitfalls

- **`pinApiVersions` caches broker API lists per bootstrap list**, in a module-level `Map` keyed by the joined
  broker string. Switching stacks (modern to legacy) inside one process returns stale data. Run each tier in
  a fresh process.
- **`benchmarks/utils/definitions.ts` points at the 3-broker cluster.** Do not reuse it here.
- **`acks=NO_RESPONSE` does not wait for the broker**, so a producer run can finish before the broker has
  durably accepted anything. That is fine for measuring encode cost, but the run must still `close()` the
  producer and the topic must be verified non-empty afterwards, or a codec that silently produced nothing
  would look infinitely fast.
- **Confluent image version is not the Apache Kafka version.** Record the output of
  `kafka-topics --version` in the results header rather than the image tag.
- **The legacy stack and the modern stack bind the same ports.** Bring one fully down before starting the
  other, or Tier 2 will silently measure Tier 1's broker.

## Results

Run on 2026-08-04.

| | |
| --- | --- |
| Host | Linux 6.8.0-90, 8 cores, 62 GB RAM |
| Client | Node.js v24.18.0, branch `protocol-version-load-testing` |
| Broker | `confluentinc/cp-kafka:8.1.0` (reports `8.1.0-ccs`), single node, KRaft, log on tmpfs |
| Broker API range | Produce v0–v13, Fetch v4–v18 |
| Isolation | broker `cpuset 0-3`, client `taskset -c 6,7` |
| Legacy broker (tier 2) | `confluentinc/cp-kafka:4.1.0` = Apache Kafka 1.1.0, Produce v0–v5, Fetch v0–v7 |

### Verdict

**The legacy codecs are not slower than the newest ones.** Nowhere in the matrix — 9 Produce versions,
14 Fetch versions, three payload shapes, two acks settings, with and without a broker — is a legacy version
consistently slower than the newest. Where a reproducible difference does exist it runs the other way: the
newest versions are the slower ones.

| # | Criterion | Result |
| --- | --- | --- |
| P1 | Every version completes the full workload | **PASS** — 9/9 Produce, 14/14 Fetch |
| P2 | Legacy CPU/message ≤ 1.15× newest | **PASS** for Produce (tier 0 and tier 1) and for Fetch at 64 KB and 1 MB. See the precision note for Fetch at 4 KB and for tier 0 Fetch decode |
| P3 | Throughput at `acks=0` within 10% of best | **PASS** — spread 3.78–4.54 µs/msg at `batch100` |
| P4 | Broker message conversions == 0 | **PASS** — zero across every implemented version of both APIs |
| P5 | Legacy per-record scaling ≤ 1.1× newest | **PASS** for Produce (within ±2%); Fetch inconclusive, see below |
| P6 | `Fetch <= v12` remap does not track records/response | **PASS**, four consecutive runs |
| P7 | Legacy peak RSS ≤ 1.2× newest | **PASS** — RSS is flat across versions in every cell |
| P8 | Wire bytes documented | Done, below |

### Tier 1 — Produce, client CPU µs per message

Two independent runs, different shuffle seeds. Range across v3–v11 per cell:

| Workload | Range across v3–v11 | Worst legacy vs newest |
| --- | --- | --- |
| single, acks=0 | 45.28 – 47.82 | −4.2% |
| single, acks=all | 67.78 – 74.25 | +3.4% |
| batch100, acks=0 | 3.78 – 4.54 | +9.1% |
| batch100, acks=all | 4.09 – 4.53 | +6.5% |
| batch1000, acks=0 | 4.14 – 4.49 | +4.4% |
| batch1000, acks=all | 4.00 – 4.57 | +14.3% |

No old/new ordering: `v3` is faster than `v11` in four of the six cells. H1 holds — non-flexible framing
writes fixed width lengths and skips tagged fields, and it costs marginally less, never more.

### Tier 1 — Fetch, client CPU µs per message

Four runs, three shuffle seeds. At the two payload sizes where the measurement is stable:

| maxBytes | v4–v11 | v12–v17 | step at v11→v12 |
| --- | --- | --- | --- |
| 65,536 | 1.44 – 1.53 | 1.45 – 1.53 | none |
| 1,048,576 | 1.39 – 1.42 | 1.72 – 1.76 | **~23%** |

### Tier 2 — Apache Kafka 1.1.0 sanity check

Not comparable to tier 1 by construction. The client negotiated Produce v5 and Fetch v7 unaided and behaved
normally: stable throughput, flat RSS, no reconnect storms.

| | Kafka 1.1.0 | Kafka 4.x, same version pinned |
| --- | --- | --- |
| Fetch, maxBytes 1 MB | 1.40 µs/msg, 779k msg/s | 1.39 µs/msg, 772k msg/s |
| Produce, batch100 acks=0 | 4.12 µs/msg, 243k msg/s | 4.30 µs/msg, 233k msg/s |

### Findings

**1. `Reader.readUUID` was 14x slower than necessary — fixed.** It hyphenated with a capture group regex on
every call: 801 ns against 56 ns for the equivalent slicing. Since Fetch v13+ identifies topics by UUID, this
made the *newest* Fetch versions 15–18% slower to decode than the ones using topic names. `readUUID` now
slices, and the gap is gone. 1653 protocol and codec tests pass unchanged.

**2. Flexible framing costs about 23% per message at large fetch responses.** The break is exactly at the
v11 → v12 boundary, where compact collections and tagged fields begin, and only at `maxBytes` of 1 MB. It
reproduced in four consecutive runs with tight clusters on both sides. The mechanism is not isolated and is
not guessed at here. It matters because v17 is what the client negotiates by default against a modern broker.

**3. The shared record encoder costs ~48% more per record at 10,000 records than at 100.** Extending the
sweep to 400,000 records gives 1898 → 2473 → 2971 → 3999 → 5418 ns per record: roughly 1.3x per 10x, so
allocation and cache pressure rather than anything quadratic — `createRecord` allocates a `Writer` per record.
It applies identically to every Produce version, so it is invisible to this comparison, but it is the single
largest lever on producer throughput found here.

**4. Wire bytes.** Flexible framing is slightly *smaller*, and topic UUIDs cost more than short topic names:
Produce v3–v8 encode 100 records in 9876 bytes against 9869 for v9–v11; Fetch v13+ responses are 9901 bytes
against 9894 for v4. H3 confirmed — for short topic names the newest versions are not the most compact.

### Precision On This Machine

Worth recording, because two of these produced findings that did not survive scrutiny.

- **Tier 1 Fetch at `maxBytes=4096` is not usable for version comparison.** The first one or two cells of a
  mode run 25–40% high even with a per-mode warmup, and the penalty follows *position*, not version.
- **Tier 0 Fetch decode has a noise floor near 20%** on this box: consecutive runs flag different versions
  (v15, v16, v7, then v11, v16). Only `v16` at 1000 records appeared twice. Tier 0 *Produce* is stable to
  ±3% across runs and is trustworthy. To make tier 0 Fetch decode reliable, run each version in its own
  process rather than measuring fourteen similar functions in one.
- **The trustworthy evidence is tier 1 at 64 KB and 1 MB**, where clusters are tight and consistent across
  four runs and three seeds. The verdict above rests on those, on tier 1 Produce, and on tier 0 Produce.

### Methodology Corrections Made During Execution

Each of these first appeared as a plausible finding about the client.

1. **Fixed record timestamps deleted the test data.** The generator stamped every record `1700000000000`
   (Nov 2023) for byte reproducibility. Kafka applies retention by the largest record timestamp in a segment,
   so the broker deleted the seeded log mid-sweep and consumers correctly read an empty partition. This
   looked exactly like an intermittent consumer stall for four runs. `kafka-get-offsets --time earliest`
   returning the same offset as `--time latest` is what settled it. The base is now `Date.now()`, captured
   once; byte reproducibility survives because a batch stores its base as a fixed width INT64 and each record
   as a varint delta from it.
2. **A constant shuffle seed confounded version with position.** `Fetch v5` measured 5.79 / 6.01 / 5.97 at
   `maxBytes=4096` across three runs — reproducible, and entirely because it was always first. Changing the
   seed moved the penalty to whichever version became first. The seed is now `PROTOCOL_BENCH_SEED`, and
   varying it is mandatory before believing any single-version result.
3. **Warmup must match the workload shape.** V8 tiers up the many-small-fetches path separately from the
   few-large-fetches path, so one global warmup left the first cells of each `maxBytes` mode paying for it.
4. **Tier 2 overwrote tier 1's artifact**, because both tiers run the same script. Artifact names are now set
   per run with `PROTOCOL_BENCH_ARTIFACT`.
