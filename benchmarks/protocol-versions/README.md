# Protocol version benchmarks

Measures every `Produce` and `Fetch` version this package implements, to answer one question: **are the
legacy codecs as fast as the newest ones?**

`Base[kGetApi]` always negotiates the highest version a broker advertises, so against any broker in CI the
newest codec always wins and the rest are never serialized, sent or parsed. `test/integration/*.compat-test.ts`
covers them for correctness. This suite covers them for speed.

Raw per-sample results land in `regression/artifacts/`. The recorded verdict is [below](#recorded-results).

## Quick start

```bash
# Everything, in the right order (~90 minutes)
./scripts/run-protocol-load-test.sh

# Or one tier at a time
./scripts/run-protocol-load-test.sh 0    # codecs, no Docker
./scripts/run-protocol-load-test.sh 1    # live broker
./scripts/run-protocol-load-test.sh 2    # Apache Kafka 1.1.0 sanity check
```

Tier 0 needs nothing but Node. Tiers 1 and 2 need Docker.

## The one design decision worth knowing

The comparison is **the same broker, with the client's codec pinned**. That makes the protocol version the
only variable.

Comparing a Kafka 1.1.0 broker against a Kafka 4.x broker would look like the same experiment and is not: it
varies the JVM, the storage engine, the on-disk format, the config defaults *and* the codec at once, so a
difference cannot be attributed to our code. That run still happens, as tier 2, but as a sanity check that
nothing pathological occurs against a genuinely old broker — never as a measurement. Its numbers are labelled
non-comparable wherever they appear.

Pinning works through `pinApiVersions()` in `test/helpers/api-versions.ts`, which rewrites `client[kApis]`.
A Kafka 4.x broker still accepts every version implemented here — KIP-896 set the 4.0 floor at exactly
`Produce v3` and `Fetch v4` — so the whole matrix runs against one broker.

## What each script does

| Script | Tier | Broker | Measures |
| --- | --- | --- | --- |
| `codecs.ts` | 0 | none | `createRequest` for Produce v3–v11 and `parseResponse` for Fetch v4–v17, over 1 / 10 / 100 / 1000 / 10000 records. Reports ns/op, ns/record and exact wire bytes |
| `produce-versions.ts` | 1, and 2 unpinned | live | Every Produce version, three batch sizes, `acks=0` and `acks=all` |
| `consume-versions.ts` | 1, and 2 unpinned | live | Every Fetch version at `maxBytes` 4 KB / 64 KB / 1 MB, which varies records per response by ~250x |
| `guards.ts` | guard 1 | live | Proves the broker performs **zero** record batch conversions, so the sweeps measure our codecs and not the broker's down-conversion work |

A second guard has no script of its own: every live cell asserts that the version the client negotiated equals
the version it was pinned to (`assertNegotiated()` in `utils/live.ts`). A silently unpinned run would report
the newest codec's numbers under an old version's label, which is the one failure mode here that yields a
confident wrong answer.

`utils/` holds the shared harness: `codecs.ts` (version discovery and per-version traits), `payload.ts`
(deterministic records), `fetch-response.ts` (response synthesizer), `measure.ts` (timing, medians, shuffle),
`live.ts` (client factories, CPU/GC/RSS sampling, pin assertions).

## The primary metric is CPU, not throughput

**Client CPU microseconds per message.** With `acks=all`, or any broker round trip, wall clock is dominated by
the broker and every version looks identical — which would hide a real codec regression. Throughput is
reported alongside, but it is the secondary number.

## Configuration

All optional.

| Variable | Default | Notes |
| --- | --- | --- |
| `PROTOCOL_BENCH_BROKERS` | `localhost:9001` | Single broker, deliberately: cluster replication adds variance unrelated to the codec |
| `PROTOCOL_BENCH_REPETITIONS` | `5` | Measured runs per cell; the median is reported |
| `PROTOCOL_BENCH_WARMUPS` | `1` | Discarded runs per cell, on top of the global warmup |
| `PROTOCOL_BENCH_SEED` | `0x5eed` | Shuffle seed for cell order. **Vary this** — see below |
| `PROTOCOL_BENCH_ARTIFACT` | per script | Output filename under `regression/artifacts/` |
| `PROTOCOL_BENCH_PIN` | `true` | `false` lets the client negotiate naturally; this is what tier 2 uses |
| `PROTOCOL_BENCH_SINGLE` | `20000` | Messages for the single-message producer workload |
| `PROTOCOL_BENCH_BATCH` | `100000` | Messages for the batched producer workloads |
| `PROTOCOL_BENCH_CONSUME` | `50000` | Messages seeded and consumed per Fetch run |
| `PROTOCOL_BENCH_STALL_MS` | `30000` | No-progress timeout before a consume is declared stalled |
| `PROTOCOL_BENCH_STALL_RETRIES` | `3` | Stalled attempts before a cell is abandoned and reported |

A quick smoke run, roughly two minutes:

```bash
PROTOCOL_BENCH_REPETITIONS=1 PROTOCOL_BENCH_WARMUPS=0 \
PROTOCOL_BENCH_CONSUME=5000 PROTOCOL_BENCH_BATCH=5000 \
  taskset -c 6,7 ./scripts/node benchmarks/protocol-versions/consume-versions.ts
```

## Getting numbers that mean something

This suite produced three findings that did not survive scrutiny before it produced any that did. The
countermeasures are built in, but they only work if you use them.

**Vary the seed before believing any single-version result.** Cell order is shuffled, but with a fixed seed
it is the *same* shuffle every run, so a version that is always first is indistinguishable from a version
that is slow. `Fetch v5` measured 5.79 / 6.01 / 5.97 µs/msg across three runs and looked like a real
regression; it was first in the order every time. Change `PROTOCOL_BENCH_SEED` and re-run. If the penalty
follows the position rather than the version, it is not a finding.

**Pin CPUs.** The broker JVM and the client contend on any machine with few cores, and an unpinned run
attributes scheduler noise to whichever codec was running. `docker-compose.perf.yml` gives the broker
`cpuset 0-3` and puts its log on tmpfs; run the client under `taskset -c 6,7`.

**Run one sweep at a time.** Two concurrent sweeps produced a full set of plausible, entirely wrong numbers.
To stop a run, match the node process rather than the pattern — `pkill -f consume-versions` also matches the
shell you type it in, which kills the wrapper and leaves the benchmark running:

```bash
for p in $(pgrep -x node); do
  grep -qa protocol-versions /proc/$p/cmdline 2>/dev/null && kill -9 $p
done
```

**Know the noise floor.** On an 8-core box, tier 1 Fetch at `maxBytes=4096` and tier 0 Fetch decode both sit
near 20%, above the 15% threshold the criteria use. Cells that trip the threshold in one run and not the next
are noise; a finding is something that reproduces across seeds. Tier 1 at 64 KB and 1 MB, and everything on
the Produce side, are stable to a few percent.

**Check the guard.** `guards.ts` must report zero conversions. If the broker is down-converting, the sweeps
are measuring its CPU and not ours. It needs JMX, which `docker-compose.perf.yml` enables and the default
stack does not.

## Record timestamps

`payload.ts` bases record timestamps on `Date.now()`, captured once per process. It is tempting to hardcode a
constant for byte-identical runs — do not. Kafka applies retention by the largest record timestamp in a
segment, so records dated before the retention window get their segment deleted mid-sweep, and consumers then
read an empty partition and look like they have stalled. Byte reproducibility is unaffected by the change: a
batch stores its base timestamp as a fixed-width INT64 and each record as a varint delta from it, so moving
the base changes the values but never the lengths.

If a consume ever does stall, `kafka-get-offsets --time earliest` against the topic is the first thing to
check. Matching `--time latest` means the log is empty and the client is behaving correctly.

## Output

Human-readable tables on stdout, plus JSON with every individual sample under `regression/artifacts/`. Those
are gitignored: they describe one machine, not the code. The committed record is the section below.

## Recorded results

Run 2026-08-04 on Linux 6.8.0-90, 8 cores, 62 GB RAM, Node v24.18.0, against `confluentinc/cp-kafka:8.1.0`
(single node, KRaft, log on tmpfs, advertising Produce v0–v13 and Fetch v4–v18). Tier 2 used
`confluentinc/cp-kafka:4.1.0`, which is Apache Kafka 1.1.0.

**The legacy codecs are not slower than the newest ones.** Across 9 Produce versions, 14 Fetch versions,
three payload shapes and two acks settings, no legacy version is consistently slower than the newest. Where a
reproducible difference exists it runs the other way.

| | Produce v3–v11 | Fetch v4–v11 | Fetch v12–v17 |
| --- | --- | --- | --- |
| CPU µs/msg, 1 MB fetches | 4.00–4.57, no ordering | **1.39–1.42** | **1.72–1.76** |

`v3` is faster than `v11` in four of six producer cells. Peak RSS is flat across versions everywhere. Broker
message conversions were zero for every implemented version of both APIs. Against Apache Kafka 1.1.0 the
client negotiated Produce v5 / Fetch v7 unaided and matched the modern broker to within 1% (1.40 vs
1.39 µs/msg at 1 MB fetches), with stable throughput and flat RSS.

Two findings, both in the *newest* versions:

1. **`Reader.readUUID` was 14x slower than necessary — fixed.** It hyphenated with a capture-group regex,
   801 ns against 56 ns for the equivalent slicing. Fetch v13+ identifies topics by UUID, so this made the
   newest Fetch versions 15–18% slower to decode than the name-based ones.
2. **Flexible framing costs ~23% per message at 1 MB fetch responses**, breaking exactly at the v11→v12
   boundary where compact collections and tagged fields begin. Four consecutive runs, tight clusters either
   side. Not fixed, and the mechanism is not isolated — recorded rather than guessed at. It matters because
   v17 is what the client negotiates by default against a modern broker.

Version-independent, so invisible to this comparison but worth knowing: the shared record encoder costs ~48%
more per record at 10,000 records than at 100 (1898 → 5418 ns/record out to 400k). Roughly 1.3x per 10x, so
allocation and cache pressure — `createRecord` allocates a `Writer` per record — not anything quadratic.

Wire bytes, confirming that the newest versions are not the most compact for short topic names: Produce v3–v8
encode 100 records in 9876 bytes against 9869 for v9–v11; Fetch v13+ responses are 9901 bytes against 9894
for v4.

The verdict rests on tier 1 at 64 KB and 1 MB and on everything Produce-side, where clusters are tight across
four runs and three shuffle seeds. It does **not** rest on tier 1 Fetch at `maxBytes=4096` or on tier 0 Fetch
decode; see the noise floor note above.

## Adding a version

Nothing to register. Versions are discovered from `src/apis/index.ts` through `implementedVersions()`, and the
sweeps intersect that with what the broker advertises, so a new `produce-v12.ts` is picked up automatically.
A new Fetch version needs one thing: if it changes the response layout, add the trait to
`fetchResponseTraits()` in `utils/codecs.ts` and handle it in `createFetchResponse()`. The tier 0 self-check
parses what it writes with the real codec and asserts the record count survives, so getting this wrong fails
loudly rather than quietly measuring a malformed buffer.
