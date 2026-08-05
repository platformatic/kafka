# Protocol version benchmarks

Measures every `Produce` and `Fetch` version this package implements, to answer one question: **are the
legacy codecs as fast as the newest ones?**

`Base[kGetApi]` always negotiates the highest version a broker advertises, so against any broker in CI the
newest codec always wins and the rest are never serialized, sent or parsed. `test/integration/*.compat-test.ts`
covers them for correctness. This suite covers them for speed.

The plan, the criteria and the recorded results live in [`../../LOAD_TESTING.md`](../../LOAD_TESTING.md).
This file is how to run it.

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

| Script | Broker | Measures |
| --- | --- | --- |
| `codecs.ts` | none | `createRequest` for Produce v3–v11 and `parseResponse` for Fetch v4–v17, over 1 / 10 / 100 / 1000 / 10000 records. Reports ns/op, ns/record and exact wire bytes |
| `produce-versions.ts` | live | Every Produce version, three batch sizes, `acks=0` and `acks=all` |
| `consume-versions.ts` | live | Every Fetch version at `maxBytes` 4 KB / 64 KB / 1 MB, which varies records per response by ~250x |
| `guards.ts` | live | Validity guard: proves the broker performs **zero** record batch conversions, so the sweeps measure our codecs and not the broker's down-conversion work |

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
are gitignored: they describe one machine, not the code. The committed record is the results section of
`LOAD_TESTING.md`.

## Adding a version

Nothing to register. Versions are discovered from `src/apis/index.ts` through `implementedVersions()`, and the
sweeps intersect that with what the broker advertises, so a new `produce-v12.ts` is picked up automatically.
A new Fetch version needs one thing: if it changes the response layout, add the trait to
`fetchResponseTraits()` in `utils/codecs.ts` and handle it in `createFetchResponse()`. The tier 0 self-check
parses what it writes with the real codec and asserts the record count survives, so getting this wrong fails
loudly rather than quietly measuring a malformed buffer.
