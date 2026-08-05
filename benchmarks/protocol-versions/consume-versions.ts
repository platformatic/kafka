// Tier 1 (see README.md), consumer side: every implemented Fetch version against the same
// modern broker, reading the same pre-seeded log.
//
// The maxBytes sweep is the point rather than an extra: it varies records per response by roughly
// two orders of magnitude, which is what decides whether the Fetch <= v12 topic id remap in
// src/clients/consumer/consumer.ts costs per topic (bounded, fine) or per record (a real defect).

import { writeFileSync } from 'node:fs'
import { type Consumer, MessagesStreamModes, ProduceAcks } from '../../src/index.ts'
import { pinApiVersions } from '../../test/helpers/api-versions.ts'
import {
  assertNegotiated,
  brokerRange,
  closeAll,
  createAdmin,
  createConsumer,
  createProducer,
  createTopic,
  measure,
  negotiatedVersion,
  primeApis,
  usableVersionsFor,
  type LiveSample
} from './utils/live.ts'
import { median, shuffle, table } from './utils/measure.ts'
import { createMessages, defaultShape } from './utils/payload.ts'

const repetitions = Number(process.env.PROTOCOL_BENCH_REPETITIONS ?? 5)
const warmups = Number(process.env.PROTOCOL_BENCH_WARMUPS ?? 1)
const total = Number(process.env.PROTOCOL_BENCH_CONSUME ?? 50_000)
const stallTimeoutMs = Number(process.env.PROTOCOL_BENCH_STALL_MS ?? 30_000)
const stallRetries = Number(process.env.PROTOCOL_BENCH_STALL_RETRIES ?? 3)
const pin = process.env.PROTOCOL_BENCH_PIN !== 'false'
const seed = Number(process.env.PROTOCOL_BENCH_SEED ?? 0x5eed)
// Tier 2 runs the same script against the legacy broker, so it must not overwrite tier 1's results.
const artifact = process.env.PROTOCOL_BENCH_ARTIFACT ?? 'tier1-consume'

// Roughly 100 bytes per record on the wire, so these correspond to about 40, 650 and 10000 records
// per fetch response.
const maxBytesModes = [4_096, 65_536, 1_048_576]

interface Cell {
  version: number
  maxBytes: number
  samples: LiveSample[]
  cpuUsPerMessage: number
  messagesPerSecond: number
  peakRssMb: number
}

const admin = createAdmin()
const probe = createConsumer()

await primeApis(probe)

const range = await brokerRange(probe, 'Fetch')
const versions = pin ? await usableVersionsFor(probe, 'Fetch') : [await negotiatedVersion(probe, 'Fetch')]

console.log('Tier 1: Fetch versions against a live broker')
console.log(`  broker advertises Fetch ${range}; measuring v${versions.join(', v')}`)
console.log(`  ${repetitions} repetitions, ${warmups} warmup, ${total.toLocaleString()} messages per run\n`)

// One topic, seeded once, read by every version. Reseeding per version would give each one a
// differently laid out log.
//
// Seeded in small batches on purpose. A broker returns whole record batches and will return the
// first one even when it alone exceeds maxBytes, so seeding in 1000 record batches would pin every
// response at 1000 records and the maxBytes sweep below would vary nothing. At 10 records per batch
// each one is about a kilobyte, so maxBytes actually decides how many come back.
const seedBatch = 10
const topic = await createTopic(admin)
const seeder = createProducer()
const batch = createMessages(topic, { ...defaultShape, count: seedBatch })

for (let sent = 0; sent < total; sent += seedBatch) {
  await seeder.send({ messages: batch, acks: ProduceAcks.ALL })
}

await seeder.close()
console.log(`  seeded ${total.toLocaleString()} messages into one partition\n`)

// A global warmup, before any cell is measured.
//
// The per cell warmup repetition is not enough: V8 tiers up the fetch and deserialize path once per
// process, so without this the first two or three cells in the sweep are charged for it. That is
// not a small effect and it is not random — it lands on whichever versions the shuffle happens to
// put first, and if one of them is the newest version, every other version is then compared against
// an inflated baseline.
async function warmUp (maxBytes: number, rounds = 2): Promise<void> {
  const warmer = createConsumer({ minBytes: 1, maxBytes, maxWaitTime: 500, autocommit: false })

  for (let round = 0; round < rounds; round++) {
    const stream = await warmer.consume({ topics: [topic], mode: MessagesStreamModes.EARLIEST })
    let seen = 0

    await new Promise<void>((resolve, reject) => {
      stream.on('data', () => {
        if (++seen === total) {
          resolve()
        }
      })
      stream.on('error', reject)
    })

    await stream.close()
  }

  await warmer.close()
}

/**
 * One measured consume of the whole topic.
 *
 * The stall detector stays because a sweep that hangs tells you nothing, and this one did hang
 * repeatedly. The cause turned out to be in the benchmark rather than the client: the payload
 * generator stamped a fixed 2023 timestamp on every record, so the broker's retention thread
 * deleted the seeded log mid-sweep and consumers correctly read an empty partition. See the note
 * on baseTimestamp in utils/payload.ts.
 */
async function consumeOnce (
  consumer: Consumer,
  version: number,
  maxBytes: number,
  repetition: number
): Promise<LiveSample> {
  let stream: Awaited<ReturnType<Consumer['consume']>> | undefined

  try {
    stream = await consumer.consume({ topics: [topic], mode: MessagesStreamModes.EARLIEST })

    const sample = await measure(total - 1, async ({ restart }) => {
      await new Promise<void>((resolve, reject) => {
        let seen = 0
        let lastSeen = -1

        // A stalled run used to hang the whole sweep with no indication of which version or how far
        // it got. Failing with the count makes the difference between "delivered nothing" and
        // "delivered all but the last batch" obvious from the error alone.
        const stall = setInterval(() => {
          if (seen === lastSeen) {
            clearInterval(stall)
            reject(
              new Error(
                `Fetch v${version} maxBytes=${maxBytes} repetition ${repetition}: stalled at ${seen}/${total} messages`
              )
            )
            return
          }

          lastSeen = seen
        }, stallTimeoutMs)

        stream!.on('data', () => {
          seen++

          // The clock starts on the first record, so group join and the first round trip stay out.
          if (seen === 1) {
            restart()
          }

          if (seen === total) {
            clearInterval(stall)
            resolve()
          }
        })

        stream!.on('error', error => {
          clearInterval(stall)
          reject(error)
        })
      })
    })

    return sample
  } finally {
    // The stream must be closed before the consumer ever is: Consumer.close() refuses with
    // "Cannot leave group while consuming messages" while a stream is open, which turned a stalled
    // run into a failed teardown instead of a retry.
    if (stream) {
      await stream.close()
    }
  }
}

/** Stalls seen across the whole sweep, reported rather than silently retried away. */
const stalls: string[] = []

async function runCell (version: number, maxBytes: number): Promise<Cell> {
  const samples: LiveSample[] = []

  // One consumer per cell rather than one per repetition, purely to keep group churn down: a sweep
  // creates enough groups to make the coordinator's cleanup logging the loudest thing in the broker
  // log otherwise.
  const consumer = createConsumer({ minBytes: 1, maxBytes, maxWaitTime: 500, autocommit: false })

  try {
    if (pin) {
      await pinApiVersions(consumer, { Fetch: version })
      await assertNegotiated(consumer, 'Fetch', version)
    }

    for (let repetition = 0; repetition < warmups + repetitions; repetition++) {
      let sample: LiveSample | undefined

      // Retried, but counted. Dropping the cell would leave a version unmeasured, and silently
      // retrying would hide how often this happens.
      for (let attempt = 0; attempt < stallRetries && !sample; attempt++) {
        try {
          sample = await consumeOnce(consumer, version, maxBytes, repetition)
        } catch (error) {
          const message = (error as Error).message

          if (!message.includes('stalled at')) {
            throw error
          }

          stalls.push(message)
          process.stdout.write(`\n  STALL: ${message} (attempt ${attempt + 1}/${stallRetries})\n`)
        }
      }

      if (!sample) {
        throw new Error(`Fetch v${version} maxBytes=${maxBytes}: stalled ${stallRetries} times in a row`)
      }

      if (repetition >= warmups) {
        samples.push(sample)
      }
    }
  } finally {
    await consumer.close()
  }

  return {
    version,
    maxBytes,
    samples,
    cpuUsPerMessage: median(samples.map(sample => sample.cpuUsPerMessage)),
    messagesPerSecond: median(samples.map(sample => sample.messagesPerSecond)),
    peakRssMb: median(samples.map(sample => sample.peakRssMb))
  }
}

const cells: Cell[] = []

const abandoned: string[] = []

for (const maxBytes of maxBytesModes) {
  // Warmed at this maxBytes, not once globally. A sweep at 4096 issues a couple of thousand small
  // fetches per run where one at 1048576 issues a handful of large ones, and V8 tiers those up
  // separately: with a single global warmup the first one or two cells of each mode were still
  // paying for it, which showed up as a 40% penalty on whichever version the shuffle put first.
  // Changing the shuffle seed moved the penalty to the new first version, which is how it was
  // caught — it looked exactly like a slow codec until then.
  await warmUp(maxBytes)
  console.log(`  warmed up at maxBytes=${maxBytes}`)

  for (const version of shuffle(versions, seed + maxBytes)) {
    process.stdout.write(`  maxBytes=${maxBytes} v${version}: running...\r`)

    try {
      const cell = await runCell(version, maxBytes)

      cells.push(cell)
      process.stdout.write(
        `  maxBytes=${maxBytes} v${version}: ${cell.cpuUsPerMessage.toFixed(2)} us/msg, ${Math.round(cell.messagesPerSecond).toLocaleString()} msg/s\n`
      )
    } catch (error) {
      // A cell that cannot be measured is reported and skipped rather than taking the sweep with
      // it. Losing one version's number is bad; losing the other forty-one is worse.
      abandoned.push(`v${version} maxBytes=${maxBytes}: ${(error as Error).message}`)
      process.stdout.write(`  maxBytes=${maxBytes} v${version}: ABANDONED — ${(error as Error).message}\n`)
    }
  }
}

console.log('\nFetch, client CPU microseconds per message (lower is better):\n')

for (const maxBytes of maxBytesModes) {
  const forCell = cells.filter(cell => cell.maxBytes === maxBytes).sort((a, b) => a.version - b.version)

  if (!forCell.length) {
    console.log(`  maxBytes=${maxBytes.toLocaleString()}: no version completed\n`)
    continue
  }

  const newest = forCell.reduce((best, cell) => (cell.version > best.version ? cell : best))

  console.log(`  maxBytes=${maxBytes.toLocaleString()} (about ${Math.round(maxBytes / 100).toLocaleString()} records per response), ${forCell.length}/${versions.length} versions measured:`)
  console.log(
    table(
      ['version', 'remap', 'us/msg', 'vs newest', 'msg/s', 'peak RSS MB', 'verdict'],
      forCell.map(cell => [
        `v${cell.version}${cell.version === newest.version ? ' (newest)' : ''}`,
        cell.version <= 12 ? 'yes' : 'no',
        cell.cpuUsPerMessage.toFixed(2),
        cell.version === newest.version ? '-' : `${((cell.cpuUsPerMessage / newest.cpuUsPerMessage - 1) * 100).toFixed(1)}%`,
        Math.round(cell.messagesPerSecond).toLocaleString(),
        cell.peakRssMb.toFixed(0),
        cell.version === newest.version ? '-' : cell.cpuUsPerMessage / newest.cpuUsPerMessage > 1.15 ? 'FAIL' : 'ok'
      ])
    )
  )
  console.log()
}

// P6: if the remap cost tracked records rather than topics, the gap between the remapping versions
// and the rest would grow with records per response. Across a 250x range it must not.
console.log('P6 — does the Fetch <= v12 topic id remap track records per response?\n')

const remapGaps = maxBytesModes
  .map(maxBytes => {
    const forCell = cells.filter(cell => cell.maxBytes === maxBytes)
    const remapping = forCell.filter(cell => cell.version <= 12).map(cell => cell.cpuUsPerMessage)
    const direct = forCell.filter(cell => cell.version > 12).map(cell => cell.cpuUsPerMessage)

    return { maxBytes, remap: median(remapping), direct: median(direct), gap: median(remapping) - median(direct), sides: remapping.length && direct.length }
  })
  // A mode where one side of the comparison went unmeasured cannot contribute to P6.
  .filter(entry => entry.sides)

console.log(
  table(
    ['records/response', 'v<=12 us/msg', 'v>=13 us/msg', 'gap us/msg'],
    remapGaps.map(entry => [
      `~${Math.round(entry.maxBytes / 100).toLocaleString()}`,
      entry.remap.toFixed(3),
      entry.direct.toFixed(3),
      entry.gap.toFixed(3)
    ])
  )
)

if (remapGaps.length < 2) {
  console.log('\n  INCONCLUSIVE — fewer than two maxBytes modes produced both sides of the comparison')
} else {
  const smallest = remapGaps[0]!
  const largest = remapGaps.at(-1)!
  const grew = Math.abs(largest.gap) > Math.abs(smallest.gap) * 2 && Math.abs(largest.gap) > 0.05

  console.log(
    `\n  ${grew ? 'FAIL — the gap grows with records per response, so the remap is not per topic' : 'PASS — the gap does not grow with records per response'}`
  )
}

const failures = cells.filter(cell => {
  const newest = cells.filter(other => other.maxBytes === cell.maxBytes).reduce((best, other) => (other.version > best.version ? other : best))

  return cell.version !== newest.version && cell.cpuUsPerMessage / newest.cpuUsPerMessage > 1.15
})

console.log(
  failures.length === 0
    ? '\nP2 PASS — no legacy Fetch version costs more than 15% extra CPU per message'
    : `\nP2 FAIL — ${failures.length} cell(s) over 15%: ${failures.map(cell => `v${cell.version} maxBytes=${cell.maxBytes}`).join(', ')}`
)

if (stalls.length || abandoned.length) {
  console.log(`\nStalls: ${stalls.length} consume attempt(s) stopped receiving mid-stream; ${abandoned.length} cell(s) abandoned.`)
  console.log('  Not a codec property — it lands on a different version every run, always at the')
  console.log('  smallest maxBytes, where the fetch round trip count is highest. Detail in the artifact.')

  for (const entry of abandoned) {
    console.log(`    abandoned: ${entry}`)
  }
}

writeFileSync(
  new URL(`../../regression/artifacts/${artifact}.json`, import.meta.url),
  JSON.stringify(
    { tier: 1, api: 'Fetch', node: process.version, brokerRange: range, pinned: pin, repetitions, total, cells, remapGaps, stalls, abandoned },
    null,
    2
  )
)

console.log(`\nWrote regression/artifacts/${artifact}.json`)

await closeAll()
