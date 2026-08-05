// Tier 1 (see README.md), producer side: every implemented Produce version against the same
// modern broker, with the codec as the only variable.

import { writeFileSync } from 'node:fs'
import { ProduceAcks } from '../../src/index.ts'
import { pinApiVersions } from '../../test/helpers/api-versions.ts'
import {
  assertNegotiated,
  brokerRange,
  closeAll,
  createAdmin,
  createProducer,
  createTopic,
  measure,
  negotiatedVersion,
  primeApis,
  usableVersionsFor,
  type LiveSample
} from './utils/live.ts'
import { median, shuffle, table } from './utils/measure.ts'
import { createMessages, defaultShape, payloadChecksum } from './utils/payload.ts'

const repetitions = Number(process.env.PROTOCOL_BENCH_REPETITIONS ?? 5)
const warmups = Number(process.env.PROTOCOL_BENCH_WARMUPS ?? 1)
const singleMessages = Number(process.env.PROTOCOL_BENCH_SINGLE ?? 20_000)
const batchMessages = Number(process.env.PROTOCOL_BENCH_BATCH ?? 100_000)
const pin = process.env.PROTOCOL_BENCH_PIN !== 'false'
const seed = Number(process.env.PROTOCOL_BENCH_SEED ?? 0x5eed)
// Tier 2 runs the same script against the legacy broker, so it must not overwrite tier 1's results.
const artifact = process.env.PROTOCOL_BENCH_ARTIFACT ?? 'tier1-produce'

interface Workload {
  name: string
  batchSize: number
  messages: number
}

const workloads: Workload[] = [
  { name: 'single', batchSize: 1, messages: singleMessages },
  { name: 'batch100', batchSize: 100, messages: batchMessages },
  { name: 'batch1000', batchSize: 1000, messages: batchMessages }
]

const acksModes = [
  { name: 'acks=0', value: ProduceAcks.NO_RESPONSE },
  { name: 'acks=all', value: ProduceAcks.ALL }
]

interface Cell {
  version: number
  workload: string
  acks: string
  samples: LiveSample[]
  cpuUsPerMessage: number
  messagesPerSecond: number
  peakRssMb: number
}

const admin = createAdmin()
const probe = createProducer()

await primeApis(probe)

const range = await brokerRange(probe, 'Produce')
const versions = pin ? await usableVersionsFor(probe, 'Produce') : [await negotiatedVersion(probe, 'Produce')]

console.log('Tier 1: Produce versions against a live broker')
console.log(`  broker advertises Produce ${range}; measuring v${versions.join(', v')}`)
console.log(`  ${repetitions} repetitions, ${warmups} warmup, pinning ${pin ? 'on' : 'off'}\n`)

// A global warmup, on top of the per cell one: V8 tiers up the send path once per process, and
// without this the first cell in the sweep is charged for it no matter which version it happens
// to be. The shuffled cell order spreads drift, but it cannot undo a one off cost.
{
  const topic = await createTopic(admin)
  const warmup = createProducer()
  const messages = createMessages(topic, { ...defaultShape, count: 100 })

  for (let batch = 0; batch < 200; batch++) {
    await warmup.send({ messages, acks: ProduceAcks.NO_RESPONSE })
  }

  await warmup.close()
  console.log('  warmed up\n')
}

const cells: Cell[] = []

async function runCell (version: number, workload: Workload, acks: { name: string, value: number }): Promise<Cell> {
  const topic = await createTopic(admin)
  const messages = createMessages(topic, { ...defaultShape, count: workload.batchSize })
  const batches = Math.ceil(workload.messages / workload.batchSize)
  const samples: LiveSample[] = []

  for (let repetition = 0; repetition < warmups + repetitions; repetition++) {
    const producer = createProducer()

    if (pin) {
      await pinApiVersions(producer, { Produce: version })
      await assertNegotiated(producer, 'Produce', version)
    }

    // Warm the connection and metadata so the first timed batch is not paying for them.
    await producer.send({ messages: messages.slice(0, 1), acks: ProduceAcks.LEADER })

    const sample = await measure(batches * workload.batchSize, async (): Promise<void> => {
      for (let batch = 0; batch < batches; batch++) {
        await producer.send({ messages, acks: acks.value })
      }
    })

    await producer.close()

    if (repetition >= warmups) {
      samples.push(sample)
    }
  }

  return {
    version,
    workload: workload.name,
    acks: acks.name,
    samples,
    cpuUsPerMessage: median(samples.map(sample => sample.cpuUsPerMessage)),
    messagesPerSecond: median(samples.map(sample => sample.messagesPerSecond)),
    peakRssMb: median(samples.map(sample => sample.peakRssMb))
  }
}

for (const workload of workloads) {
  for (const acks of acksModes) {
    // Interleaved: one pass over a shuffled version list per repetition would be ideal, but a cell
    // owns its topic and its clients, so the shuffle is applied to the cell order instead.
    for (const version of shuffle(versions, seed + workload.batchSize)) {
      const cell = await runCell(version, workload, acks)

      cells.push(cell)
      process.stdout.write(
        `  ${workload.name}/${acks.name} v${version}: ${cell.cpuUsPerMessage.toFixed(2)} us/msg, ${Math.round(cell.messagesPerSecond).toLocaleString()} msg/s\n`
      )
    }
  }
}

console.log('\nProduce, client CPU microseconds per message (lower is better):\n')

for (const workload of workloads) {
  for (const acks of acksModes) {
    const forCell = cells.filter(cell => cell.workload === workload.name && cell.acks === acks.name).sort((a, b) => a.version - b.version)
    const newest = forCell.reduce((best, cell) => (cell.version > best.version ? cell : best))

    console.log(`  ${workload.name}, ${acks.name} (${workload.messages.toLocaleString()} messages):`)
    console.log(
      table(
        ['version', 'us/msg', 'vs newest', 'msg/s', 'peak RSS MB', 'verdict'],
        forCell.map(cell => [
          `v${cell.version}${cell.version === newest.version ? ' (newest)' : ''}`,
          cell.cpuUsPerMessage.toFixed(2),
          cell.version === newest.version ? '-' : `${((cell.cpuUsPerMessage / newest.cpuUsPerMessage - 1) * 100).toFixed(1)}%`,
          Math.round(cell.messagesPerSecond).toLocaleString(),
          cell.peakRssMb.toFixed(0),
          cell.version === newest.version
            ? '-'
            : cell.cpuUsPerMessage / newest.cpuUsPerMessage > 1.15
              ? 'FAIL'
              : 'ok'
        ])
      )
    )
    console.log()
  }
}

const failures = cells.filter(cell => {
  const newest = cells
    .filter(other => other.workload === cell.workload && other.acks === cell.acks)
    .reduce((best, other) => (other.version > best.version ? other : best))

  return cell.version !== newest.version && cell.cpuUsPerMessage / newest.cpuUsPerMessage > 1.15
})

console.log(
  failures.length === 0
    ? 'P2 PASS — no legacy Produce version costs more than 15% extra CPU per message'
    : `P2 FAIL — ${failures.length} cell(s) over 15%: ${failures.map(cell => `v${cell.version} ${cell.workload}/${cell.acks}`).join(', ')}`
)

writeFileSync(
  new URL(`../../regression/artifacts/${artifact}.json`, import.meta.url),
  JSON.stringify(
    {
      tier: 1,
      api: 'Produce',
      node: process.version,
      brokerRange: range,
      pinned: pin,
      repetitions,
      checksum: payloadChecksum(createMessages('x', { ...defaultShape, count: 100 })),
      cells
    },
    null,
    2
  )
)

console.log(`\nWrote regression/artifacts/${artifact}.json`)

await closeAll()
