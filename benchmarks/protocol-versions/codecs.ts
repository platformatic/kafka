// Tier 0 (see README.md): protocol codec cost with no broker, no sockets and no scheduler noise.
//
// This is the tier that can prove an algorithmic defect. Everything downstream measures the same
// codecs with a broker attached, which only ever adds variance.

import { writeFileSync } from 'node:fs'
import { createRecordsBatch, type MessageRecord } from '../../src/protocol/records.ts'
import { Reader } from '../../src/protocol/reader.ts'
import { type Writer } from '../../src/protocol/writer.ts'
import { codecs, produceIsFlexible } from './utils/codecs.ts'
import { createFetchResponse, topicIdFor } from './utils/fetch-response.ts'
import { iterationsFor, shuffle, table, time, type Timing } from './utils/measure.ts'
import { createMessages, defaultShape, payloadChecksum } from './utils/payload.ts'

const topic = 'protocol-versions'
const topicId = topicIdFor(topic)
const counts = [1, 10, 100, 1000, 10000]
const repetitions = 9
const warmups = 2
const seed = 0x5eed

interface Row {
  api: string
  version: number
  flexible: boolean
  count: number
  bytes: number
  timing: Timing
}

const produceRows: Row[] = []
const fetchDecodeRows: Row[] = []
const recordsBaseline = new Map<number, Timing>()
const checksums = new Map<number, string>()

// One record set per size, shared by every version, so no version can be measured on different bytes.
const payloads = new Map<number, MessageRecord[]>()
const batches = new Map<number, Writer>()

for (const count of counts) {
  const messages = createMessages(topic, { ...defaultShape, count })

  payloads.set(count, messages)
  checksums.set(count, payloadChecksum(messages))
  batches.set(count, createRecordsBatch(messages, {}))
}

console.log('Tier 0: protocol codec microbenchmark')
console.log(`  node ${process.version}, ${repetitions} repetitions, ${warmups} warmup blocks`)
console.log(`  payload checksums: ${counts.map(count => `${count}=${checksums.get(count)}`).join(' ')}`)
console.log()

// The record batch encoder is shared by every Produce version, so measuring it separately is what
// makes the framing cost of a version recoverable from its total.
for (const count of counts) {
  const messages = payloads.get(count)!

  recordsBaseline.set(
    count,
    time(() => { createRecordsBatch(messages, {}) }, {
      iterations: iterationsFor(count),
      repetitions,
      warmups
    })
  )
}

// Versions are interleaved rather than run to completion one at a time: with all repetitions of v3
// first and all of v11 last, thermal drift and JIT warmth would be indistinguishable from a version
// effect.
const produceCodecs = codecs('Produce')
const fetchCodecs = codecs('Fetch')

for (const count of counts) {
  const messages = payloads.get(count)!

  for (const produce of shuffle(produceCodecs, seed + count)) {
    const request = produce.createRequest(1, 0, messages, {})

    produceRows.push({
      api: 'Produce',
      version: produce.version,
      flexible: produceIsFlexible(produce.version),
      count,
      bytes: request.length,
      timing: time(() => { produce.createRequest(1, 0, messages, {}) }, {
        iterations: iterationsFor(count),
        repetitions,
        warmups
      })
    })
  }

  for (const fetch of shuffle(fetchCodecs, seed + count)) {
    const response = createFetchResponse(fetch.version, {
      topic,
      topicId,
      partitions: [{ index: 0, records: batches.get(count)! }]
    })
    const buffer = response.buffer

    // Self check: the synthesizer is only trustworthy if the real parser agrees with it.
    const parsed = fetch.parseResponse(1, 1, fetch.version, Reader.from(buffer)) as {
      responses: { partitions: { records: { records: unknown[] }[] | null }[] }[]
    }
    const decoded = parsed.responses[0]?.partitions[0]?.records?.reduce((total, batch) => total + batch.records.length, 0)

    if (decoded !== count) {
      throw new Error(`Fetch v${fetch.version}: synthesized response decoded ${decoded} records, expected ${count}`)
    }

    fetchDecodeRows.push({
      api: 'Fetch',
      version: fetch.version,
      flexible: fetch.version >= 12,
      count,
      bytes: buffer.length,
      timing: time(() => { fetch.parseResponse(1, 1, fetch.version, Reader.from(buffer)) }, {
        iterations: iterationsFor(count),
        repetitions,
        warmups
      })
    })
  }
}

function report (title: string, rows: Row[], baseline?: Map<number, Timing>): void {
  console.log(title)

  for (const count of counts) {
    const forCount = rows.filter(row => row.count === count).sort((a, b) => a.version - b.version)
    const best = Math.min(...forCount.map(row => row.timing.nsPerOp))
    const records = baseline?.get(count)

    console.log(`\n  ${count} record${count === 1 ? '' : 's'} per call:`)
    console.log(
      table(
        ['version', 'framing', 'ns/op', 'ns/record', 'vs best', 'spread', records ? 'framing ns' : 'bytes', 'bytes'],
        forCount.map(row => [
          `v${row.version}`,
          row.flexible ? 'flexible' : 'fixed',
          row.timing.nsPerOp.toFixed(0),
          (row.timing.nsPerOp / row.count).toFixed(1),
          `${((row.timing.nsPerOp / best - 1) * 100).toFixed(1)}%`,
          `${(row.timing.spread * 100).toFixed(1)}%`,
          records ? Math.max(0, row.timing.nsPerOp - records.nsPerOp).toFixed(0) : String(row.bytes),
          String(row.bytes)
        ])
      )
    )
  }

  console.log()
}

report('\nProduce: request encoding (createRequest)', produceRows, recordsBaseline)
report('Fetch: response decoding (parseResponse)', fetchDecodeRows)

const allRows = [...produceRows, ...fetchDecodeRows]

// P2, at codec level: every legacy version against the newest one for the same API and payload.
// This, not the absolute numbers above, is what the branch is actually on trial for.
console.log('P2 — legacy versions against the newest version of the same API:\n')

const versusNewest = allRows
  .map(row => {
    const newest = allRows
      .filter(other => other.api === row.api && other.count === row.count)
      .reduce((best, other) => (other.version > best.version ? other : best))

    return {
      api: row.api,
      version: row.version,
      count: row.count,
      delta: row.timing.nsPerOp / newest.timing.nsPerOp - 1,
      bytesDelta: row.bytes - newest.bytes,
      isNewest: row.version === newest.version
    }
  })
  .filter(entry => !entry.isNewest)

console.log(
  table(
    ['api', 'worst legacy version', 'payload', 'slower than newest', 'verdict'],
    ['Produce', 'Fetch'].flatMap(api =>
      counts.map(count => {
        const worst = versusNewest
          .filter(entry => entry.api === api && entry.count === count)
          .reduce((a, b) => (b.delta > a.delta ? b : a))

        return [
          api,
          `v${worst.version}`,
          `${count} record${count === 1 ? '' : 's'}`,
          `${(worst.delta * 100).toFixed(1)}%`,
          worst.delta > 0.15 ? 'FAIL' : 'ok'
        ]
      })
    )
  )
)

const slowLegacy = versusNewest.filter(entry => entry.delta > 0.15)

console.log(`\n  ${slowLegacy.length === 0 ? 'PASS — no legacy codec is more than 15% slower than the newest' : `FAIL: ${slowLegacy.length} legacy measurement(s) over 15%`}`)

// P5: per record cost must not rise faster for a legacy codec than for the newest one.
//
// The absolute ratio is reported too, but it is not the criterion: it is dominated by the shared
// record encoder, which allocates a Writer per record and so pays more GC as batches grow. That
// applies identically to v3 and v11, so it cannot distinguish a legacy defect from a shared one.
console.log('\nP5 — per record scaling from 100 to 10000 records, against the newest version:\n')

const scaling = allRows
  .filter(row => row.count === 100)
  .map(row => {
    const large = allRows.find(other => other.api === row.api && other.version === row.version && other.count === 10000)!

    return {
      api: row.api,
      version: row.version,
      small: row.timing.nsPerOp / row.count,
      big: large.timing.nsPerOp / large.count,
      ratio: large.timing.nsPerOp / large.count / (row.timing.nsPerOp / row.count)
    }
  })

const scalingRows = scaling
  .map(entry => {
    const newest = scaling
      .filter(other => other.api === entry.api)
      .reduce((best, other) => (other.version > best.version ? other : best))

    return { ...entry, excess: entry.ratio / newest.ratio - 1, isNewest: entry.version === newest.version }
  })
  .sort((a, b) => b.excess - a.excess)

console.log(
  table(
    ['api/version', 'ns/record @100', 'ns/record @10000', 'ratio', 'vs newest ratio', 'verdict'],
    scalingRows.map(entry => [
      `${entry.api} v${entry.version}${entry.isNewest ? ' (newest)' : ''}`,
      entry.small.toFixed(1),
      entry.big.toFixed(1),
      entry.ratio.toFixed(2),
      entry.isNewest ? '-' : `${(entry.excess * 100).toFixed(1)}%`,
      entry.isNewest ? '-' : entry.excess > 0.1 ? 'FAIL' : 'ok'
    ])
  )
)

const failures = scalingRows.filter(entry => !entry.isNewest && entry.excess > 0.1)

console.log(
  `\n  ${failures.length === 0 ? 'PASS — no legacy codec degrades faster with batch size than the newest' : `FAIL: ${failures.length} codec(s) scale worse than the newest`}`
)
console.log(
  `  Note: the shared record encoder costs ${(scaling.find(entry => entry.api === 'Produce')!.ratio * 100 - 100).toFixed(0)}% more per record at 10000 than at 100, at every version alike.`
)

const artifact = {
  tier: 0,
  node: process.version,
  repetitions,
  checksums: Object.fromEntries(checksums),
  produce: produceRows,
  fetchDecode: fetchDecodeRows,
  recordsBaseline: Object.fromEntries(recordsBaseline),
  scaling: scalingRows,
  versusNewest
}

writeFileSync(
  new URL('../../regression/artifacts/tier0-codecs.json', import.meta.url),
  JSON.stringify(artifact, null, 2)
)

console.log('\nWrote regression/artifacts/tier0-codecs.json')
