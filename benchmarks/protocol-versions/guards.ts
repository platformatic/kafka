// Guard 1 of LOAD_TESTING.md: prove the broker is not down-converting record batches.
//
// If it were, the sweeps would be measuring the broker's conversion cost under the label of the
// client's codec, and every conclusion about the legacy versions would be wrong. Every version this
// package implements carries RecordBatch v2 (Produce v3 and Fetch v4 are exactly where that format
// arrived), so the expected answer is zero. Verifying beats assuming.

import { execFileSync } from 'node:child_process'
import { MessagesStreamModes, ProduceAcks } from '../../src/index.ts'
import { pinApiVersions } from '../../test/helpers/api-versions.ts'
import {
  assertNegotiated,
  closeAll,
  createAdmin,
  createConsumer,
  createProducer,
  createTopic,
  primeApis,
  usableVersionsFor
} from './utils/live.ts'
import { table } from './utils/measure.ts'
import { createMessages, defaultShape } from './utils/payload.ts'

const messagesPerVersion = 2_000

// JmxTool moved out of the kafka.tools package in Apache Kafka 4.0. Both names are tried so this
// guard also works against the 1.1.0 stack in tier 2.
const jmxToolClasses = ['org.apache.kafka.tools.JmxTool', 'kafka.tools.JmxTool']

function conversions (metric: string): number {
  let lastError: unknown

  for (const toolClass of jmxToolClasses) {
    try {
      const output = execFileSync(
        'docker',
        [
          'exec',
          'broker-single',
          'kafka-run-class',
          toolClass,
          '--jmx-url',
          'service:jmx:rmi:///jndi/rmi://localhost:9101/jmxrmi',
          '--object-name',
          `kafka.server:type=BrokerTopicMetrics,name=${metric}`,
          '--attributes',
          'Count',
          '--one-time',
          'true'
        ],
        { encoding: 'utf-8', stdio: ['ignore', 'pipe', 'ignore'] }
      )

      // JmxTool prints a CSV header line then one row of values.
      const rows = output.trim().split('\n')
      const values = rows.at(-1)!.split(',')

      return Number(values.at(-1)!.replace(/"/g, '')) || 0
    } catch (error) {
      lastError = error
    }
  }

  throw new Error(`Could not read ${metric} over JMX. Is docker-compose.perf.yml applied?`, { cause: lastError })
}

const admin = createAdmin()
const probe = createProducer()

await primeApis(probe)

const produceVersions = await usableVersionsFor(probe, 'Produce')
const consumerProbe = createConsumer()

await primeApis(consumerProbe)

const fetchVersions = await usableVersionsFor(consumerProbe, 'Fetch')

const before = {
  produce: conversions('ProduceMessageConversionsPerSec'),
  fetch: conversions('FetchMessageConversionsPerSec')
}

console.log('Guard 1: broker side record batch conversions')
console.log(`  starting counters: produce=${before.produce} fetch=${before.fetch}\n`)

const topic = await createTopic(admin)
const messages = createMessages(topic, { ...defaultShape, count: 1000 })

for (const version of produceVersions) {
  const producer = createProducer()

  await pinApiVersions(producer, { Produce: version })
  await assertNegotiated(producer, 'Produce', version)

  for (let sent = 0; sent < messagesPerVersion; sent += 1000) {
    await producer.send({ messages, acks: ProduceAcks.ALL })
  }

  await producer.close()
  process.stdout.write(`  produced at v${version}\n`)
}

for (const version of fetchVersions) {
  const consumer = createConsumer({ minBytes: 1, maxBytes: 65_536, maxWaitTime: 500, autocommit: false })

  await pinApiVersions(consumer, { Fetch: version })
  await assertNegotiated(consumer, 'Fetch', version)

  const stream = await consumer.consume({ topics: [topic], mode: MessagesStreamModes.EARLIEST })
  let seen = 0

  await new Promise<void>((resolve, reject) => {
    stream.on('data', () => {
      if (++seen === messagesPerVersion) {
        resolve()
      }
    })
    stream.on('error', reject)
  })

  await stream.close()
  await consumer.close()
  process.stdout.write(`  consumed at v${version}\n`)
}

const after = {
  produce: conversions('ProduceMessageConversionsPerSec'),
  fetch: conversions('FetchMessageConversionsPerSec')
}

console.log()
console.log(
  table(
    ['metric', 'before', 'after', 'delta', 'verdict'],
    [
      ['ProduceMessageConversions', before.produce, after.produce, after.produce - before.produce, after.produce === before.produce ? 'ok' : 'FAIL'],
      ['FetchMessageConversions', before.fetch, after.fetch, after.fetch - before.fetch, after.fetch === before.fetch ? 'ok' : 'FAIL']
    ]
  )
)

const clean = after.produce === before.produce && after.fetch === before.fetch

console.log(
  `\n  ${clean ? 'PASS — the broker converted nothing; the sweeps measure client codecs only' : 'FAIL — the broker is down-converting, so the sweeps are not measuring what they claim'}`
)

await closeAll()

process.exit(clean ? 0 : 1)
