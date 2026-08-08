import { ok, strictEqual } from 'node:assert'
import { randomUUID } from 'node:crypto'
import test from 'node:test'
import { FetchIsolationLevels, ProduceAcks } from '../../src/index.ts'
import {
  createConsumer,
  createProducer,
  createTopic,
  kafkaBootstrapServers,
  pinApiVersions,
  runAtVersion,
  stringDeserializers,
  stringSerializers,
  usableVersions
} from './helpers.ts'

// Transactions run against the cluster: the transaction state log needs more than one broker to be
// representative, and this is the only place the transaction coordinator APIs are exercised at all.

function createTransactionalProducer (t: any, pins: Record<string, number> = {}) {
  const producer = createProducer(t, {
    bootstrapBrokers: kafkaBootstrapServers,
    serializers: stringSerializers,
    idempotent: true,
    transactionalId: `compat-txn-${randomUUID()}`
  })

  return Object.keys(pins).length ? pinApiVersions(producer, pins) : Promise.resolve(producer)
}

async function readCommitted (t: any, topic: string, expected: number) {
  const consumer = createConsumer(t, {
    bootstrapBrokers: kafkaBootstrapServers,
    deserializers: stringDeserializers
  })

  const stream = await consumer.consume({
    topics: [topic],
    mode: 'earliest',
    maxWaitTime: 500,
    isolationLevel: FetchIsolationLevels.READ_COMMITTED
  })

  const values: string[] = []

  try {
    for await (const message of stream) {
      values.push(message.value as unknown as string)

      if (values.length === expected) {
        break
      }
    }
  } finally {
    await stream.close()
  }

  return values
}

// Every transactional API is pinned in turn: they all take part in the same flow, so sweeping one
// at a time keeps the failure attributable to a single codec.
const TRANSACTIONAL_APIS = ['InitProducerId', 'AddPartitionsToTxn', 'AddOffsetsToTxn', 'EndTxn', 'TxnOffsetCommit']

test('A transaction commits at every version of every transactional API', async t => {
  const probe = createProducer(t, { bootstrapBrokers: kafkaBootstrapServers })

  for (const name of TRANSACTIONAL_APIS) {
    const versions = await usableVersions(probe, name)

    if (!versions.length) {
      t.diagnostic(`${name}: no usable version on this broker`)
      continue
    }

    for (const version of versions) {
      await t.test(`${name} v${version}`, async t => {
        await runAtVersion(t, `${name} v${version}`, async () => {
          const topic = await createTopic(t, 1, kafkaBootstrapServers)
          const producer = await createTransactionalProducer(t, { [name]: version })

          const transaction = await producer.beginTransaction()
          ok(producer.producerId! >= 0n, `${name} v${version} produced no producer id`)
          ok(producer.producerEpoch! >= 0, `${name} v${version} produced no producer epoch`)

          await transaction.send({
            messages: [{ topic, partition: 0, key: 'k', value: `committed-${name}-${version}` }],
            acks: ProduceAcks.ALL
          })

          // AddOffsetsToTxn and TxnOffsetCommit only run when a consumer group takes part in the
          // transaction, so enrol one: commit() then drives the offset commit through the coordinator.
          const consumer = createConsumer(t, {
            bootstrapBrokers: kafkaBootstrapServers,
            deserializers: stringDeserializers,
            autocommit: false
          })
          await consumer.joinGroup({})
          await transaction.addConsumer(consumer)

          await transaction.commit()

          const values = await readCommitted(t, topic, 1)
          strictEqual(values[0], `committed-${name}-${version}`, `${name} v${version} did not commit the record`)
        })
      })
    }
  }
})

test('A transaction aborts at every version of every transactional API', async t => {
  const probe = createProducer(t, { bootstrapBrokers: kafkaBootstrapServers })

  for (const name of ['InitProducerId', 'AddPartitionsToTxn', 'EndTxn']) {
    const versions = await usableVersions(probe, name)

    for (const version of versions) {
      await t.test(`${name} v${version}`, async t => {
        await runAtVersion(t, `${name} v${version}`, async () => {
          const topic = await createTopic(t, 1, kafkaBootstrapServers)
          const producer = await createTransactionalProducer(t, { [name]: version })

          const aborted = await producer.beginTransaction()
          await aborted.send({
            messages: [{ topic, partition: 0, key: 'k', value: `aborted-${name}-${version}` }],
            acks: ProduceAcks.ALL
          })
          await aborted.abort()

          // A read committed consumer must not see the aborted record, only the committed one which
          // follows it. Writing a second transaction gives the reader something to stop on.
          const committed = await producer.beginTransaction()
          await committed.send({
            messages: [{ topic, partition: 0, key: 'k', value: `committed-${name}-${version}` }],
            acks: ProduceAcks.ALL
          })
          await committed.commit()

          const values = await readCommitted(t, topic, 1)
          strictEqual(
            values[0],
          `committed-${name}-${version}`,
          `${name} v${version} leaked an aborted record to a read committed consumer`
          )
        })
      })
    }
  }
})
