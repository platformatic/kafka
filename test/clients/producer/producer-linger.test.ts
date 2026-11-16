import { deepStrictEqual, ok, rejects, strictEqual } from 'node:assert'
import { test, type TestContext } from 'node:test'
import {
  type ConnectionDiagnosticEvent,
  connectionsApiChannel,
  MessagesStreamModes,
  ProduceAcks,
  Producer,
  type ProduceResult,
  stringDeserializers,
  stringSerializers,
  type TopicWithPartitionAndOffset,
  UserError
} from '../../../src/index.ts'
import { createConsumer, createProducer, createTestGroupId, createTopic, kafkaBootstrapServers } from '../../helpers.ts'

const produceApiKey = 0

// Counts the Produce requests which are written to the brokers from now on and do not fail.
// The ones which fail are not counted since a broker can reject the first requests for a topic
// which was just created, for instance while it is still becoming the leader, and the client retries.
function countProduceRequests (t: TestContext): () => number {
  let count = 0

  function onStart (context: ConnectionDiagnosticEvent): void {
    if (context.apiKey === produceApiKey) {
      count++
    }
  }

  function onError (context: ConnectionDiagnosticEvent): void {
    if (context.apiKey === produceApiKey) {
      count--
    }
  }

  connectionsApiChannel.start.subscribe(onStart as (message: unknown) => void)
  connectionsApiChannel.error.subscribe(onError as (message: unknown) => void)

  t.after(() => {
    connectionsApiChannel.start.unsubscribe(onStart as (message: unknown) => void)
    connectionsApiChannel.error.unsubscribe(onError as (message: unknown) => void)
  })

  return () => count
}

function sortOffsets (offsets: TopicWithPartitionAndOffset[] | undefined): TopicWithPartitionAndOffset[] {
  return [...(offsets ?? [])].sort((a, b) => a.partition - b.partition)
}

test('linger and batchMaxBytes should be validated', () => {
  for (const options of [{ linger: -1 }, { linger: 'fast' }, { batchMaxBytes: 0 }, { batchMaxBytes: 1.5 }]) {
    try {
      // eslint-disable-next-line no-new
      new Producer({ clientId: 'test', bootstrapBrokers: kafkaBootstrapServers, strict: true, ...(options as object) })
      throw new Error('Expected the constructor to throw.')
    } catch (error) {
      ok(error instanceof UserError, `${JSON.stringify(options)} should be rejected`)
    }
  }
})

test('linger and batchMaxBytes should be accepted', async t => {
  const producer = new Producer({
    clientId: 'test',
    bootstrapBrokers: kafkaBootstrapServers,
    strict: true,
    linger: 5,
    batchMaxBytes: 1024
  })
  t.after(() => producer.close())

  ok(producer instanceof Producer)
})

test('send should produce on its own when linger is not set', async t => {
  const producer = createProducer(t, { serializers: stringSerializers })
  const topic = await createTopic(t, true)
  await producer.metadata({ topics: [topic] })

  const produceRequests = countProduceRequests(t)

  const results = await Promise.all([
    producer.send({ messages: [{ topic, value: 'a' }] }),
    producer.send({ messages: [{ topic, value: 'b' }] }),
    producer.send({ messages: [{ topic, value: 'c' }] })
  ])

  strictEqual(produceRequests(), 3)
  deepStrictEqual(results.map(result => result.offsets![0].offset).sort(), [0n, 1n, 2n])
})

test('send should merge the calls made within the linger time in a single request', async t => {
  const producer = createProducer(t, { serializers: stringSerializers, linger: 50 })
  const topic = await createTopic(t, true)
  await producer.metadata({ topics: [topic] })

  const produceRequests = countProduceRequests(t)

  const sends: Promise<ProduceResult>[] = []
  for (let i = 0; i < 20; i++) {
    sends.push(producer.send({ messages: [{ topic, key: `key-${i}`, value: `value-${i}` }] }))
  }

  const results = await Promise.all(sends)

  strictEqual(produceRequests(), 1)

  // Each call gets the offset of its own message, in the order the calls were made
  for (let i = 0; i < 20; i++) {
    deepStrictEqual(results[i], { offsets: [{ topic, partition: 0, offset: BigInt(i) }] })
  }

  // And those are the offsets the messages really have
  const consumer = createConsumer(t, { groupId: createTestGroupId(), deserializers: stringDeserializers })
  const stream = await consumer.consume({ topics: [topic], mode: MessagesStreamModes.EARLIEST, maxFetches: 1 })

  const consumed: [bigint, string][] = []
  for await (const message of stream) {
    consumed.push([message.offset, message.value])
  }

  deepStrictEqual(
    consumed,
    Array.from({ length: 20 }, (_, i) => [BigInt(i), `value-${i}`])
  )
})

test('send should give each call the offset of its first message in each partition', async t => {
  const producer = createProducer(t, { serializers: stringSerializers, linger: 50 })
  const topic = await createTopic(t, true, 3)
  await producer.metadata({ topics: [topic] })

  // Occupy the first offsets, so that the ones which follow are not just positions
  await producer.send({
    messages: [
      { topic, partition: 0, value: 'x' },
      { topic, partition: 0, value: 'x' },
      { topic, partition: 2, value: 'x' }
    ]
  })

  const [first, second, third] = await Promise.all([
    producer.send({
      messages: [
        { topic, partition: 0, value: 'a1' },
        { topic, partition: 1, value: 'a2' },
        { topic, partition: 0, value: 'a3' }
      ]
    }),
    producer.send({
      messages: [
        { topic, partition: 1, value: 'b1' },
        { topic, partition: 2, value: 'b2' }
      ]
    }),
    producer.send({ messages: [{ topic, partition: 0, value: 'c1' }] })
  ])

  deepStrictEqual(sortOffsets(first.offsets), [
    { topic, partition: 0, offset: 2n },
    { topic, partition: 1, offset: 0n }
  ])
  deepStrictEqual(sortOffsets(second.offsets), [
    { topic, partition: 1, offset: 1n },
    { topic, partition: 2, offset: 1n }
  ])
  deepStrictEqual(sortOffsets(third.offsets), [{ topic, partition: 0, offset: 4n }])
})

test('send should not wait for the linger time once batchMaxBytes are waiting', async t => {
  const producer = createProducer(t, { serializers: stringSerializers, linger: 60_000, batchMaxBytes: 100 })
  const topic = await createTopic(t, true)
  await producer.metadata({ topics: [topic] })

  const produceRequests = countProduceRequests(t)

  // Neither of the first two calls reaches the limit, the third one does
  const results = await Promise.all([
    producer.send({ messages: [{ topic, value: 'a'.repeat(40) }] }),
    producer.send({ messages: [{ topic, value: 'b'.repeat(40) }] }),
    producer.send({ messages: [{ topic, value: 'c'.repeat(40) }] })
  ])

  strictEqual(produceRequests(), 1)
  deepStrictEqual(
    results.map(result => result.offsets![0].offset),
    [0n, 1n, 2n]
  )
})

test('send should not merge calls made with different settings', async t => {
  const producer = createProducer(t, { serializers: stringSerializers, linger: 50 })
  const topic = await createTopic(t, true)
  await producer.metadata({ topics: [topic] })

  const produceRequests = countProduceRequests(t)

  const results = await Promise.all([
    producer.send({ messages: [{ topic, value: 'a' }], acks: ProduceAcks.LEADER }),
    producer.send({ messages: [{ topic, value: 'b' }], acks: ProduceAcks.LEADER }),
    producer.send({ messages: [{ topic, value: 'c' }], acks: ProduceAcks.ALL }),
    producer.send({ messages: [{ topic, value: 'd' }], acks: ProduceAcks.ALL, compression: 'gzip' })
  ])

  strictEqual(produceRequests(), 3)
  strictEqual(results[0].offsets![0].offset, 0n)
  strictEqual(results[1].offsets![0].offset, 1n)
  deepStrictEqual(results.map(result => result.offsets![0].offset).sort(), [0n, 1n, 2n, 3n])
})

test('send should report unwritable nodes to every merged call when no response is requested', async t => {
  const producer = createProducer(t, { serializers: stringSerializers, linger: 50 })
  const topic = await createTopic(t, true)
  await producer.metadata({ topics: [topic] })

  const produceRequests = countProduceRequests(t)

  const results = await Promise.all([
    producer.send({ messages: [{ topic, value: 'a' }], acks: ProduceAcks.NO_RESPONSE }),
    producer.send({ messages: [{ topic, value: 'b' }], acks: ProduceAcks.NO_RESPONSE })
  ])

  strictEqual(produceRequests(), 1)
  deepStrictEqual(results, [{ unwritableNodes: [] }, { unwritableNodes: [] }])
})

test('send should fail every merged call when producing fails', async t => {
  const producer = createProducer(t, { serializers: stringSerializers, linger: 50, retries: 0 })
  const topic = await createTopic(t, false)

  const results = await Promise.allSettled([
    producer.send({ messages: [{ topic, value: 'a' }] }),
    producer.send({ messages: [{ topic, value: 'b' }] })
  ])

  strictEqual(results[0].status, 'rejected')
  strictEqual(results[1].status, 'rejected')
  strictEqual((results[0] as PromiseRejectedResult).reason, (results[1] as PromiseRejectedResult).reason)
})

test('send should work with an idempotent producer when lingering', async t => {
  const producer = createProducer(t, { serializers: stringSerializers, linger: 20, idempotent: true })
  const topic = await createTopic(t, true)
  await producer.metadata({ topics: [topic] })

  const offsets: bigint[] = []

  // Several rounds, so that the sequence numbers of a merged request are continued by the next one
  for (let round = 0; round < 3; round++) {
    const results = await Promise.all(
      Array.from({ length: 5 }, (_, i) => producer.send({ messages: [{ topic, value: `value-${round}-${i}` }] }))
    )

    for (const result of results) {
      offsets.push(result.offsets![0].offset)
    }
  }

  deepStrictEqual(
    offsets,
    Array.from({ length: 15 }, (_, i) => BigInt(i))
  )
})

test('close should send the messages which are lingering', async t => {
  const producer = createProducer(t, { serializers: stringSerializers, linger: 60_000 })
  const topic = await createTopic(t, true)
  await producer.metadata({ topics: [topic] })

  const sends = Promise.all([
    producer.send({ messages: [{ topic, value: 'a' }] }),
    producer.send({ messages: [{ topic, value: 'b' }] })
  ])

  await producer.close()

  const results = await sends
  deepStrictEqual(
    results.map(result => result.offsets![0].offset),
    [0n, 1n]
  )

  await rejects(() => producer.send({ messages: [{ topic, value: 'c' }] }))
})
