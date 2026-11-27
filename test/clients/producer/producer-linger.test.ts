import { deepStrictEqual, ok, rejects, strictEqual } from 'node:assert'
import { test, type TestContext } from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import type { Callback } from '../../../src/apis/definitions.ts'
import type { ProduceResponse } from '../../../src/apis/producer/produce-v11.ts'
import { kConnections } from '../../../src/clients/base/base.ts'
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
import {
  createConsumer,
  createProducer,
  createTestGroupId,
  createTopic,
  kafkaBootstrapServers,
  mockAPI
} from '../../helpers.ts'

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

  // Together the two calls reach the limit exactly
  const results = await Promise.all([
    producer.send({ messages: [{ topic, value: 'a'.repeat(50) }] }),
    producer.send({ messages: [{ topic, value: 'b'.repeat(50) }] })
  ])

  strictEqual(produceRequests(), 1)
  deepStrictEqual(
    results.map(result => result.offsets![0].offset),
    [0n, 1n]
  )
})

test('send should send what is waiting before a call which would exceed batchMaxBytes', async t => {
  const producer = createProducer(t, { serializers: stringSerializers, linger: 60_000, batchMaxBytes: 100 })
  const topic = await createTopic(t, true)
  await producer.metadata({ topics: [topic] })

  const produceRequests = countProduceRequests(t)

  // The first two calls wait. The third would take them over the limit, so they are sent first,
  // then the third reaches the limit on its own and is sent without waiting either.
  const results = await Promise.all([
    producer.send({ messages: [{ topic, value: 'a'.repeat(40) }] }),
    producer.send({ messages: [{ topic, value: 'b'.repeat(40) }] }),
    producer.send({ messages: [{ topic, value: 'c'.repeat(100) }] })
  ])

  strictEqual(produceRequests(), 2)
  deepStrictEqual(
    results.map(result => result.offsets![0].offset),
    [0n, 1n, 2n]
  )
})

test('send should let the calls made while a request is in flight wait for it', async t => {
  const producer = createProducer(t, { serializers: stringSerializers, linger: 60_000, batchMaxBytes: 100 })
  const topic = await createTopic(t, true)
  await producer.metadata({ topics: [topic] })

  const produceRequests = countProduceRequests(t)

  // The first call reaches the limit on its own and is sent. Each of the others reaches it too, but they
  // are made while the first request is in flight: they are all sent together once it completes.
  const results = await Promise.all(
    ['a', 'b', 'c', 'd', 'e', 'f'].map(letter => producer.send({ messages: [{ topic, value: letter.repeat(100) }] }))
  )

  strictEqual(produceRequests(), 2)
  deepStrictEqual(
    results.map(result => result.offsets![0].offset),
    [0n, 1n, 2n, 3n, 4n, 5n]
  )
})

test('send should limit what waits for a request in flight', async t => {
  const producer = createProducer(t, { serializers: stringSerializers, linger: 60_000, batchMaxBytes: 100 })
  const topic = await createTopic(t, true)
  await producer.metadata({ topics: [topic] })

  /*
    The Produce requests are answered by the test, which decides when each of them completes.
    Nothing is sent to the brokers: other tests set quotas on them while this one runs, and producing this
    many bytes for real would get the connection throttled.
  */
  const requests: Callback<ProduceResponse>[] = []
  mockAPI(producer[kConnections], produceApiKey, null, null, (_original: unknown, ...args: unknown[]) => {
    requests.push(args.at(-1) as Callback<ProduceResponse>)
    return true
  })

  function respond (index: number): void {
    requests[index](null, {
      responses: [
        { name: topic, partitionResponses: [{ index: 0, errorCode: 0, baseOffset: BigInt(index) }] }
      ]
    } as unknown as ProduceResponse)
  }

  const completed: string[] = []
  function send (letter: string, length: number): Promise<void> {
    return producer.send({ messages: [{ topic, value: letter.repeat(length) }] }).then(() => {
      completed.push(letter)
    })
  }

  // The first call reaches batchMaxBytes on its own and is sent at once
  const sends = [send('a', 100)]
  await sleep(10)
  strictEqual(requests.length, 1)

  // The second and the third wait for it, as together they are below 256 KiB
  sends.push(send('b', 100_000), send('c', 100_000))
  await sleep(10)
  strictEqual(requests.length, 1)

  // The fourth would take what is waiting over 256 KiB, so the second and the third are sent
  sends.push(send('d', 100_000))
  await sleep(10)
  strictEqual(requests.length, 2)

  // The fourth is only sent once nothing is in flight anymore
  respond(0)
  await sleep(10)
  strictEqual(requests.length, 2)
  deepStrictEqual(completed, ['a'])

  respond(1)
  await sleep(10)
  strictEqual(requests.length, 3)
  deepStrictEqual(completed, ['a', 'b', 'c'])

  respond(2)
  await Promise.all(sends)
  deepStrictEqual(completed, ['a', 'b', 'c', 'd'])
})

test('send should fail the calls which were waiting for a request in flight on their own', async t => {
  const producer = createProducer(t, { serializers: stringSerializers, linger: 60_000, batchMaxBytes: 100 })
  const topic = await createTopic(t, true)
  await producer.metadata({ topics: [topic] })

  // The second request is for a topic which does not exist and cannot be created
  const results = await Promise.allSettled([
    producer.send({ messages: [{ topic, value: 'a'.repeat(100) }] }),
    producer.send({ messages: [{ topic: 'invalid topic name', value: 'b'.repeat(100) }], autocreateTopics: false }),
    producer.send({ messages: [{ topic, value: 'c'.repeat(100) }] })
  ])

  deepStrictEqual(
    results.map(result => result.status),
    ['fulfilled', 'rejected', 'fulfilled']
  )
})

test('send should accept null values and null header values when lingering', async t => {
  const producer = createProducer(t, { linger: 10 })
  const topic = await createTopic(t, true)
  await producer.metadata({ topics: [topic] })

  const results = await Promise.all([
    producer.send({ messages: [{ topic, key: Buffer.from('k'), value: null as unknown as Buffer }] }),
    producer.send({
      messages: [{ topic, value: Buffer.from('v'), headers: new Map([[Buffer.from('h'), null as unknown as Buffer]]) }]
    })
  ])

  deepStrictEqual(
    results.map(result => result.offsets![0].offset),
    [0n, 1n]
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
  // The first request can be rejected and retried while the broker is still picking up the new topic,
  // so another one can be written before it. Only the merged calls are known to be consecutive.
  strictEqual(results[1].offsets![0].offset, results[0].offsets![0].offset + 1n)
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

test('autoBatching should be validated', () => {
  try {
    // eslint-disable-next-line no-new
    new Producer({
      clientId: 'test',
      bootstrapBrokers: kafkaBootstrapServers,
      strict: true,
      autoBatching: 'yes' as unknown as boolean
    })
    throw new Error('Expected the constructor to throw.')
  } catch (error) {
    ok(error instanceof UserError)
  }
})

test('send should merge the calls made in the same tick when autoBatching is set', async t => {
  const producer = createProducer(t, { serializers: stringSerializers, autoBatching: true })
  const topic = await createTopic(t, true)
  await producer.metadata({ topics: [topic] })

  const produceRequests = countProduceRequests(t)

  const sends: Promise<ProduceResult>[] = []
  for (let i = 0; i < 20; i++) {
    sends.push(producer.send({ messages: [{ topic, key: `key-${i}`, value: `value-${i}` }] }))
  }

  // Calls made from promise callbacks which are already due are still in the same tick
  await Promise.resolve()
  sends.push(producer.send({ messages: [{ topic, key: 'key-20', value: 'value-20' }] }))
  await Promise.resolve()
  sends.push(producer.send({ messages: [{ topic, key: 'key-21', value: 'value-21' }] }))

  const results = await Promise.all(sends)

  strictEqual(produceRequests(), 1)

  // Each call gets the offset of its own message, in the order the calls were made
  for (let i = 0; i < 22; i++) {
    deepStrictEqual(results[i], { offsets: [{ topic, partition: 0, offset: BigInt(i) }] })
  }
})

test('send should not merge the calls made in different ticks when autoBatching is set', async t => {
  const producer = createProducer(t, { serializers: stringSerializers, autoBatching: true })
  const topic = await createTopic(t, true)
  await producer.metadata({ topics: [topic] })

  const produceRequests = countProduceRequests(t)

  const first = producer.send({ messages: [{ topic, value: 'a' }] })
  await new Promise<void>(resolve => process.nextTick(resolve))
  const second = producer.send({ messages: [{ topic, value: 'b' }] })

  const results = await Promise.all([first, second])

  strictEqual(produceRequests(), 2)
  deepStrictEqual(
    results.map(result => result.offsets![0].offset),
    [0n, 1n]
  )
})

test('send should not wait for the end of the tick once batchMaxBytes are waiting when autoBatching is set', async t => {
  const producer = createProducer(t, { serializers: stringSerializers, autoBatching: true, batchMaxBytes: 100 })
  const topic = await createTopic(t, true)
  await producer.metadata({ topics: [topic] })

  const produceRequests = countProduceRequests(t)

  // The first two calls reach the limit exactly, so the third one is produced on its own
  const results = await Promise.all([
    producer.send({ messages: [{ topic, value: 'a'.repeat(50) }] }),
    producer.send({ messages: [{ topic, value: 'b'.repeat(50) }] }),
    producer.send({ messages: [{ topic, value: 'c'.repeat(50) }] })
  ])

  strictEqual(produceRequests(), 2)
  deepStrictEqual(
    results.map(result => result.offsets![0].offset).sort(),
    [0n, 1n, 2n]
  )
})

test('send should wait for the linger time when both linger and autoBatching are set', async t => {
  const producer = createProducer(t, { serializers: stringSerializers, autoBatching: true, linger: 50 })
  const topic = await createTopic(t, true)
  await producer.metadata({ topics: [topic] })

  const produceRequests = countProduceRequests(t)

  const first = producer.send({ messages: [{ topic, value: 'a' }] })
  await sleep(5)
  const second = producer.send({ messages: [{ topic, value: 'b' }] })

  const results = await Promise.all([first, second])

  strictEqual(produceRequests(), 1)
  deepStrictEqual(
    results.map(result => result.offsets![0].offset),
    [0n, 1n]
  )
})

test('send should keep the order of the calls with autoBatching and orderedSends', async t => {
  const producer = createProducer(t, { serializers: stringSerializers, autoBatching: true, orderedSends: true })
  const topic = await createTopic(t, true)
  await producer.metadata({ topics: [topic] })

  const sends: Promise<ProduceResult>[] = []
  for (let round = 0; round < 5; round++) {
    for (let i = 0; i < 10; i++) {
      sends.push(producer.send({ messages: [{ topic, value: `value-${round * 10 + i}` }] }))
    }

    await new Promise<void>(resolve => setImmediate(resolve))
  }

  const results = await Promise.all(sends)

  for (let i = 0; i < 50; i++) {
    deepStrictEqual(results[i], { offsets: [{ topic, partition: 0, offset: BigInt(i) }] })
  }
})

test('close should send the messages which are waiting when autoBatching is set', async t => {
  const producer = createProducer(t, { serializers: stringSerializers, autoBatching: true })
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
})

test('send should send what is waiting before a call which would exceed batchMaxBytes when autoBatching is set', async t => {
  const producer = createProducer(t, { serializers: stringSerializers, autoBatching: true, batchMaxBytes: 100 })
  const topic = await createTopic(t, true)
  await producer.metadata({ topics: [topic] })

  const produceRequests = countProduceRequests(t)

  // The second call does not fit with the first one, while the third one fits with the second
  const results = await Promise.all([
    producer.send({ messages: [{ topic, value: 'a'.repeat(60) }] }),
    producer.send({ messages: [{ topic, value: 'b'.repeat(60) }] }),
    producer.send({ messages: [{ topic, value: 'c'.repeat(30) }] })
  ])

  strictEqual(produceRequests(), 2)
  deepStrictEqual(results.map(result => result.offsets![0].offset).sort(), [0n, 1n, 2n])
})

test('send should merge the calls made in different ticks while a request is in flight when autoBatching is set', async t => {
  const producer = createProducer(t, { serializers: stringSerializers, autoBatching: true })
  const topic = await createTopic(t, true)
  await producer.metadata({ topics: [topic] })

  const produceRequests = countProduceRequests(t)

  // Once the tick ends the first call is in flight
  const first = producer.send({ messages: [{ topic, value: 'a' }] })
  await new Promise<void>(resolve => process.nextTick(resolve))

  // These two are made in different ticks of the same iteration of the event loop
  const second = producer.send({ messages: [{ topic, value: 'b' }] })
  await new Promise<void>(resolve => process.nextTick(resolve))
  const third = producer.send({ messages: [{ topic, value: 'c' }] })

  const results = await Promise.all([first, second, third])

  strictEqual(produceRequests(), 2)
  deepStrictEqual(
    results.map(result => result.offsets![0].offset),
    [0n, 1n, 2n]
  )
})

test('close should send the messages which are waiting for a request in flight when autoBatching is set', async t => {
  const producer = createProducer(t, { serializers: stringSerializers, autoBatching: true })
  const topic = await createTopic(t, true)
  await producer.metadata({ topics: [topic] })

  const first = producer.send({ messages: [{ topic, value: 'a' }] })
  await new Promise<void>(resolve => process.nextTick(resolve))
  const second = producer.send({ messages: [{ topic, value: 'b' }] })

  await producer.close()

  const results = await Promise.all([first, second])
  deepStrictEqual(results.map(result => result.offsets![0].offset).sort(), [0n, 1n])
})
