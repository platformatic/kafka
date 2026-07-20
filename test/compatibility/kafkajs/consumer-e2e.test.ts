import { deepStrictEqual, equal, ok } from 'node:assert'
import { randomUUID } from 'node:crypto'
import { setTimeout as sleep } from 'node:timers/promises'
import { test } from 'node:test'
import { Kafka } from '../../../src/compatibility/kafkajs/index.ts'
import { KafkaJSMessagesStream } from '../../../src/compatibility/kafkajs/messages-stream.ts'
import { createTopic, executeWithTimeout, kafkaBootstrapServers } from '../../helpers.ts'

function createKafka () {
  return new Kafka({ brokers: kafkaBootstrapServers, clientId: `consumer-e2e-${randomUUID()}` })
}

test('eachBatch resolves, reports and explicitly commits offsets', async t => {
  const topic = await createTopic(t, true)
  const groupId = `consumer-batch-${randomUUID()}`
  const kafka = createKafka()
  const producer = kafka.producer()
  const consumer = kafka.consumer({ groupId })
  const requests: Array<Record<string, unknown>> = []
  consumer.on(consumer.events.REQUEST, event => requests.push(event.payload as Record<string, unknown>))
  t.after(async () => Promise.allSettled([producer.disconnect(), consumer.disconnect()]))

  await producer.connect()
  await producer.send({ topic, messages: ['zero', 'one', 'two'].map(value => ({ value })) })
  const processed = Promise.withResolvers<void>()
  let batchSize = 0
  await consumer.subscribe({ topic, fromBeginning: true })
  await consumer.run({
    autoCommit: false,
    eachBatchAutoResolve: false,
    eachBatch: async ({ batch, resolveOffset, uncommittedOffsets, commitOffsetsIfNecessary }) => {
      batchSize = Math.max(batchSize, batch.messages.length)
      equal(batch.highWatermark, '3')
      equal(batch.offsetLag(), '0')
      equal(batch.offsetLagLow(), '2')
      resolveOffset(batch.messages[1].offset)
      deepStrictEqual(uncommittedOffsets(), {
        topics: [{ topic, partitions: [{ partition: 0, offset: '2' }] }]
      })
      await commitOffsetsIfNecessary({ topics: [{ topic, partitions: [{ partition: 0, offset: '2' }] }] })
      processed.resolve()
    }
  })
  await executeWithTimeout(processed.promise, 10_000)
  ok((consumer as unknown as { stream: unknown }).stream instanceof KafkaJSMessagesStream)
  await consumer.stop()
  ok(batchSize > 1)
  ok(requests.length > 0)
  for (const field of ['apiKey', 'apiName', 'apiVersion', 'broker', 'clientId', 'correlationId', 'createdAt', 'sentAt', 'size']) {
    ok(requests[0][field] !== undefined, `missing request field ${field}`)
  }

  const next = kafka.consumer({ groupId })
  t.after(() => next.disconnect())
  const offset = Promise.withResolvers<string>()
  await next.subscribe({ topic, fromBeginning: true })
  await next.run({ eachMessage: async ({ message }) => offset.resolve(message.offset) })
  equal(await executeWithTimeout(offset.promise, 10_000), '2')
  await next.stop()
})

test('preserves repeated header names', async t => {
  const topic = await createTopic(t, true)
  const kafka = createKafka()
  const producer = kafka.producer()
  const consumer = kafka.consumer({ groupId: `consumer-headers-${randomUUID()}` })
  t.after(async () => Promise.allSettled([producer.disconnect(), consumer.disconnect()]))

  await producer.connect()
  await producer.send({
    topic,
    messages: [{ value: 'value', headers: { repeated: ['first', Buffer.from('second')], single: 'only' } }]
  })
  const received = Promise.withResolvers<Record<string, Buffer | Buffer[] | undefined>>()
  await consumer.subscribe({ topic, fromBeginning: true })
  await consumer.run({ eachMessage: async ({ message }) => received.resolve(message.headers as never) })
  const headers = await executeWithTimeout(received.promise, 10_000)
  if (typeof headers === 'string') {
    throw new Error('Timed out waiting for headers')
  }
  deepStrictEqual((headers.repeated as Buffer[]).map(value => value.toString()), ['first', 'second'])
  equal((headers.single as Buffer).toString(), 'only')
  await consumer.stop()
})

test('preserves explicit offset commit metadata', async t => {
  const topic = await createTopic(t, true)
  const groupId = `consumer-metadata-${randomUUID()}`
  const kafka = createKafka()
  const producer = kafka.producer()
  const consumer = kafka.consumer({ groupId })
  const admin = kafka.admin()
  t.after(async () => Promise.allSettled([producer.disconnect(), consumer.disconnect(), admin.disconnect()]))

  await Promise.all([producer.connect(), admin.connect()])
  await producer.send({ topic, messages: [{ value: 'value' }] })
  const committed = Promise.withResolvers<void>()
  await consumer.subscribe({ topic, fromBeginning: true })
  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ message }) => {
      await consumer.commitOffsets([{ topic, partition: 0, offset: '1', metadata: 'checkpoint' }])
      equal(message.offset, '0')
      committed.resolve()
    }
  })
  await executeWithTimeout(committed.promise, 10_000)
  await consumer.stop()
  deepStrictEqual(await admin.fetchOffsets({ groupId, topics: [topic] }), [
    { topic, partitions: [{ partition: 0, offset: '1', metadata: 'checkpoint' }] }
  ])
})

test('recovers an out-of-range offset from the topic beginning', async t => {
  const topic = await createTopic(t, true)
  const groupId = `consumer-out-of-range-${randomUUID()}`
  const kafka = createKafka()
  const producer = kafka.producer()
  const consumer = kafka.consumer({ groupId })
  const admin = kafka.admin()
  t.after(async () => Promise.allSettled([producer.disconnect(), consumer.disconnect(), admin.disconnect()]))

  await Promise.all([producer.connect(), admin.connect()])
  await producer.send({ topic, messages: ['zero', 'one', 'two'].map(value => ({ value })) })
  await admin.setOffsets({ groupId, topic, partitions: [{ partition: 0, offset: '0' }] })
  await admin.deleteTopicRecords({ topic, partitions: [{ partition: 0, offset: '2' }] })

  const received = Promise.withResolvers<string>()
  await consumer.subscribe({ topic, fromBeginning: true })
  await consumer.run({ eachMessage: async ({ message }) => received.resolve(message.value!.toString()) })
  equal(await executeWithTimeout(received.promise, 10_000), 'two')
  await consumer.stop()
})

test('seek suppresses buffered messages and resumes at the requested offset', async t => {
  const topic = await createTopic(t, true)
  const kafka = createKafka()
  const producer = kafka.producer()
  const consumer = kafka.consumer({ groupId: `consumer-seek-${randomUUID()}` })
  t.after(async () => Promise.allSettled([producer.disconnect(), consumer.disconnect()]))
  await producer.connect()
  await producer.send({ topic, messages: ['zero', 'one', 'two', 'three'].map(value => ({ value })) })

  const values: string[] = []
  const completed = Promise.withResolvers<void>()
  await consumer.subscribe({ topic, fromBeginning: true })
  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ message }) => {
      values.push(message.value!.toString())
      if (message.offset === '0') {
        consumer.seek({ topic, partition: 0, offset: '2' })
      } else {
        completed.resolve()
      }
    }
  })
  await executeWithTimeout(completed.promise, 10_000)
  await consumer.stop()
  deepStrictEqual(values.slice(0, 2), ['zero', 'two'])
  equal(values.includes('one'), false)
})

test('partition pause is selective and resume retains the next message', async t => {
  const topic = await createTopic(t, true, 2)
  const kafka = createKafka()
  const producer = kafka.producer()
  const consumer = kafka.consumer({ groupId: `consumer-pause-${randomUUID()}` })
  t.after(async () => Promise.allSettled([producer.disconnect(), consumer.disconnect()]))
  await producer.connect()
  await producer.send({
    topic,
    messages: [
      { partition: 0, value: 'paused-0' },
      { partition: 0, value: 'paused-1' },
      { partition: 1, value: 'active-0' }
    ]
  })

  const values: string[] = []
  const active = Promise.withResolvers<void>()
  const pauseApplied = Promise.withResolvers<void>()
  const resumed = Promise.withResolvers<void>()
  await consumer.subscribe({ topic, fromBeginning: true })
  await consumer.run({
    partitionsConsumedConcurrently: 2,
    eachMessage: async ({ partition, message, pause }) => {
      const value = message.value!.toString()
      values.push(value)
      if (partition === 0 && message.offset === '0') {
        pause()
        pauseApplied.resolve()
      }
      if (value === 'active-0') {
        active.resolve()
      }
      if (value === 'paused-1') {
        resumed.resolve()
      }
    }
  })
  await Promise.all([executeWithTimeout(active.promise, 10_000), executeWithTimeout(pauseApplied.promise, 10_000)])
  equal(values.includes('paused-1'), false)
  deepStrictEqual(consumer.paused(), [{ topic, partitions: [0] }])
  consumer.resume([{ topic, partitions: [0] }])
  await executeWithTimeout(resumed.promise, 10_000)
  await consumer.stop()
})

test('processes partitions concurrently while preserving partition order', async t => {
  const topic = await createTopic(t, true, 2)
  const kafka = createKafka()
  const producer = kafka.producer()
  const consumer = kafka.consumer({ groupId: `consumer-concurrency-${randomUUID()}` })
  t.after(async () => Promise.allSettled([producer.disconnect(), consumer.disconnect()]))
  await producer.connect()
  await producer.send({
    topic,
    messages: [
      { partition: 0, value: '0-0' },
      { partition: 0, value: '0-1' },
      { partition: 1, value: '1-0' },
      { partition: 1, value: '1-1' }
    ]
  })

  let active = 0
  let maximum = 0
  const byPartition: string[][] = [[], []]
  const completed = Promise.withResolvers<void>()
  await consumer.subscribe({ topic, fromBeginning: true })
  await consumer.run({
    partitionsConsumedConcurrently: 2,
    eachMessage: async ({ partition, message }) => {
      active++
      maximum = Math.max(maximum, active)
      byPartition[partition].push(message.value!.toString())
      await sleep(30)
      active--
      if (byPartition[0].length === 2 && byPartition[1].length === 2) {
        completed.resolve()
      }
    }
  })
  await executeWithTimeout(completed.promise, 10_000)
  await consumer.stop()
  equal(maximum, 2)
  deepStrictEqual(byPartition, [
    ['0-0', '0-1'],
    ['1-0', '1-1']
  ])
})

test('stop leaves the group for immediate same-group handoff', async t => {
  const topic = await createTopic(t, true)
  const groupId = `consumer-handoff-${randomUUID()}`
  const kafka = createKafka()
  const producer = kafka.producer()
  const first = kafka.consumer({ groupId })
  const second = kafka.consumer({ groupId })
  t.after(async () => Promise.allSettled([producer.disconnect(), first.disconnect(), second.disconnect()]))

  await producer.connect()
  await producer.send({ topic, messages: [{ value: 'first' }] })
  const firstReceived = Promise.withResolvers<void>()
  await first.subscribe({ topic, fromBeginning: true })
  await first.run({ eachMessage: async () => firstReceived.resolve() })
  await executeWithTimeout(firstReceived.promise, 10_000)
  await first.stop()

  await producer.send({ topic, messages: [{ value: 'second' }] })
  const handedOff = Promise.withResolvers<string>()
  await second.subscribe({ topic, fromBeginning: true })
  await second.run({ eachMessage: async ({ message }) => handedOff.resolve(message.value!.toString()) })
  equal(await executeWithTimeout(handedOff.promise, 10_000), 'second')
  await second.stop()
})

test('reconnects the same consumer instance after disconnect', async t => {
  const topic = await createTopic(t, true)
  const kafka = createKafka()
  const producer = kafka.producer()
  const consumer = kafka.consumer({ groupId: `consumer-reconnect-${randomUUID()}` })
  t.after(async () => Promise.allSettled([producer.disconnect(), consumer.disconnect()]))

  await Promise.all([producer.connect(), consumer.connect()])
  await producer.send({ topic, messages: [{ value: 'first' }] })
  const first = Promise.withResolvers<void>()
  const committed = Promise.withResolvers<void>()
  consumer.on(consumer.events.COMMIT_OFFSETS, () => committed.resolve())
  await consumer.subscribe({ topic, fromBeginning: true })
  await consumer.run({ eachMessage: async () => first.resolve() })
  await executeWithTimeout(first.promise, 10_000)
  await executeWithTimeout(committed.promise, 10_000)
  await consumer.stop()
  await consumer.disconnect()

  await consumer.connect()
  await producer.send({ topic, messages: [{ value: 'second' }] })
  const second = Promise.withResolvers<string>()
  await consumer.run({ eachMessage: async ({ message }) => second.resolve(message.value!.toString()) })
  equal(await executeWithTimeout(second.promise, 10_000), 'second')
  await consumer.stop()
})
