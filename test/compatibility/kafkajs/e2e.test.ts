import { deepStrictEqual, equal, ok } from 'node:assert'
import { randomUUID } from 'node:crypto'
import { test } from 'node:test'
import { Kafka, KafkaJSError } from '../../../src/compatibility/kafkajs/index.ts'
import { createTopic, executeWithTimeout, kafkaBootstrapServers } from '../../helpers.ts'

test('KafkaJS producer, consumer and admin compatibility', async t => {
  const topic = await createTopic(t, true, 2)
  const kafka = new Kafka({ brokers: kafkaBootstrapServers, clientId: `kafkajs-compat-${randomUUID()}` })
  const admin = kafka.admin()
  const producer = kafka.producer()
  const consumer = kafka.consumer({ groupId: `kafkajs-compat-${randomUUID()}` })

  t.after(async () => {
    await Promise.allSettled([admin.disconnect(), producer.disconnect(), consumer.disconnect()])
  })

  await Promise.all([admin.connect(), producer.connect(), consumer.connect()])
  const topics = await admin.listTopics()
  ok(topics.includes(topic))
  const metadata = await admin.fetchTopicMetadata({ topics: [topic] })
  equal(metadata.topics[0].partitions.length, 2)

  const sent = await producer.sendBatch({
    topicMessages: [
      { topic, messages: [{ key: 'a', value: 'one' }] },
      { topic, messages: [{ key: 'b', value: 'two' }] }
    ]
  })
  ok(sent.length >= 1)

  const received: string[] = []
  const completed = Promise.withResolvers<void>()
  await consumer.subscribe({ topic, fromBeginning: true })
  await consumer.run({
    eachMessage: async ({ message }) => {
      received.push(message.value!.toString())
      if (received.length === 2) {
        completed.resolve()
      }
    }
  })
  await completed.promise
  await consumer.stop()
  deepStrictEqual(received.sort(), ['one', 'two'])

  const offsets = await admin.fetchTopicOffsets(topic)
  equal(offsets.length, 2)
})

test('defaults topic autocreation and supports tombstones', async t => {
  const topic = await createTopic(t)
  const kafka = new Kafka({ brokers: kafkaBootstrapServers, clientId: `kafkajs-autocreate-${randomUUID()}` })
  const producer = kafka.producer()
  const consumer = kafka.consumer({ groupId: `kafkajs-autocreate-${randomUUID()}` })
  t.after(async () => {
    await Promise.allSettled([producer.disconnect(), consumer.disconnect()])
  })

  await producer.connect()
  await producer.send({ topic, messages: [{ key: 'deleted', value: null }] })
  const received = Promise.withResolvers<Buffer | null>()
  await consumer.subscribe({ topic, fromBeginning: true })
  await consumer.run({ eachMessage: async ({ message }) => received.resolve(message.value) })
  equal(await received.promise, null)
  await consumer.stop()
})

test('resumes from committed offsets when fromBeginning is true', async t => {
  const topic = await createTopic(t, true)
  const groupId = `kafkajs-committed-${randomUUID()}`
  const kafka = new Kafka({ brokers: kafkaBootstrapServers, clientId: `kafkajs-committed-${randomUUID()}` })
  const producer = kafka.producer()
  const first = kafka.consumer({ groupId })
  t.after(async () => {
    await Promise.allSettled([producer.disconnect(), first.disconnect()])
  })

  await producer.connect()
  await producer.send({ topic, messages: [{ value: 'first' }] })
  const firstReceived = Promise.withResolvers<void>()
  const firstCommitted = Promise.withResolvers<void>()
  first.on(first.events.COMMIT_OFFSETS, () => firstCommitted.resolve())
  await first.subscribe({ topic, fromBeginning: true })
  await first.run({ eachMessage: async () => firstReceived.resolve() })
  equal(await executeWithTimeout(firstReceived.promise, 10_000), undefined)
  equal(await executeWithTimeout(firstCommitted.promise, 10_000), undefined)
  await first.stop()
  await first.disconnect()

  await producer.send({ topic, messages: [{ value: 'second' }] })
  const second = kafka.consumer({ groupId })
  t.after(() => second.disconnect())
  const values: string[] = []
  const secondReceived = Promise.withResolvers<void>()
  await second.subscribe({ topic, fromBeginning: true })
  await second.run({
    eachMessage: async ({ message }) => {
      values.push(message.value!.toString())
      secondReceived.resolve()
    }
  })
  equal(await executeWithTimeout(secondReceived.promise, 10_000), undefined)
  await second.stop()
  deepStrictEqual(values, ['second'])
})

test('does not commit a message when its handler fails', async t => {
  const topic = await createTopic(t, true)
  const groupId = `kafkajs-failure-${randomUUID()}`
  const kafka = new Kafka({ brokers: kafkaBootstrapServers, clientId: `kafkajs-failure-${randomUUID()}` })
  const producer = kafka.producer()
  let restartChecks = 0
  const failing = kafka.consumer({
    groupId,
    retry: {
      restartOnFailure: async () => {
        restartChecks++
        return false
      }
    }
  })
  t.after(async () => {
    await Promise.allSettled([producer.disconnect(), failing.disconnect()])
  })
  await producer.connect()
  await producer.send({ topic, messages: [{ value: 'retry-me' }] })

  const crashed = Promise.withResolvers<{ restart?: boolean }>()
  failing.on(failing.events.CRASH, event => crashed.resolve(event.payload as { restart?: boolean }))
  await failing.subscribe({ topic, fromBeginning: true })
  await failing.run({
    eachMessage: async () => {
      throw new KafkaJSError('handler failed')
    }
  })
  const crash = await executeWithTimeout(crashed.promise, 10_000)
  ok(typeof crash !== 'string')
  equal(crash.restart, false)
  equal(restartChecks, 1)
  await failing.stop()
  await failing.disconnect()

  const retrying = kafka.consumer({ groupId })
  t.after(() => retrying.disconnect())
  const retried = Promise.withResolvers<string>()
  await retrying.subscribe({ topic, fromBeginning: true })
  await retrying.run({ eachMessage: async ({ message }) => retried.resolve(message.value!.toString()) })
  equal(await executeWithTimeout(retried.promise, 10_000), 'retry-me')
  await retrying.stop()
})

test('restarts after a retriable crash when restartOnFailure allows it', async t => {
  const topic = await createTopic(t, true)
  const kafka = new Kafka({ brokers: kafkaBootstrapServers, clientId: `kafkajs-restart-${randomUUID()}` })
  const producer = kafka.producer()
  let restartChecks = 0
  const consumer = kafka.consumer({
    groupId: `kafkajs-restart-${randomUUID()}`,
    retry: {
      initialRetryTime: 10,
      restartOnFailure: async () => {
        restartChecks++
        return true
      }
    }
  })
  t.after(async () => {
    await Promise.allSettled([producer.disconnect(), consumer.disconnect()])
  })
  await producer.connect()
  await producer.send({ topic, messages: [{ value: 'eventually' }] })

  let attempts = 0
  const restarted = Promise.withResolvers<boolean>()
  const completed = Promise.withResolvers<string>()
  consumer.on(consumer.events.CRASH, event => {
    restarted.resolve((event.payload as { restart: boolean }).restart)
  })
  await consumer.subscribe({ topic, fromBeginning: true })
  await consumer.run({
    eachMessage: async ({ message }) => {
      if (attempts++ === 0) {
        throw new KafkaJSError('temporary failure')
      }
      completed.resolve(message.value!.toString())
    }
  })
  equal(await executeWithTimeout(restarted.promise, 10_000), true)
  equal(await executeWithTimeout(completed.promise, 10_000), 'eventually')
  equal(restartChecks, 1)
  await consumer.stop()
})
