import { deepStrictEqual, equal, ok, rejects } from 'node:assert'
import { randomUUID } from 'node:crypto'
import { test } from 'node:test'
import { CompressionTypes, Kafka, Partitioners } from '../../../src/compatibility/kafkajs/index.ts'
import { createTopic, executeWithTimeout, kafkaBootstrapServers } from '../../helpers.ts'

test('producer lifecycle reconnects and sends compressed records', async t => {
  const topic = await createTopic(t, true, 2)
  const producer = new Kafka({ brokers: kafkaBootstrapServers }).producer({
    createPartitioner: Partitioners.DefaultPartitioner
  })
  t.after(() => producer.disconnect())

  const events: string[] = []
  const requests: Array<Record<string, unknown>> = []
  producer.on(producer.events.CONNECT, event => events.push(event.type))
  producer.on(producer.events.DISCONNECT, event => events.push(event.type))
  producer.on(producer.events.REQUEST, event => requests.push(event.payload as Record<string, unknown>))

  await rejects(producer.send({ topic, messages: [{ value: 'disconnected' }] }), /producer is disconnected/)
  await producer.connect()
  await producer.send({ topic, compression: CompressionTypes.GZIP, messages: [{ value: 'gzip' }] })
  await producer.send({ topic, compression: CompressionTypes.Snappy, messages: [{ value: 'snappy' }] })
  await producer.send({ topic, compression: CompressionTypes.LZ4, messages: [{ value: 'lz4' }] })
  await producer.disconnect()
  await rejects(producer.send({ topic, messages: [{ value: 'disconnected' }] }), /producer is disconnected/)

  await producer.connect()
  await producer.send({ topic, messages: [{ value: 'reconnected' }] })
  deepStrictEqual(events, [producer.events.CONNECT, producer.events.DISCONNECT, producer.events.CONNECT])
  ok(requests.length > 0)
  for (const field of ['apiKey', 'apiName', 'apiVersion', 'broker', 'clientId', 'correlationId', 'createdAt', 'sentAt', 'size']) {
    ok(requests[0][field] !== undefined, `missing request field ${field}`)
  }
})

test('transaction combines batch records and guards an ended transaction', async t => {
  const topic = await createTopic(t, true, 2)
  const kafka = new Kafka({ brokers: kafkaBootstrapServers })
  const producer = kafka.producer({
    transactionalId: `kafkajs-transaction-${randomUUID()}`,
    transactionTimeout: 30_000,
    createPartitioner: Partitioners.DefaultPartitioner
  })
  const consumer = kafka.consumer({ groupId: `kafkajs-transaction-${randomUUID()}` })
  t.after(async () => Promise.allSettled([producer.disconnect(), consumer.disconnect()]))

  await producer.connect()
  const transaction = await producer.transaction()
  await transaction.sendBatch({
    topicMessages: [
      { topic, messages: [{ key: 'a', value: 'one' }] },
      { topic, messages: [{ key: 'b', value: 'two' }] }
    ]
  })
  await transaction.commit()
  equal(transaction.isActive(), false)
  await rejects(
    transaction.send({ topic, messages: [{ value: 'late' }] }),
    /Cannot continue to use transaction once ended/
  )

  const values: string[] = []
  const complete = Promise.withResolvers<void>()
  await consumer.connect()
  await consumer.subscribe({ topic, fromBeginning: true })
  await consumer.run({
    eachMessage: async ({ message }) => {
      values.push(message.value!.toString())
      if (values.length === 2) {
        complete.resolve()
      }
    }
  })
  await executeWithTimeout(complete.promise, 10_000)
  await consumer.stop()
  deepStrictEqual(values.sort(), ['one', 'two'])
})

test('preserves an active transaction across reconnect', async t => {
  const topic = await createTopic(t, true)
  const kafka = new Kafka({ brokers: kafkaBootstrapServers })
  const producer = kafka.producer({
    transactionalId: `kafkajs-reconnect-transaction-${randomUUID()}`,
    createPartitioner: Partitioners.DefaultPartitioner
  })
  const consumer = kafka.consumer({ groupId: `kafkajs-reconnect-transaction-${randomUUID()}` })
  t.after(async () => Promise.allSettled([producer.disconnect(), consumer.disconnect()]))

  await producer.connect()
  const transaction = await producer.transaction()
  await transaction.send({ topic, messages: [{ value: 'before' }] })
  await producer.disconnect()
  await producer.connect()
  equal(transaction.isActive(), true)
  await transaction.send({ topic, messages: [{ value: 'after' }] })
  await transaction.commit()

  const received: string[] = []
  const complete = Promise.withResolvers<void>()
  await consumer.subscribe({ topic, fromBeginning: true })
  await consumer.run({
    eachMessage: async ({ message }) => {
      received.push(message.value!.toString())
      if (received.length === 2) {
        complete.resolve()
      }
    }
  })
  await executeWithTimeout(complete.promise, 10_000)
  await consumer.stop()
  deepStrictEqual(received.sort(), ['after', 'before'])
})

test('transaction commits and aborts arbitrary consumer group offsets', async t => {
  const topic = await createTopic(t, true)
  const groupId = `kafkajs-offset-transaction-${randomUUID()}`
  const kafka = new Kafka({ brokers: kafkaBootstrapServers })
  const producer = kafka.producer({
    transactionalId: `kafkajs-offset-transaction-${randomUUID()}`,
    createPartitioner: Partitioners.DefaultPartitioner
  })
  const admin = kafka.admin()
  t.after(async () => Promise.allSettled([producer.disconnect(), admin.disconnect()]))
  await Promise.all([producer.connect(), admin.connect()])

  const committed = await producer.transaction()
  await committed.sendOffsets({
    consumerGroupId: groupId,
    topics: [{ topic, partitions: [{ partition: 0, offset: '1' }] }]
  })
  equal(committed.isActive(), true)
  await committed.commit()
  equal((await admin.fetchOffsets({ groupId, topics: [topic] }))[0].partitions[0].offset, '1')

  const aborted = await producer.transaction()
  await aborted.sendOffsets({
    consumerGroupId: groupId,
    topics: [{ topic, partitions: [{ partition: 0, offset: '2' }] }]
  })
  await aborted.abort()
  equal((await admin.fetchOffsets({ groupId, topics: [topic] }))[0].partitions[0].offset, '1')
})
