import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { test } from 'node:test'
import { Admin } from '../../../src/clients/admin/index.ts'
import { AdminClient, KafkaConsumer, Producer, type IAdminClient, type Message, type Metadata } from '../../../src/compatibility/node-rdkafka/index.ts'
import { kafkaBootstrapServers } from '../../helpers.ts'

const broker = process.env.KAFKA_HOST ?? kafkaBootstrapServers.join(',')

test('transactions expose only committed records to read_committed consumers', { timeout: 30_000 }, async t => {
  const topic = `rdkafka-transaction-${randomUUID()}`
  const common = { 'bootstrap.servers': broker }
  const admin = AdminClient.create(common)
  const producer = new Producer({ ...common, 'transactional.id': `rdkafka-${randomUUID()}` })
  const consumer = new KafkaConsumer({
    ...common,
    'group.id': randomUUID(),
    'enable.auto.commit': false,
    'isolation.level': 'read_committed'
  }, { 'auto.offset.reset': 'earliest' })
  t.after(async () => {
    await Promise.allSettled([disconnect(producer), disconnect(consumer)])
    admin.disconnect()
  })

  await createTopic(admin, topic)
  await connect(producer)
  await initTransactions(producer)

  await beginTransaction(producer)
  producer.produce(topic, 0, Buffer.from('committed'))
  await commitTransaction(producer)

  await beginTransaction(producer)
  producer.produce(topic, 0, Buffer.from('aborted'))
  await abortTransaction(producer)

  await connect(consumer)
  consumer.subscribe([topic])
  const messages = await consumeAtMost(consumer, 2, 10_000)
  assert.deepEqual(messages.map(message => message.value?.toString()), ['committed'])
})

test('transactional offsets commit and abort with the transaction', { timeout: 30_000 }, async t => {
  const topic = `rdkafka-offset-transaction-${randomUUID()}`
  const groupId = `rdkafka-offset-group-${randomUUID()}`
  const common = { 'bootstrap.servers': broker }
  const admin = AdminClient.create(common)
  const offsetsAdmin = new Admin({ bootstrapBrokers: broker.split(',') })
  const seed = new Producer(common)
  const producer = new Producer({ ...common, 'transactional.id': `rdkafka-offset-${randomUUID()}` })
  const consumer = new KafkaConsumer({
    ...common,
    'group.id': groupId,
    'enable.auto.commit': false
  }, { 'auto.offset.reset': 'earliest' })
  t.after(async () => {
    await Promise.allSettled([disconnect(seed), disconnect(producer), disconnect(consumer)])
    await offsetsAdmin.close()
    admin.disconnect()
  })

  await createTopic(admin, topic)
  await Promise.all([connect(seed), connect(producer), connect(consumer)])
  seed.produce(topic, 0, Buffer.from('one'))
  seed.produce(topic, 0, Buffer.from('two'))
  await flush(seed)

  consumer.subscribe([topic])
  const messages = await consumeAtMost(consumer, 2, 10_000)
  assert.equal(messages.length, 2)
  await initTransactions(producer)

  await beginTransaction(producer)
  await sendOffsets(producer, [{ topic, partition: 0, offset: 1 }], consumer)
  await commitTransaction(producer)
  assert.equal(await committedOffset(offsetsAdmin, groupId, topic), 1n)

  await beginTransaction(producer)
  await sendOffsets(producer, [{ topic, partition: 0, offset: 2 }], consumer)
  await abortTransaction(producer)
  assert.equal(await committedOffset(offsetsAdmin, groupId, topic), 1n)
})

interface Connectable {
  connect (options: object, callback: (error: Error | null, metadata?: Metadata) => void): unknown
  disconnect (callback: (error: Error | null) => void): unknown
}

function connect (client: Connectable): Promise<Metadata> {
  return new Promise((resolve, reject) => {
    client.connect({}, (error, metadata) => error || !metadata ? reject(error) : resolve(metadata))
  })
}

function disconnect (client: Connectable): Promise<void> {
  return new Promise((resolve, reject) => {
    client.disconnect(error => error ? reject(error) : resolve())
  })
}

function createTopic (admin: IAdminClient, topic: string): Promise<void> {
  return new Promise((resolve, reject) => {
    admin.createTopic({ topic, num_partitions: 1, replication_factor: 1 }, error => error ? reject(error) : resolve())
  })
}

function initTransactions (producer: Producer): Promise<void> {
  return new Promise((resolve, reject) => {
    producer.initTransactions(10_000, error => error ? reject(error) : resolve())
  })
}

function beginTransaction (producer: Producer): Promise<void> {
  return new Promise((resolve, reject) => {
    producer.beginTransaction(error => error ? reject(error) : resolve())
  })
}

function commitTransaction (producer: Producer): Promise<void> {
  return new Promise((resolve, reject) => {
    producer.commitTransaction(10_000, error => error ? reject(error) : resolve())
  })
}

function abortTransaction (producer: Producer): Promise<void> {
  return new Promise((resolve, reject) => {
    producer.abortTransaction(10_000, error => error ? reject(error) : resolve())
  })
}

function sendOffsets (producer: Producer, offsets: Array<{ topic: string; partition: number; offset: number }>, consumer: KafkaConsumer): Promise<void> {
  return new Promise((resolve, reject) => {
    producer.sendOffsetsToTransaction(offsets, consumer, 10_000, error => error ? reject(error) : resolve())
  })
}

function flush (producer: Producer): Promise<void> {
  return new Promise((resolve, reject) => {
    producer.flush(10_000, error => error ? reject(error) : resolve())
  })
}

async function committedOffset (admin: Admin, groupId: string, topic: string): Promise<bigint> {
  const groups = await admin.listConsumerGroupOffsets({
    groups: [{ groupId, topics: [{ name: topic, partitionIndexes: [0] }] }],
    requireStable: true
  })
  return groups[0].topics[0].partitions[0].committedOffset
}

async function consumeAtMost (consumer: KafkaConsumer, count: number, timeout: number): Promise<Message[]> {
  const messages: Message[] = []
  const deadline = Date.now() + timeout
  while (messages.length < count && Date.now() < deadline) {
    const batch = await new Promise<Message[]>((resolve, reject) => {
      consumer.consume(count - messages.length, (error, result) => error ? reject(error) : resolve(result ?? []))
    })
    messages.push(...batch)
    if (batch.length === 0 && messages.length > 0) {
      break
    }
  }
  return messages
}
