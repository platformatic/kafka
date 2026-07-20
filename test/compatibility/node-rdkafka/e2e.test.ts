import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { test } from 'node:test'
import { AdminClient, KafkaConsumer, Producer, type Message, type Metadata } from '../../../src/compatibility/node-rdkafka/index.ts'
import { CODES } from '../../../src/compatibility/node-rdkafka/errors.ts'
import { kafkaBootstrapServers } from '../../helpers.ts'

const broker = process.env.KAFKA_HOST ?? kafkaBootstrapServers.join(',')

test('adapts the node-rdkafka produce and consume lifecycle', { timeout: 30_000 }, async t => {
  const topic = `rdkafka-compat-${randomUUID()}`
  const common = { 'bootstrap.servers': broker }
  const admin = AdminClient.create(common)
  let producer: Producer | undefined
  let consumer: KafkaConsumer | undefined
  t.after(async () => {
    await Promise.allSettled([
      producer ? disconnect(producer) : Promise.resolve(),
      consumer ? disconnect(consumer) : Promise.resolve()
    ])
    admin.disconnect()
  })
  await new Promise<void>((resolve, reject) => {
    admin.createTopic({ topic, num_partitions: 1, replication_factor: 1 }, error => error ? reject(error) : resolve())
  })

  const activeProducer = new Producer({ ...common, dr_cb: true })
  producer = activeProducer
  await connect(activeProducer)
  activeProducer.setPollInterval(10)
  const delivered = new Promise<void>((resolve, reject) => {
    activeProducer.once('delivery-report', (error: Error | null, report: { offset: number }) => {
      if (error) {
        reject(error)
      } else {
        assert.ok(report.offset >= 0)
        resolve()
      }
    })
  })
  activeProducer.produce(topic, null, Buffer.from('compatible'), 'key')
  await delivered

  const activeConsumer = new KafkaConsumer({
    ...common,
    'group.id': randomUUID(),
    'enable.auto.commit': false
  }, { 'auto.offset.reset': 'earliest' })
  consumer = activeConsumer
  await connect(activeConsumer)
  activeConsumer.subscribe([topic])

  let message: Message | undefined
  const deadline = Date.now() + 15_000
  while (!message && Date.now() < deadline) {
    const messages = await new Promise<Message[]>((resolve, reject) => {
      activeConsumer.consume(1, (error, result) => error ? reject(error) : resolve(result ?? []))
    })
    message = messages[0]
  }
  assert.equal(message?.value?.toString(), 'compatible')
  assert.equal(message?.key?.toString(), 'key')

  await disconnect(activeConsumer)
  consumer = undefined
  await disconnect(activeProducer)
  producer = undefined
  await new Promise<void>((resolve, reject) => {
    admin.deleteTopic(topic, error => error ? reject(error) : resolve())
  })
})

test('commits a manually assigned message without active group membership', { timeout: 30_000 }, async t => {
  const topic = `rdkafka-simple-commit-${randomUUID()}`
  const groupId = `rdkafka-simple-commit-group-${randomUUID()}`
  const common = { 'bootstrap.servers': broker }
  const admin = AdminClient.create(common)
  const commit = Promise.withResolvers<void>()
  const consumer = new KafkaConsumer({
    ...common,
    'group.id': groupId,
    'enable.auto.commit': false,
    offset_commit_cb: error => error ? commit.reject(error) : commit.resolve()
  })
  const observer = new KafkaConsumer({ ...common, 'group.id': groupId })
  t.after(async () => {
    await Promise.allSettled([disconnect(consumer), disconnect(observer)])
    admin.disconnect()
  })
  await new Promise<void>((resolve, reject) => {
    admin.createTopic({ topic, num_partitions: 1, replication_factor: 1 }, error => error ? reject(error) : resolve())
  })
  await Promise.all([connect(consumer), connect(observer)])

  consumer.assign([{ topic, partition: 0 }])
  consumer.commitMessage({ topic, partition: 0, offset: 0 })
  await commit.promise

  const offsets = await new Promise<Array<{ offset: number }>>((resolve, reject) => {
    observer.committed([{ topic, partition: 0 }], 10_000, (error, offsets) => error ? reject(error) : resolve(offsets ?? []))
  })
  assert.equal(offsets[0].offset, 1)
})

test('function rebalance callback gates delivery until it assigns', { timeout: 30_000 }, async t => {
  const topic = `rdkafka-rebalance-owner-${randomUUID()}`
  const common = { 'bootstrap.servers': broker }
  const admin = AdminClient.create(common)
  const producer = new Producer({ ...common, dr_cb: true })
  let callbackOwnedAssignment = false
  const consumer = new KafkaConsumer({
    ...common,
    'group.id': randomUUID(),
    'enable.auto.commit': false,
    rebalance_cb (this: KafkaConsumer, error, assignments) {
      if (error.code === CODES.ERRORS.ERR__REVOKE_PARTITIONS) {
        this.unassign()
        return
      }
      assert.deepEqual(this.assignments(), [])
      callbackOwnedAssignment = true
      this.assign(assignments)
    }
  }, { 'auto.offset.reset': 'earliest' })
  t.after(async () => {
    await Promise.allSettled([disconnect(producer), disconnect(consumer)])
    admin.disconnect()
  })
  await new Promise<void>((resolve, reject) => {
    admin.createTopic({ topic, num_partitions: 1, replication_factor: 1 }, error => error ? reject(error) : resolve())
  })
  await Promise.all([connect(producer), connect(consumer)])
  producer.setPollInterval(10)
  const delivered = new Promise<void>((resolve, reject) => {
    producer.once('delivery-report', error => error ? reject(error) : resolve())
  })
  producer.produce(topic, 0, Buffer.from('owned'))
  await delivered

  consumer.subscribe([topic])
  const messagePromise = new Promise<Message>((resolve, reject) => {
    consumer.consume(1, (error, messages) => {
      return error || !messages?.[0] ? reject(error) : resolve(messages[0])
    })
  })
  assert.equal((await messagePromise).value?.toString(), 'owned')
  assert.equal(callbackOwnedAssignment, true)
})

test('reconnects producer and consumer and exposes producer offsetsForTimes', { timeout: 30_000 }, async t => {
  const topic = `rdkafka-reconnect-${randomUUID()}`
  const common = { 'bootstrap.servers': broker }
  const admin = AdminClient.create(common)
  const producer = new Producer(common)
  const consumer = new KafkaConsumer({ ...common, 'group.id': randomUUID() })
  t.after(async () => {
    await Promise.allSettled([disconnect(producer), disconnect(consumer)])
    admin.disconnect()
  })
  await new Promise<void>((resolve, reject) => {
    admin.createTopic({ topic, num_partitions: 1, replication_factor: 1 }, error => error ? reject(error) : resolve())
  })

  await Promise.all([connect(producer), connect(consumer)])
  await Promise.all([disconnect(producer), disconnect(consumer)])
  await Promise.all([connect(producer), connect(consumer)])

  const timestamp = Date.now()
  producer.produce(topic, 0, Buffer.from('reconnected'), null, timestamp)
  await new Promise<void>((resolve, reject) => producer.flush(10_000, error => error ? reject(error) : resolve()))
  const offsets = await new Promise<Array<{ offset?: number }>>((resolve, reject) => {
    producer.offsetsForTimes([{ topic, partition: 0, offset: timestamp }], 10_000, (error, result) => {
      if (error) {
        reject(error)
      } else {
        resolve(result ?? [])
      }
    })
  })
  assert.equal(offsets[0].offset, 0)
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
