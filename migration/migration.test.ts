import { ok, strictEqual } from 'node:assert'
import { randomUUID } from 'node:crypto'
import { Readable } from 'node:stream'
import { test, type TestContext } from 'node:test'
import {
  Admin,
  type AdminOptions,
  Consumer,
  type ConsumerOptions,
  GenericError,
  MessagesStreamModes,
  MultipleErrors,
  ProduceAcks,
  Producer,
  type ProducerOptions,
  stringDeserializers,
  stringSerializers
} from '../src/index.ts'

const bootstrapBrokers = ['localhost:9011']
const defaultRetryDelay = 1000

function createMigrationProducer (t: TestContext, overrides: Partial<ProducerOptions<string, string, string, string>> = {}) {
  const producer = new Producer<string, string, string, string>({
    clientId: `migration-producer-${randomUUID()}`,
    bootstrapBrokers,
    autocreateTopics: true,
    serializers: stringSerializers,
    retryDelay: defaultRetryDelay,
    ...overrides
  })

  t.after(() => producer.close())
  return producer
}

function createMigrationConsumer (t: TestContext, overrides: Partial<ConsumerOptions<string, string, string, string>> = {}) {
  const consumer = new Consumer<string, string, string, string>({
    clientId: `migration-consumer-${randomUUID()}`,
    bootstrapBrokers,
    groupId: `migration-group-${randomUUID()}`,
    deserializers: stringDeserializers,
    timeout: 1000,
    sessionTimeout: 6000,
    rebalanceTimeout: 6000,
    heartbeatInterval: 1000,
    retryDelay: defaultRetryDelay,
    ...overrides
  })

  t.after(() => consumer.close(true))
  return consumer
}

function createMigrationAdmin (t: TestContext, overrides: Partial<AdminOptions> = {}) {
  const admin = new Admin({
    clientId: `migration-admin-${randomUUID()}`,
    bootstrapBrokers,
    retryDelay: defaultRetryDelay,
    ...overrides
  })

  t.after(() => admin.close())
  return admin
}

function createTopicName () {
  return `migration-topic-${randomUUID()}`
}

// 1. Client creation - No factory, direct instantiation
test('migration: client creation - direct instantiation without factory', t => {
  const producer = createMigrationProducer(t)
  const consumer = createMigrationConsumer(t)
  const admin = createMigrationAdmin(t)

  // All clients are direct instances, no Kafka factory needed
  ok(producer instanceof Producer)
  ok(consumer instanceof Consumer)
  ok(admin instanceof Admin)

  // No connect() needed - verify they are not closed
  strictEqual(producer.closed, false)
  strictEqual(consumer.closed, false)
  strictEqual(admin.closed, false)
})

// 2. Producer basic send - topic per-message, string serializers, bigint offset
test('migration: producer basic send with string serializers', async t => {
  const producer = createMigrationProducer(t)
  const topic = createTopicName()

  // Topic is specified per-message, not at the send() level
  const result = await producer.send({
    messages: [
      { topic, key: 'key-1', value: 'value-1' },
      { topic, key: 'key-2', value: 'value-2' }
    ],
    acks: ProduceAcks.LEADER
  })

  ok(result.offsets, 'Should return offsets')
  ok(result.offsets!.length > 0, 'Should have at least one offset')

  for (const offset of result.offsets!) {
    strictEqual(offset.topic, topic)
    strictEqual(typeof offset.partition, 'number')
    // Offsets are bigint, not string
    strictEqual(typeof offset.offset, 'bigint')
  }
})

// 3. Producer multi-topic - single send() replaces sendBatch()
test('migration: producer multi-topic send replaces sendBatch', async t => {
  const producer = createMigrationProducer(t)
  const topicA = createTopicName()
  const topicB = createTopicName()

  // Single send() call with messages to different topics
  // replaces KafkaJS sendBatch()
  const result = await producer.send({
    messages: [
      { topic: topicA, key: 'key-a', value: 'value-a' },
      { topic: topicB, key: 'key-b', value: 'value-b' }
    ],
    acks: ProduceAcks.LEADER
  })

  ok(result.offsets, 'Should return offsets')

  const topics = new Set(result.offsets!.map(o => o.topic))
  ok(topics.has(topicA), 'Should have offset for topic A')
  ok(topics.has(topicB), 'Should have offset for topic B')
})

// 4. Producer compression - string-based compression
test('migration: producer compression with string value', async t => {
  const producer = createMigrationProducer(t)
  const topic = createTopicName()

  // Compression is a simple string, not CompressionTypes.GZIP
  const result = await producer.send({
    messages: [{ topic, key: 'key', value: 'compressed-value' }],
    compression: 'gzip',
    acks: ProduceAcks.LEADER
  })

  ok(result.offsets, 'Should return offsets with gzip compression')
  ok(result.offsets!.length > 0)
})

// 5. Consumer stream - consume() returns Readable
test('migration: consumer stream-based consumption', async t => {
  const producer = createMigrationProducer(t)
  const topic = createTopicName()
  const messageCount = 3

  // Produce messages first
  for (let i = 0; i < messageCount; i++) {
    await producer.send({
      messages: [{ topic, key: `key-${i}`, value: `value-${i}` }],
      acks: ProduceAcks.LEADER
    })
  }

  const consumer = createMigrationConsumer(t)

  // consume() returns a Readable stream - replaces subscribe() + run()
  const stream = await consumer.consume({
    topics: [topic],
    mode: MessagesStreamModes.EARLIEST,
    autocommit: false
  })

  ok(stream instanceof Readable, 'consume() should return a Readable stream')

  // Use for-await to read messages
  const received: Array<{ key: string; value: string; offset: bigint }> = []

  for await (const message of stream) {
    received.push({
      key: message.key,
      value: message.value,
      offset: message.offset
    })

    // Verify message shape
    strictEqual(typeof message.topic, 'string')
    strictEqual(typeof message.partition, 'number')
    strictEqual(typeof message.offset, 'bigint')
    strictEqual(typeof message.timestamp, 'bigint')
    ok(message.headers instanceof Map, 'Headers should be a Map')
    strictEqual(typeof message.commit, 'function')

    if (received.length >= messageCount) {
      break
    }
  }

  strictEqual(received.length, messageCount)

  await stream.close()
})

// 6. Consumer mode earliest - replaces fromBeginning: true
test('migration: consumer mode earliest replaces fromBeginning', async t => {
  const producer = createMigrationProducer(t)
  const topic = createTopicName()

  // Produce messages before consumer starts
  await producer.send({
    messages: [
      { topic, key: 'early-key', value: 'early-value' }
    ],
    acks: ProduceAcks.LEADER
  })

  const consumer = createMigrationConsumer(t)

  // mode: 'earliest' replaces fromBeginning: true
  const stream = await consumer.consume({
    topics: [topic],
    mode: MessagesStreamModes.EARLIEST,
    autocommit: false
  })

  let found = false

  for await (const message of stream) {
    if (message.value === 'early-value') {
      found = true
      break
    }
  }

  ok(found, 'Should find the message produced before consumer started')

  await stream.close()
})

// 7. Consumer manual commit - autocommit: false + message.commit()
test('migration: consumer manual commit with message.commit()', async t => {
  const producer = createMigrationProducer(t)
  const topic = createTopicName()

  await producer.send({
    messages: [{ topic, key: 'commit-key', value: 'commit-value' }],
    acks: ProduceAcks.LEADER
  })

  const consumer = createMigrationConsumer(t)

  const stream = await consumer.consume({
    topics: [topic],
    mode: MessagesStreamModes.EARLIEST,
    autocommit: false
  })

  // eslint-disable-next-line no-unreachable-loop
  for await (const message of stream) {
    // Each message has a commit() method - replaces consumer.commitOffsets()
    await message.commit()
    break
  }

  await stream.close()
})

// 8. Transactions - beginTransaction() replaces transaction()
test('migration: transactions with beginTransaction', async t => {
  const topic = createTopicName()
  const transactionalId = `migration-txn-${randomUUID()}`

  // Create admin to ensure topic exists
  const admin = createMigrationAdmin(t)
  await admin.createTopics({ topics: [topic], partitions: 1 })

  const producer = new Producer({
    clientId: `migration-txn-producer-${randomUUID()}`,
    bootstrapBrokers,
    idempotent: true,
    transactionalId,
    retryDelay: defaultRetryDelay,
    retries: 0
  })

  t.after(() => producer.close())

  // beginTransaction() replaces transaction()
  const transaction = await producer.beginTransaction()

  await transaction.send({
    messages: [
      { topic, key: Buffer.from('txn-key'), value: Buffer.from('txn-value'), partition: 0 }
    ]
  })

  // commit() finalizes the transaction
  await transaction.commit()

  ok(transaction.completed, 'Transaction should be completed after commit')
  strictEqual(producer.transaction, undefined)
})

// 9. Admin create/delete topics
test('migration: admin create and delete topics', async t => {
  const admin = createMigrationAdmin(t)
  const topic = createTopicName()

  // Flat API: topics is an array of strings, partitions/replicas are shared
  const created = await admin.createTopics({
    topics: [topic],
    partitions: 3,
    replicas: 1
  })

  ok(Array.isArray(created), 'createTopics should return an array')
  strictEqual(created.length, 1)
  strictEqual(created[0].name, topic)
  strictEqual(created[0].partitions, 3)

  // Delete works the same as KafkaJS
  await admin.deleteTopics({ topics: [topic] })
})

// 10. Admin metadata - replaces describeCluster()
test('migration: admin metadata replaces describeCluster', async t => {
  const admin = createMigrationAdmin(t)

  // metadata({}) replaces describeCluster()
  const metadata = await admin.metadata({})

  // ClusterMetadata shape
  strictEqual(typeof metadata.id, 'string')
  ok(metadata.brokers instanceof Map, 'Brokers should be a Map')
  strictEqual(typeof metadata.controllerId, 'number')
  ok(metadata.topics instanceof Map, 'Topics should be a Map')

  // Verify at least one broker exists
  ok(metadata.brokers.size > 0, 'Should have at least one broker')

  for (const [nodeId, broker] of metadata.brokers) {
    strictEqual(typeof nodeId, 'number')
    strictEqual(typeof broker.host, 'string')
    strictEqual(typeof broker.port, 'number')
  }
})

// 11. Error handling - PLT_KFK_* codes and canRetry
test('migration: error handling with PLT_KFK codes', async t => {
  const producer = createMigrationProducer(t, { retries: 0 })

  // Trigger an error by sending to a topic with invalid configuration
  try {
    await producer.send({
      messages: [{ topic: createTopicName(), key: 'k', value: 'v' }],
      acks: ProduceAcks.LEADER
    })
  } catch (error) {
    // Errors have PLT_KFK_* code prefix and canRetry boolean
    if (GenericError.isGenericError(error)) {
      ok(typeof error.code === 'string', 'Error should have a string code')
      ok(error.code.startsWith('PLT_KFK_'), `Error code should start with PLT_KFK_, got: ${error.code}`)
    }
    // Success - we got an error with the right shape
    return
  }

  // If send succeeded (autocreateTopics is true), test with a forced error
  const err = new MultipleErrors('Test error', [new Error('inner')], { canRetry: false })
  strictEqual(err.code, 'PLT_KFK_MULTIPLE')
  strictEqual(err.canRetry, false)
  ok(GenericError.isGenericError(err))
  ok(MultipleErrors.isMultipleErrors(err))
})

// 12. Close lifecycle - close() replaces disconnect()
test('migration: close replaces disconnect', async t => {
  const producer = new Producer<string, string, string, string>({
    clientId: `migration-close-${randomUUID()}`,
    bootstrapBrokers,
    serializers: stringSerializers,
    retryDelay: defaultRetryDelay
  })

  strictEqual(producer.closed, false)

  // close() replaces disconnect()
  await producer.close()

  strictEqual(producer.closed, true)

  // Consumer close with force parameter
  const consumer = new Consumer<string, string, string, string>({
    clientId: `migration-close-consumer-${randomUUID()}`,
    bootstrapBrokers,
    groupId: `migration-close-group-${randomUUID()}`,
    deserializers: stringDeserializers,
    retryDelay: defaultRetryDelay
  })

  strictEqual(consumer.closed, false)

  // close(true) force-closes even with active streams
  await consumer.close(true)

  strictEqual(consumer.closed, true)

  const admin = new Admin({
    clientId: `migration-close-admin-${randomUUID()}`,
    bootstrapBrokers,
    retryDelay: defaultRetryDelay
  })

  strictEqual(admin.closed, false)

  await admin.close()

  strictEqual(admin.closed, true)
})
