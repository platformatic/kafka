import assert from 'node:assert/strict'
import { EventEmitter, once } from 'node:events'
import { Readable } from 'node:stream'
import { test } from 'node:test'
import { Client } from '../../../src/compatibility/node-rdkafka/client.ts'
import { mapConsumerConfig, mapProducerConfig } from '../../../src/compatibility/node-rdkafka/config.ts'
import { KafkaConsumer } from '../../../src/compatibility/node-rdkafka/consumer.ts'
import { CODES, LibrdKafkaError, toLibrdKafkaError } from '../../../src/compatibility/node-rdkafka/errors.ts'
import { ConsumerStream } from '../../../src/compatibility/node-rdkafka/streams.ts'
import {
  type ConsumerGlobalConfig,
  type EventListenerMap,
  type Message,
  type ProducerGlobalConfig
} from '../../../src/compatibility/node-rdkafka/types.ts'
import compatibility, {
  AdminClient,
  Consumer,
  HighLevelProducer,
  KafkaConsumer as ExportedKafkaConsumer,
  Producer,
  Topic,
  createReadStream,
  createWriteStream,
  features,
  librdkafkaVersion
} from '../../../src/compatibility/node-rdkafka/index.ts'
import { type ClusterMetadata } from '../../../src/clients/base/types.ts'
import * as Kafka from '../../../src/index.ts'

class FakePlatformClient extends EventEmitter {
  closed = false

  close (callback: (error: Error | null) => void): void {
    this.closed = true
    callback(null)
  }

  isConnected (): boolean {
    return !this.closed
  }

  metadata (_options: unknown, callback: (error: Error | null, metadata?: ClusterMetadata) => void): void {
    callback(null, {
      id: 'cluster',
      controllerId: 1,
      lastUpdate: Date.now(),
      brokers: new Map([[1, { host: 'localhost', port: 9092, rack: null }]]),
      topics: new Map([['events', {
        id: 'topic-id',
        partitionsCount: 1,
        lastUpdate: Date.now(),
        partitions: [{ leader: 1, leaderEpoch: 2, replicas: [1], isr: [1], offlineReplicas: [] }]
      }]])
    })
  }

  listOffsets (options: { timestamp?: bigint }, callback: (error: Error | null, offsets?: Map<string, bigint[]>) => void): void {
    callback(null, new Map([['events', [options.timestamp === -2n ? 3n : 9n]]]))
  }

  listOffsetsWithTimestamps (options: { timestamp?: bigint }, callback: (error: Error | null, offsets?: Map<string, Map<number, { offset: bigint }>>) => void): void {
    callback(null, new Map([['events', new Map([[0, { offset: options.timestamp === 123n ? 4n : 8n }]])]]))
  }
}

class TestClient extends Client {
  constructor (client: FakePlatformClient, factory: (() => FakePlatformClient) | null = null) {
    super(client, {}, null, factory)
  }
}

function connectConsumer (consumer: KafkaConsumer): void {
  consumer.isConnected = () => true
}

test('exports the node-rdkafka 3.6 surface and numeric constants', () => {
  assert.notEqual(Consumer, ExportedKafkaConsumer)
  assert.equal(Consumer.prototype, ExportedKafkaConsumer.prototype)
  assert.equal(typeof AdminClient, 'object')
  assert.deepEqual(Object.keys(AdminClient), ['create'])
  assert.equal(compatibility.Producer, Producer)
  assert.equal(compatibility.HighLevelProducer, HighLevelProducer)
  assert.equal(compatibility.AdminClient, AdminClient)
  assert.equal(compatibility.createReadStream, createReadStream)
  assert.equal(compatibility.createWriteStream, createWriteStream)
  assert.equal(Topic('events'), 'events')
  assert.doesNotThrow(() => Reflect.construct(Topic, ['events']))
  assert.equal(Topic.OFFSET_BEGINNING, -2)
  assert.equal(CODES.ERRORS.ERR__ASSIGN_PARTITIONS, -175)
  assert.equal(CODES.ERRORS.ERR__INVALID_DIFFERENT_RECORD, -138)
  assert.equal(CODES.ERRORS.ERR__DESTROY_BROKER, -137)
  assert.equal(CODES.ERRORS.ERR_REBOOTSTRAP_REQUIRED, 129)
  assert.equal(CODES.ERRORS.ERR_TOPIC_ALREADY_EXISTS, 36)
  assert.ok(features.includes('sasl_scram'))
  assert.equal(librdkafkaVersion, '2.12.0')
  assert.equal('kAssignmentsForTopic' in Kafka, false)
  assert.equal('kAssignmentsForTopic' in compatibility, false)
  assert.equal('Client' in compatibility, false)
  assert.equal('LibrdKafkaError' in compatibility, false)
  assert.deepEqual(Object.keys(compatibility), [
    'Consumer',
    'Producer',
    'HighLevelProducer',
    'AdminClient',
    'KafkaConsumer',
    'createReadStream',
    'createWriteStream',
    'CODES',
    'Topic',
    'features',
    'librdkafkaVersion'
  ])
})

test('exposes typed configuration and event listener declarations', () => {
  const producer: ProducerGlobalConfig = {
    'bootstrap.servers': 'localhost:9092',
    'queue.buffering.max.messages': 100,
    'message.send.max.retries': 3
  }
  const consumer: ConsumerGlobalConfig = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'workers',
    'enable.auto.offset.store': false
  }
  const ready: EventListenerMap['ready'] = [{ name: 'broker' }, { orig_broker_id: 1, orig_broker_name: 'broker', brokers: [], topics: [] }]
  assert.equal(producer['queue.buffering.max.messages'], 100)
  assert.equal(consumer['enable.auto.offset.store'], false)
  assert.equal(ready[0].name, 'broker')
})

test('maps common producer and consumer configuration', () => {
  const producer = mapProducerConfig({
    'client.id': 'producer',
    'metadata.broker.list': 'one:9092,two:9092',
    'request.required.acks': -1,
    'enable.idempotence': true,
    'transactional.id': 'transaction',
    'message.send.max.retries': 8,
    'retry.backoff.ms': 25
  })
  assert.deepEqual(producer.bootstrapBrokers, ['one:9092', 'two:9092'])
  assert.equal(producer.clientId, 'producer')
  assert.equal(producer.acks, -1)
  assert.equal(producer.idempotent, true)
  assert.equal(producer.transactionalId, 'transaction')
  assert.equal(producer.retries, 8)
  assert.equal(producer.retryDelay, 25)

  const consumer = mapConsumerConfig({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'workers',
    'enable.auto.commit': true,
    'auto.commit.interval.ms': 250,
    'isolation.level': 'read_uncommitted'
  }, { 'auto.offset.reset': 'earliest' })
  assert.equal(consumer.groupId, 'workers')
  assert.equal(consumer.autocommit, 250)
  assert.equal(consumer.isolationLevel, 0)
  assert.deepEqual(consumer.context, { offsetReset: 'earliest' })

  const fetchSizes = mapConsumerConfig({
    'fetch.max.bytes': 1000,
    'max.partition.fetch.bytes': 250,
    'group.id': 'workers'
  })
  assert.equal(fetchSizes.maxBytes, 1000)
  assert.equal(fetchSizes.maxBytesPerPartition, 250)

  const range = mapConsumerConfig({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'workers',
    'partition.assignment.strategy': 'cooperative-sticky, range'
  }).partitionAssigner
  const rangeAssignments = range?.('', new Map([
    ['a', { memberId: 'a' }],
    ['b', { memberId: 'b' }]
  ]), new Set(['events']), {
    id: 'cluster',
    controllerId: 1,
    lastUpdate: 0,
    brokers: new Map(),
    topics: new Map([['events', { id: 'events', partitionsCount: 5, lastUpdate: 0, partitions: [] }]])
  })
  assert.deepEqual(rangeAssignments?.map(assignment => assignment.assignments.get('events')?.partitions), [[0, 1], [2, 3, 4]])

  const topicProducer = mapProducerConfig({ 'bootstrap.servers': 'global:9092' }, {
    topic: 'default-topic',
    'message.timeout.ms': 123,
    'compression.codec': 'gzip'
  })
  assert.equal(topicProducer.timeout, 123)
  assert.equal(topicProducer.compression, 'gzip')
  assert.deepEqual(topicProducer.bootstrapBrokers, ['global:9092'])

  assert.throws(() => mapProducerConfig({
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanisms': 'GSSAPI'
  }), (error: LibrdKafkaError) => error.code === CODES.ERRORS.ERR__NOT_IMPLEMENTED)
})

test('provides callback lifecycle, metadata conversion, and watermarks', async () => {
  const platform = new FakePlatformClient()
  const client = new TestClient(platform)
  const ready = once(client, 'ready')
  const metadata = await new Promise((resolve, reject) => {
    client.connect({}, (error, result) => error ? reject(error) : resolve(result))
  })
  await ready
  assert.deepEqual(metadata, {
    orig_broker_id: 1,
    orig_broker_name: 'localhost:9092',
    brokers: [{ id: 1, host: 'localhost', port: 9092 }],
    topics: [{ name: 'events', partitions: [{ id: 0, leader: 1, replicas: [1], isrs: [1] }] }]
  })
  const watermarks = await new Promise((resolve, reject) => {
    client.queryWatermarkOffsets('events', 0, (error, result) => error ? reject(error) : resolve(result))
  })
  assert.deepEqual(watermarks, { lowOffset: 3, highOffset: 9 })
  assert.equal(client.isConnected(), true)

  const disconnected = once(client, 'disconnected')
  client.disconnect()
  await disconnected
  assert.equal(platform.closed, true)
})

test('rejects public metadata while disconnected without breaking connect', async () => {
  const client = new TestClient(new FakePlatformClient())
  const metadataError = await new Promise<LibrdKafkaError>(resolve => {
    assert.equal(client.getMetadata({}, error => resolve(error as LibrdKafkaError)), client)
  })
  assert.equal(metadataError.code, CODES.ERRORS.ERR__STATE)

  const metadata = await new Promise((resolve, reject) => {
    assert.equal(client.connect({}, (error, result) => error ? reject(error) : resolve(result)), client)
  })
  assert.ok(metadata)
})

test('provides shared timestamp offsets and recreates a closed client', async () => {
  const clients = [new FakePlatformClient()]
  const client = new TestClient(clients[0], () => {
    const replacement = new FakePlatformClient()
    clients.push(replacement)
    return replacement
  })
  await new Promise<void>((resolve, reject) => client.connect({}, error => error ? reject(error) : resolve()))
  const offsets = await new Promise((resolve, reject) => {
    client.offsetsForTimes([
      { topic: 'events', partition: 0, offset: 123 },
      { topic: 'events', partition: 0, offset: 456 },
      { topic: 'events', partition: 0, offset: 123 }
    ], (error, result) => error ? reject(error) : resolve(result))
  })
  assert.deepEqual(offsets, [
    { topic: 'events', partition: 0, offset: 4 },
    { topic: 'events', partition: 0, offset: 8 },
    { topic: 'events', partition: 0, offset: 4 }
  ])
  await new Promise<void>((resolve, reject) => client.disconnect(error => error ? reject(error) : resolve()))
  await new Promise<void>((resolve, reject) => client.connect({}, error => error ? reject(error) : resolve()))
  assert.equal(clients.length, 2)
  assert.equal(client.getClient(), clients[1])
})

test('recreates a client closed after a failed connection', async () => {
  const failed = new FakePlatformClient()
  failed.metadata = (_options, callback) => callback(new Error('connection failed'))
  const replacement = new FakePlatformClient()
  const client = new TestClient(failed, () => replacement)

  await new Promise<void>(resolve => client.connect({}, error => {
    assert.ok(error)
    resolve()
  }))
  await new Promise<void>((resolve, reject) => client.disconnect(error => error ? reject(error) : resolve()))
  await new Promise<void>((resolve, reject) => client.connect({}, error => error ? reject(error) : resolve()))

  assert.equal(failed.closed, true)
  assert.equal(client.getClient(), replacement)
})

test('queryWatermarkOffsets defaults an omitted timeout to 1000ms', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] })
  const platform = new FakePlatformClient()
  platform.listOffsets = () => {}
  const client = new TestClient(platform)
  await new Promise<void>((resolve, reject) => client.connect({}, error => error ? reject(error) : resolve()))
  let error: LibrdKafkaError | null = null

  client.queryWatermarkOffsets('events', 0, result => { error = result as LibrdKafkaError })
  t.mock.timers.tick(999)
  assert.equal(error, null)
  t.mock.timers.tick(1)
  assert.equal(error?.code, CODES.ERRORS.ERR__TIMED_OUT)
})

test('normalizes errors to the numeric facade', () => {
  const timeout = Object.assign(new Error('late'), { code: 'PLT_KFK_TIMEOUT', canRetry: true })
  const converted = toLibrdKafkaError(timeout)
  assert.ok(converted instanceof LibrdKafkaError)
  assert.equal(converted.code, CODES.ERRORS.ERR__TIMED_OUT)
  assert.equal(converted.errno, CODES.ERRORS.ERR__TIMED_OUT)
  assert.equal(converted.isRetriable, true)
})

test('caches consumer subscriptions, assignments, positions, and seeks', async () => {
  const consumer = new KafkaConsumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'test'
  })
  connectConsumer(consumer)
  const subscribed = once(consumer, 'subscribed')
  consumer.subscribe(['events', /^logs-/])
  await subscribed
  assert.deepEqual(consumer.subscription(), ['events', '/^logs-/'])

  consumer.assign([{ topic: 'events', partition: 0, offset: 4 }])
  assert.deepEqual(consumer.assignments(), [{ topic: 'events', partition: 0, offset: 4 }])
  assert.deepEqual(consumer.position(), [{ topic: 'events', partition: 0, offset: Topic.OFFSET_INVALID }])
  await new Promise<void>((resolve, reject) => {
    consumer.seek({ topic: 'events', partition: 0, offset: 8 }, 0, error => error ? reject(error) : resolve())
  })
  assert.deepEqual(consumer.position(), [{ topic: 'events', partition: 0, offset: 8 }])
  consumer.pause([{ topic: 'events', partition: 0 }])
  consumer.resume([{ topic: 'events', partition: 0 }])
  consumer.unassign()
  assert.deepEqual(consumer.assignments(), [])
})

test('eager rebalances revoke the complete previous assignment', () => {
  const consumer = new KafkaConsumer({ 'bootstrap.servers': 'localhost:9092', 'group.id': 'test', rebalance_cb: true })
  const events: Array<{ code: number; assignments: object[] }> = []
  consumer.on('rebalance', (error, assignments) => events.push({ code: error.code, assignments }))
  consumer.consumer.emit('consumer:group:join', { groupId: 'test', memberId: 'member', assignments: [{ topic: 'events', partitions: [0, 1] }] })
  consumer.consumer.emit('consumer:group:join', { groupId: 'test', memberId: 'member', assignments: [{ topic: 'events', partitions: [1] }] })
  assert.deepEqual(events[1], {
    code: CODES.ERRORS.ERR__REVOKE_PARTITIONS,
    assignments: [{ topic: 'events', partition: 0 }, { topic: 'events', partition: 1 }]
  })
})

test('rejects unsupported synchronous commits and accepts logical manual assignments', () => {
  const consumer = new KafkaConsumer({ 'group.id': 'test' })
  assert.throws(
    () => consumer.commitSync(null),
    (error: LibrdKafkaError) => error.code === CODES.ERRORS.ERR__NOT_IMPLEMENTED
  )
  assert.throws(
    () => consumer.commitMessageSync({ topic: 'events', partition: 0, offset: 1 }),
    (error: LibrdKafkaError) => error.code === CODES.ERRORS.ERR__NOT_IMPLEMENTED
  )
  consumer.assign([{ topic: 'events', partition: 0 }])
  assert.deepEqual(consumer.assignments(), [{ topic: 'events', partition: 0 }])
})

test('uses bounded and flowing consume semantics without advancing unread positions', async () => {
  const consumer = new KafkaConsumer({ 'group.id': 'test' }, { 'auto.offset.reset': 'earliest' })
  const source = createMessageSource()
  connectConsumer(consumer)
  let consumeOptions: Record<string, unknown> | undefined
  let commitOptions: Record<string, unknown> | undefined
  const platform = consumer.consumer as unknown as {
    consume (options: Record<string, unknown>): Promise<Readable & { close (): Promise<void> }>
    commit (options: Record<string, unknown>, callback: (error: Error | null) => void): void
  }
  platform.consume = async options => {
    consumeOptions = options
    return source
  }
  consumer.subscribe(['events'])
  platform.commit = (options, callback) => {
    commitOptions = options
    callback(null)
  }
  consumer.consumer.emit('consumer:group:join', {
    groupId: 'test',
    memberId: 'member',
    assignments: [{ topic: 'events', partitions: [0] }]
  })

  const batchPromise = new Promise<Message[]>((resolve, reject) => {
    consumer.consume(2, (error, messages) => error ? reject(error) : resolve(messages ?? []))
  })
  await new Promise(resolve => setImmediate(resolve))
  assert.deepEqual(consumer.position([{ topic: 'events', partition: 0 }]), [
    { topic: 'events', partition: 0, offset: Topic.OFFSET_INVALID }
  ])
  source.push(message(0))
  source.push(message(1))
  source.push(message(2))
  const batch = await batchPromise
  assert.deepEqual(batch.map(item => item.offset), [0, 1])
  assert.deepEqual(consumer.position([{ topic: 'events', partition: 0 }]), [
    { topic: 'events', partition: 0, offset: 2 }
  ])
  consumer.commit()
  assert.deepEqual(commitOptions, {
    offsets: [{ topic: 'events', partition: 0, offset: 2n, leaderEpoch: 0 }]
  })
  assert.equal(consumeOptions?.mode, 'committed')
  assert.equal(consumeOptions?.fallbackMode, 'earliest')
  assert.equal(consumeOptions?.offsets, undefined)

  const flowing = new Promise<Message>((resolve, reject) => {
    consumer.consume((error, item) => error || !item ? reject(error) : resolve(item))
  })
  assert.equal((await flowing).offset, 2)
  assert.deepEqual(consumer.position([{ topic: 'events', partition: 0 }]), [
    { topic: 'events', partition: 0, offset: 3 }
  ])
  consumer.unsubscribe()
})

test('maps subscription reset policy to committed fallback modes', async () => {
  for (const [reset, fallbackMode] of [['latest', 'latest'], ['error', 'fail']] as const) {
    const consumer = new KafkaConsumer({ 'group.id': 'test' }, { 'auto.offset.reset': reset })
    connectConsumer(consumer)
    const source = createMessageSource()
    let consumeOptions: Record<string, unknown> | undefined
    const platform = consumer.consumer as unknown as {
      consume (options: Record<string, unknown>): Promise<Readable & { close (): Promise<void> }>
    }
    platform.consume = async options => {
      consumeOptions = options
      return source
    }
    consumer.subscribe(['events'])
    consumer.consume(1)
    await new Promise(resolve => setImmediate(resolve))
    assert.equal(consumeOptions?.mode, 'committed')
    assert.equal(consumeOptions?.fallbackMode, fallbackMode)
    consumer.unsubscribe()
  }
})

test('manual assignments filter delivery and use their starting offsets', async () => {
  const consumer = new KafkaConsumer({ 'group.id': 'test' })
  connectConsumer(consumer)
  const source = createMessageSource()
  let consumeOptions: Record<string, unknown> | undefined
  const platform = consumer.consumer as unknown as {
    consume (options: Record<string, unknown>): Promise<Readable & { close (): Promise<void> }>
  }
  platform.consume = async options => {
    consumeOptions = options
    return source
  }
  consumer.assign([{ topic: 'events', partition: 1, offset: 5 }])
  consumer.consumer.emit('consumer:group:join', {
    groupId: 'test',
    memberId: 'member',
    assignments: [{ topic: 'events', partitions: [0] }]
  })
  const consumed = new Promise<Message[]>((resolve, reject) => {
    consumer.consume(1, (error, messages) => error ? reject(error) : resolve(messages ?? []))
  })
  await new Promise(resolve => setImmediate(resolve))
  source.push(message(5, 0))
  source.push(message(5, 1))
  assert.deepEqual((await consumed).map(item => item.partition), [1])
  assert.equal(consumeOptions?.mode, 'manual')
  assert.equal(consumeOptions?.autocommit, false)
  assert.deepEqual(consumeOptions?.offsets, [{ topic: 'events', partition: 1, offset: 5n }])
  assert.deepEqual(consumer.position([{ topic: 'events', partition: 0 }]), [
    { topic: 'events', partition: 0, offset: Topic.OFFSET_INVALID }
  ])
  consumer.unassign()
})

test('consumer stream performs bounded pulls and honors stream backpressure', async () => {
  const fake = new FakeConsumer([[message(0), message(1)], [message(2)]])
  const stream = new ConsumerStream(fake as unknown as KafkaConsumer, {
    topics: ['events'],
    fetchSize: 2,
    objectMode: true,
    highWaterMark: 1
  })
  await once(stream, 'readable')
  assert.equal(Number((stream.read() as { offset: number }).offset), 0)
  await once(stream, 'readable')
  assert.equal(Number((stream.read() as { offset: number }).offset), 1)
  await once(stream, 'readable')
  assert.equal(Number((stream.read() as { offset: number }).offset), 2)
  assert.ok(fake.requests.every(count => count === 2))
  stream.close()
})

test('failed delivery reports preserve message correlation and static factories exist', async () => {
  const producer = new FailingProducer()
  const reportPromise = once(producer, 'delivery-report')
  const opaque = { id: 42 }
  producer.failNext(new Error('delivery failed'))
  producer.produce('events', 2, Buffer.from('value'), 'key', 10, opaque)
  await new Promise(resolve => setImmediate(resolve))
  producer.poll()
  const [error, report] = await reportPromise
  assert.ok(error instanceof LibrdKafkaError)
  assert.deepEqual(report, {
    topic: 'events',
    partition: 2,
    offset: -1,
    size: 5,
    key: 'key',
    timestamp: 10,
    opaque
  })
  assert.equal(typeof Producer.createWriteStream, 'function')
  assert.equal(typeof KafkaConsumer.createReadStream, 'function')
})

function createMessageSource (): Readable & { close (): Promise<void> } {
  const source = new Readable({ objectMode: true, read () {} }) as Readable & { close (): Promise<void> }
  source.close = async () => {
    source.push(null)
  }
  return source
}

function message (offset: number, partition = 0): object {
  return {
    topic: 'events',
    partition,
    offset: BigInt(offset),
    value: Buffer.from(String(offset)),
    key: Buffer.from('key'),
    timestamp: 1n,
    headers: new Map(),
    metadata: {},
    commit () {},
    toJSON () { return {} }
  }
}

class FakeConsumer extends EventEmitter {
  requests: number[] = []
  #batches: object[][]

  constructor (batches: object[][]) {
    super()
    this.#batches = batches
  }

  connect (_options: unknown, callback: (error: Error | null, metadata?: object) => void): void {
    callback(null, { topics: [], brokers: [], orig_broker_id: 1, orig_broker_name: 'broker' })
  }

  subscribe (_topics: unknown[]): void {}

  consume (count: number, callback: (error: Error | null, messages?: object[]) => void): void {
    this.requests.push(count)
    const batch = this.#batches.shift()
    if (batch) {
      queueMicrotask(() => callback(null, batch))
    }
  }

  disconnect (callback: (error: Error | null) => void): void {
    callback(null)
  }

  setOauthBearerToken (_token: string): void {}
}

class FailingProducer extends Producer {
  constructor () {
    super({ 'bootstrap.servers': 'localhost:9092', dr_cb: true })
    this.connected = true
  }

  failNext (error: Error): void {
    const producer = this.producer as unknown as { send (): Promise<never> }
    producer.send = () => Promise.reject(error)
  }
}
