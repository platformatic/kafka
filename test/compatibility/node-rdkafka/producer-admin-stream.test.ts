import assert from 'node:assert/strict'
import { EventEmitter, once } from 'node:events'
import { test } from 'node:test'
import { Admin } from '../../../src/clients/admin/admin.ts'
import { kOptions } from '../../../src/clients/base/base.ts'
import { AdminClient } from '../../../src/compatibility/node-rdkafka/admin.ts'
import { HighLevelProducer, Producer } from '../../../src/compatibility/node-rdkafka/producer.ts'
import { ConsumerStream, ProducerStream } from '../../../src/compatibility/node-rdkafka/streams.ts'
import { CODES, type LibrdKafkaError } from '../../../src/compatibility/node-rdkafka/errors.ts'
import type { BaseOptions, ClusterMetadata } from '../../../src/clients/base/types.ts'
import type { KafkaConsumer } from '../../../src/compatibility/node-rdkafka/consumer.ts'

const metadata: ClusterMetadata = {
  id: 'cluster',
  controllerId: 1,
  lastUpdate: 0,
  brokers: new Map([[1, { host: 'localhost', port: 9092, rack: null }]]),
  topics: new Map()
}

test('producer queues reports until poll and flushes them in completion order', async () => {
  const producer = new ControlledProducer({ dr_cb: true })
  const reports: number[] = []
  producer.on('delivery-report', (_error, report) => reports.push(report.offset))

  const first = producer.nextSend()
  producer.produce('events', null, Buffer.from('first'))
  const second = producer.nextSend()
  producer.produce('events', null, Buffer.from('second'))
  second.resolve({ offsets: [{ partition: 0, offset: 2n }] })
  first.resolve({ offsets: [{ partition: 0, offset: 1n }] })
  await nextTurn()

  assert.deepEqual(reports, [])
  producer.poll()
  assert.deepEqual(reports, [2, 1])

  const third = producer.nextSend()
  producer.produce('events', null, Buffer.from('third'))
  const flushed = new Promise<void>((resolve, reject) => {
    producer.flush(100, error => error ? reject(error) : resolve())
  })
  third.resolve({ offsets: [{ partition: 0, offset: 3n }] })
  await flushed
  assert.deepEqual(reports, [2, 1, 3])
})

test('producer queue remains full until completed reports are polled', async () => {
  const producer = new ControlledProducer({ dr_cb: true, 'queue.buffering.max.messages': 1 })
  const send = producer.nextSend()
  producer.produce('events', null, Buffer.from('first'))
  assert.throws(
    () => producer.produce('events', null, Buffer.from('second')),
    (error: LibrdKafkaError) => error.code === CODES.ERRORS.ERR__QUEUE_FULL
  )

  send.resolve({ offsets: [{ partition: 0, offset: 1n }] })
  await nextTurn()
  assert.throws(
    () => producer.produce('events', null, Buffer.from('second')),
    (error: LibrdKafkaError) => error.code === CODES.ERRORS.ERR__QUEUE_FULL
  )
  producer.poll()

  const final = producer.nextSend()
  producer.produce('events', null, Buffer.from('second'))
  final.resolve({ offsets: [{ partition: 0, offset: 2n }] })
  await nextTurn()
})

test('producer uses the configured default partition for unassigned messages', async () => {
  const producer = new ControlledProducer({ partition: 2 })

  for (const partition of [null, undefined, -1]) {
    const send = producer.nextSend()
    producer.produce('events', partition, Buffer.from('value'))
    send.resolve({ offsets: [{ partition: 2, offset: 1n }] })
  }
  await nextTurn()

  assert.deepEqual(producer.partitions, [2, 2, 2])
})

test('high-level producer supports callback, synchronous, and Promise serializers', async () => {
  const producer = new ControlledHighLevelProducer()
  producer.setValueSerializer((value, callback) => callback(null, Buffer.from(`callback:${String(value)}`)))
  producer.setKeySerializer(key => Buffer.from(`sync:${String(key)}`))
  const first = producer.nextSend()
  const firstDelivery = produceHighLevel(producer, 'value', 'key')
  await nextTurn()
  assert.deepEqual(producer.sent[0], { value: Buffer.from('callback:value'), key: Buffer.from('sync:key') })
  first.resolve({ offsets: [{ partition: 0, offset: 4n }] })
  await nextTurn()
  producer.poll()
  assert.equal(await firstDelivery, 4)

  producer.setValueSerializer(async value => Buffer.from(`promise:${String(value)}`))
  const second = producer.nextSend()
  const secondDelivery = produceHighLevel(producer, 'next', null)
  await nextTurn()
  assert.deepEqual(producer.sent[1], { value: Buffer.from('promise:next'), key: Buffer.from('sync:null') })
  second.resolve({ offsets: [{ partition: 0, offset: 5n }] })
  await nextTurn()
  producer.poll()
  assert.equal(await secondDelivery, 5)
})

test('high-level producer annotates synchronous serializer throws with the input and serializer', async () => {
  const producer = new ControlledHighLevelProducer()
  const value = { event: 'created' }
  const serializer = (): Buffer => { throw new Error('sync failure') }
  producer.setValueSerializer(serializer)

  const error = await produceHighLevel(producer, value, null).then(
    () => assert.fail('serialization should fail'),
    error => error as Error & { value?: unknown; serializer?: unknown }
  )

  assert.equal(error.message, 'Could not serialize value: sync failure')
  assert.equal(error.value, value)
  assert.equal(error.serializer, serializer)
  assert.deepEqual(producer.sent, [])
})

test('high-level producer passes callback and Promise serializer errors through unchanged', async () => {
  const callbackError = new Error('callback failure')
  const promiseError = new Error('promise failure')
  const cases: Array<[(producer: ControlledHighLevelProducer) => void, Error]> = [
    [producer => producer.setValueSerializer((_value, callback) => callback(callbackError)), callbackError],
    [producer => producer.setValueSerializer(async () => { throw promiseError }), promiseError]
  ]

  for (const [configure, expected] of cases) {
    const producer = new ControlledHighLevelProducer()
    configure(producer)
    const error = await produceHighLevel(producer, 'value', null).then(
      () => assert.fail('serialization should fail'),
      error => error
    )
    assert.equal(error, expected)
    assert.deepEqual(producer.sent, [])
  }
})

test('OAuth tokens can be refreshed before producer and admin connect', async t => {
  const producer = new ConnectableProducer()
  const producerOptions = producer.options
  assert.equal(resolveOauthToken(producerOptions), '')
  producer.setOauthBearerToken('producer-token')
  assert.equal(resolveOauthToken(producerOptions), 'producer-token')
  await new Promise<void>((resolve, reject) => {
    producer.connect({}, error => error ? reject(error) : resolve())
  })

  const original = Admin.prototype.createTopics
  t.after(() => { Admin.prototype.createTopics = original })
  let adminToken: string | undefined
  Admin.prototype.createTopics = function (_options, callback): void {
    adminToken = resolveOauthToken(this[kOptions])
    callback(null, [])
  }
  const admin = AdminClient.create({
    'security.protocol': 'sasl_plaintext',
    'sasl.mechanisms': 'OAUTHBEARER'
  })
  admin.refreshOauthBearerToken('admin-token')
  await new Promise<void>((resolve, reject) => {
    admin.createTopic({ topic: 'events', num_partitions: 1, replication_factor: 1 }, error => error ? reject(error) : resolve())
  })
  assert.equal(adminToken, 'admin-token')
  admin.disconnect()
})

test('OAuth token refresh requires a non-empty string', () => {
  const producer = new ConnectableProducer()
  const admin = AdminClient.create({
    'security.protocol': 'sasl_plaintext',
    'sasl.mechanisms': 'OAUTHBEARER'
  })

  for (const token of ['', null, 1, {}]) {
    assert.throws(
      () => producer.setOauthBearerToken(token as unknown as string),
      (error: LibrdKafkaError) => error.code === CODES.ERRORS.ERR__INVALID_ARG
    )
    assert.throws(
      () => admin.refreshOauthBearerToken(token as unknown as string),
      (error: LibrdKafkaError) => error.code === CODES.ERRORS.ERR__INVALID_ARG
    )
  }
  admin.disconnect()
})

test('admin explicit timeout 0 uses the 5000ms default', t => {
  t.mock.method(Admin.prototype, 'deleteTopics', () => {})
  t.mock.timers.enable({ apis: ['setTimeout'] })
  const admin = AdminClient.create({})
  let error: LibrdKafkaError | null = null

  admin.deleteTopic('events', 0, result => { error = result as LibrdKafkaError })
  t.mock.timers.tick(4999)
  assert.equal(error, null)
  t.mock.timers.tick(1)
  assert.equal(error?.code, CODES.ERRORS.ERR__TIMED_OUT)
  admin.disconnect()
})

test('admin timeout callback is one-shot when the operation completes late', async t => {
  const original = Admin.prototype.deleteTopics
  t.after(() => { Admin.prototype.deleteTopics = original })
  let complete: ((error: Error | null) => void) | undefined
  Admin.prototype.deleteTopics = function (_options, callback): void {
    complete = callback
  }
  const admin = AdminClient.create({})
  const errors: Array<Error | null> = []
  admin.deleteTopic('events', 10, error => errors.push(error))
  await delay(25)
  assert.equal((errors[0] as LibrdKafkaError).code, CODES.ERRORS.ERR__TIMED_OUT)
  complete?.(null)
  await nextTurn()
  assert.equal(errors.length, 1)
  admin.disconnect()
})

test('admin forwards per-call deadlines to broker operations', t => {
  const timeouts: number[] = []
  t.mock.method(Admin.prototype, 'createTopics', (options, callback) => {
    timeouts.push(options.timeout!)
    callback(null, [])
  })
  t.mock.method(Admin.prototype, 'deleteTopics', (options, callback) => {
    timeouts.push(options.timeout!)
    callback(null)
  })
  t.mock.method(Admin.prototype, 'createPartitions', (options, callback) => {
    timeouts.push(options.timeout!)
    callback(null)
  })
  const admin = AdminClient.create({})

  admin.createTopic({ topic: 'events', num_partitions: 1, replication_factor: 1 }, 11, () => {})
  admin.deleteTopic('events', 12, () => {})
  admin.createPartitions('events', 2, 13, () => {})

  assert.deepEqual(timeouts, [11, 12, 13])
  admin.disconnect()
})

test('numeric consumer stream options wait between empty reads', async () => {
  const consumer = new FakeConsumer()
  const stream = new ConsumerStream(consumer as unknown as KafkaConsumer, 20)
  const [chunk] = await once(stream, 'data')
  assert.equal(chunk.offset, 1)
  assert.deepEqual(consumer.subscriptions, [[]])
  assert.ok(consumer.consumeTimes.length >= 2)
  assert.ok(consumer.consumeTimes[1] - consumer.consumeTimes[0] >= 15)
  const closed = once(stream, 'close')
  stream.destroy()
  await closed
})

test('consumer stream autoClose controls consumer disconnection', async () => {
  for (const [autoClose, expected] of [[true, 1], [false, 0]] as const) {
    const consumer = new FakeConsumer(false)
    const stream = new ConsumerStream(consumer as unknown as KafkaConsumer, { topics: [], autoClose })
    stream.destroy()
    await once(stream, 'close')
    assert.equal(consumer.disconnects, expected)
  }
})

test('consumer stream close is idempotent and disconnects once', async () => {
  const consumer = new FakeConsumer(false)
  const stream = new ConsumerStream(consumer as unknown as KafkaConsumer, { topics: [] })
  const closed = once(stream, 'close')
  const callbacks: number[] = []
  stream.close(() => callbacks.push(1))
  stream.close(() => callbacks.push(2))
  await closed
  assert.equal(consumer.disconnects, 1)
  assert.deepEqual(callbacks, [1, 2])
})

test('producer stream connects writes after ready and follows autoClose', async () => {
  for (const [autoClose, expected] of [[true, 1], [false, 0]] as const) {
    const producer = new FakeProducer()
    const stream = new ProducerStream(producer as unknown as Producer, { topic: 'events', autoClose })
    const written = new Promise<void>((resolve, reject) => {
      stream.write(Buffer.from('value'), error => error ? reject(error) : resolve())
    })
    assert.equal(producer.connects, 1)
    assert.deepEqual(producer.messages, [])
    producer.ready()
    await written
    assert.deepEqual(producer.messages, [Buffer.from('value')])
    stream.end()
    await once(stream, 'close')
    assert.equal(producer.disconnects, expected)
  }
})

test('producer stream disconnects cleanly after connection failure', async () => {
  const producer = new FailingConnectProducer()
  const stream = new ProducerStream(producer, { topic: 'events' })
  const errorEvent = once(stream, 'error')
  const closed = new Promise<void>(resolve => stream.once('close', resolve))
  const [error] = await errorEvent
  await closed
  assert.equal(error.message, 'connect failed')
  assert.equal(producer.disconnects, 1)
})

test('producer stream close destroys the stream and disconnects once', async () => {
  for (const autoClose of [true, false]) {
    const producer = new FakeProducer()
    const stream = new ProducerStream(producer as unknown as Producer, { topic: 'events', autoClose })
    const closed = once(stream, 'close')
    const callbacks: number[] = []
    stream.close(() => callbacks.push(1))
    stream.close(() => callbacks.push(2))
    await closed
    assert.equal(stream.destroyed, true)
    assert.equal(producer.disconnects, 1)
    assert.deepEqual(callbacks, [1, 2])
  }
})

test('producer stream destruction cancels queue-full retries', async () => {
  const producer = new QueueFullProducer()
  const stream = new ProducerStream(producer as unknown as Producer, { topic: 'events' })
  producer.ready()
  let callbackCalls = 0
  stream.write(Buffer.from('value'), () => callbackCalls++)
  assert.equal(producer.attempts, 1)
  stream.destroy()
  await once(stream, 'close')
  await delay(20)
  assert.equal(producer.attempts, 1)
  assert.equal(callbackCalls, 1)
})

test('producer stream destruction removes pending ready writes', async () => {
  const producer = new FakeProducer()
  const stream = new ProducerStream(producer as unknown as Producer, { topic: 'events' })
  let callbackCalls = 0
  stream.write(Buffer.from('value'), () => callbackCalls++)
  assert.equal(producer.listenerCount('ready'), 1)
  stream.destroy()
  await once(stream, 'close')
  producer.ready()
  assert.equal(producer.listenerCount('ready'), 0)
  assert.deepEqual(producer.messages, [])
  assert.equal(callbackCalls, 1)
})

class ControlledProducer extends Producer {
  readonly sent: Array<{ value: Buffer | null; key: Buffer | string | null | undefined }> = []
  readonly partitions: Array<number | undefined> = []
  readonly #sends: Array<PromiseWithResolvers<{ offsets?: { partition: number; offset: bigint }[] }>> = []

  constructor (config: ConstructorParameters<typeof Producer>[0] = {}) {
    super(config)
    this.connected = true
    const bridge = this.producer as unknown as {
      send (request: { messages: Array<{ value: Buffer | null; key: Buffer | string | null | undefined; partition?: number }> }): Promise<{ offsets?: { partition: number; offset: bigint }[] }>
      isConnected (): boolean
    }
    bridge.isConnected = () => true
    bridge.send = request => {
      this.sent.push({ value: request.messages[0].value, key: request.messages[0].key })
      this.partitions.push(request.messages[0].partition)
      const send = this.#sends.shift()
      assert.ok(send, 'nextSend() must be called before produce()')
      return send.promise
    }
  }

  nextSend (): PromiseWithResolvers<{ offsets?: { partition: number; offset: bigint }[] }> {
    const send = Promise.withResolvers<{ offsets?: { partition: number; offset: bigint }[] }>()
    this.#sends.push(send)
    return send
  }
}

class ControlledHighLevelProducer extends HighLevelProducer {
  readonly sent: Array<{ value: Buffer | null; key: Buffer | string | null | undefined }> = []
  readonly #sends: Array<PromiseWithResolvers<{ offsets?: { partition: number; offset: bigint }[] }>> = []

  constructor () {
    super({})
    this.setPollInterval(0)
    this.connected = true
    const bridge = this.producer as unknown as {
      send (request: { messages: Array<{ value: Buffer | null; key: Buffer | string | null | undefined }> }): Promise<{ offsets?: { partition: number; offset: bigint }[] }>
      isConnected (): boolean
    }
    bridge.isConnected = () => true
    bridge.send = request => {
      this.sent.push({ value: request.messages[0].value, key: request.messages[0].key })
      const send = this.#sends.shift()
      assert.ok(send, 'nextSend() must be called before produce()')
      return send.promise
    }
  }

  nextSend (): PromiseWithResolvers<{ offsets?: { partition: number; offset: bigint }[] }> {
    const send = Promise.withResolvers<{ offsets?: { partition: number; offset: bigint }[] }>()
    this.#sends.push(send)
    return send
  }
}

class ConnectableProducer extends Producer {
  constructor () {
    super({
      'security.protocol': 'sasl_plaintext',
      'sasl.mechanisms': 'OAUTHBEARER'
    })
    const bridge = this.producer as unknown as {
      metadata (options: unknown, callback: (error: Error | null, metadata?: ClusterMetadata) => void): void
      isConnected (): boolean
    }
    bridge.metadata = (_options, callback) => callback(null, metadata)
    bridge.isConnected = () => true
  }

  get options (): BaseOptions {
    return this.producer[kOptions]
  }
}

class FakeConsumer extends EventEmitter {
  readonly consumeTimes: number[] = []
  readonly subscriptions: unknown[][] = []
  disconnects = 0
  #returnMessage: boolean

  constructor (returnMessage = true) {
    super()
    this.#returnMessage = returnMessage
  }

  connect (_options: unknown, callback: (error: Error | null, metadata?: object) => void): void {
    callback(null, { topics: [], brokers: [], orig_broker_id: 1, orig_broker_name: 'broker' })
  }

  subscribe (topics: unknown[]): void {
    this.subscriptions.push(topics)
  }

  consume (_count: number, callback: (error: Error | null, messages?: object[]) => void): void {
    this.consumeTimes.push(Date.now())
    if (this.consumeTimes.length === 1 || !this.#returnMessage) {
      queueMicrotask(() => callback(null, []))
      return
    }
    this.#returnMessage = false
    queueMicrotask(() => callback(null, [{ topic: 'events', partition: 0, offset: 1, value: Buffer.from('value') }]))
  }

  disconnect (callback: (error: Error | null) => void): void {
    this.disconnects++
    callback(null)
  }
}

class FakeProducer extends EventEmitter {
  connects = 0
  disconnects = 0
  connected = false
  messages: Buffer[] = []

  setPollInterval (): void {}

  connect (): void {
    this.connects++
  }

  ready (): void {
    this.connected = true
    this.emit('ready')
  }

  isConnected (): boolean {
    return this.connected
  }

  produce (_topic: string, _partition: number | null, value: Buffer): void {
    this.messages.push(value)
  }

  poll (): void {}

  disconnect (callback: (error: Error | null) => void): void {
    this.disconnects++
    this.connected = false
    callback(null)
  }
}

class QueueFullProducer extends FakeProducer {
  attempts = 0

  override produce (): void {
    this.attempts++
    throw Object.assign(new Error('Queue full'), { code: CODES.ERRORS.ERR__QUEUE_FULL })
  }
}

class FailingConnectProducer extends Producer {
  disconnects = 0

  constructor () {
    super({})
    const bridge = this.producer as unknown as {
      metadata (options: unknown, callback: (error: Error | null, metadata?: ClusterMetadata) => void): void
      close (callback: (error: Error | null) => void): void
      isConnected (): boolean
    }
    bridge.metadata = (_options, callback) => queueMicrotask(() => callback(new Error('connect failed')))
    bridge.close = callback => {
      this.disconnects++
      callback(null)
    }
    bridge.isConnected = () => false
  }
}

function produceHighLevel (producer: ControlledHighLevelProducer, value: unknown, key: unknown): Promise<number | null | undefined> {
  return new Promise((resolve, reject) => {
    const result = producer.produce('events', null, value, key, null, (error, offset) => error ? reject(error) : resolve(offset))
    assert.equal(result, undefined)
  })
}

function resolveOauthToken (options: BaseOptions): string | undefined {
  const token = options.sasl?.token
  return typeof token === 'function' ? token() : token
}

function nextTurn (): Promise<void> {
  return new Promise(resolve => setImmediate(resolve))
}

function delay (milliseconds: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, milliseconds))
}
