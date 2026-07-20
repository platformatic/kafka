import assert from 'node:assert/strict'
import { EventEmitter } from 'node:events'
import { Readable } from 'node:stream'
import { test } from 'node:test'
import type { FetchResponsePartition } from '../../../src/apis/consumer/fetch-v17.ts'
import { kCreateConnectionPool } from '../../../src/clients/base/base.ts'
import { kCreateMessagesStream } from '../../../src/clients/consumer/consumer.ts'
import {
  kAssignmentsForOffsetRestore,
  kAssignmentsForTopic,
  kHandleFetchPartitionProgress,
  kResumeFetch,
  kSeekPartition
} from '../../../src/clients/consumer/messages-stream.ts'
import {
  kNodeRdkafkaAssignments,
  kNodeRdkafkaAssignmentFilter,
  kNodeRdkafkaPartitionEof,
  kNodeRdkafkaPausedPartitions,
  NodeRdkafkaConsumerBridge
} from '../../../src/compatibility/node-rdkafka/consumer-native.ts'
import { KafkaConsumer } from '../../../src/compatibility/node-rdkafka/consumer.ts'
import { CODES, type LibrdKafkaError } from '../../../src/compatibility/node-rdkafka/errors.ts'
import type { Message, TopicPartition, TopicPartitionOffset } from '../../../src/compatibility/node-rdkafka/types.ts'

test('disabled offset store commits only explicitly stored offsets', async () => {
  const consumer = new KafkaConsumer({
    'group.id': 'test',
    'enable.auto.commit': false,
    'enable.auto.offset.store': false
  })
  const source = new FakeMessagesStream()
  const commits: unknown[] = []
  const native = consumer.consumer as unknown as {
    consume (): Promise<FakeMessagesStream>
    commit (options: unknown, callback: (error: Error | null) => void): void
  }
  native.consume = async () => source
  connectNative(consumer)
  native.commit = (options, callback) => {
    commits.push(options)
    callback(null)
  }
  consumer.subscribe(['events'])
  consumer.consumer.emit('consumer:group:join', {
    groupId: 'test',
    memberId: 'member',
    assignments: [{ topic: 'events', partitions: [0] }]
  })

  const consumed = consumeOne(consumer)
  source.push(message(0))
  await consumed
  consumer.commit()
  consumer.offsetsStore([{ topic: 'events', partition: 0, offset: 7 }])
  consumer.commit()

  assert.deepEqual(commits, [
    { offsets: [] },
    { offsets: [{ topic: 'events', partition: 0, offset: 7n, leaderEpoch: 0 }] }
  ])
  consumer.unsubscribe()
})

test('seek invalidates messages buffered by a bounded consume request', async () => {
  const consumer = new KafkaConsumer({ 'group.id': 'test', 'enable.auto.commit': false })
  const source = new FakeMessagesStream()
  const native = consumer.consumer as unknown as { consume (): Promise<FakeMessagesStream> }
  native.consume = async () => source
  connectNative(consumer)
  consumer.assign([{ topic: 'events', partition: 0, offset: 0 }])

  const consumed = new Promise<Message[]>((resolve, reject) => {
    consumer.consume(2, (error, messages) => error ? reject(error) : resolve(messages ?? []))
  })
  source.push(message(0))
  await new Promise(resolve => setImmediate(resolve))
  await new Promise<void>((resolve, reject) => {
    consumer.seek({ topic: 'events', partition: 0, offset: 5 }, 0, error => error ? reject(error) : resolve())
  })
  source.push(message(5))
  source.push(message(6))

  assert.deepEqual((await consumed).map(item => item.offset), [5, 6])
  assert.deepEqual(source.seeks, [['events', 0, 5n, 1]])
  consumer.unassign()
})

test('consume zero selects flowing mode and positive counts validate callbacks synchronously', async () => {
  const consumer = new KafkaConsumer({ 'group.id': 'test', 'enable.auto.commit': false })
  const source = new FakeMessagesStream()
  const native = consumer.consumer as unknown as { consume (): Promise<FakeMessagesStream> }
  native.consume = async () => source
  connectNative(consumer)
  consumer.assign([{ topic: 'events', partition: 0, offset: 0 }])

  assert.throws(() => consumer.consume(1, null as never), TypeError)
  const consumed = new Promise<Message>((resolve, reject) => {
    consumer.consume(0, (error, item) => error || !item ? reject(error) : resolve(item))
  })
  source.push(message(0))

  assert.equal((await consumed).offset, 0)
  consumer.unassign()
})

test('committed enforces its deadline once and ignores late completion', t => {
  t.mock.timers.enable({ apis: ['setTimeout'] })
  const consumer = new KafkaConsumer({ 'group.id': 'test' })
  let complete: ((error: Error | null, offsets?: Map<string, bigint[]>) => void) | undefined
  const native = consumer.consumer as unknown as {
    listCommittedOffsets (options: unknown, callback: (error: Error | null, offsets?: Map<string, bigint[]>) => void): void
  }
  native.listCommittedOffsets = (_options, callback) => { complete = callback }
  let callbackCalls = 0
  let result: LibrdKafkaError | null = null

  consumer.committed([{ topic: 'events', partition: 0 }], 25, error => {
    callbackCalls++
    result = error as LibrdKafkaError
  })
  t.mock.timers.tick(24)
  assert.equal(callbackCalls, 0)
  t.mock.timers.tick(1)
  assert.equal(result?.code, CODES.ERRORS.ERR__TIMED_OUT)
  complete?.(null, new Map([['events', [4n]]]))
  assert.equal(callbackCalls, 1)
})

test('offsetsForTimes enforces supplied and default deadlines once', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] })
  const consumer = new KafkaConsumer({ 'group.id': 'test' })
  consumer.isConnected = () => true
  const completions: Array<(error: Error | null, offsets?: Map<string, Map<number, { offset: bigint }>>) => void> = []
  const native = consumer.consumer as unknown as {
    listOffsetsWithTimestamps (options: unknown, callback: (error: Error | null, offsets?: Map<string, Map<number, { offset: bigint }>>) => void): void
  }
  native.listOffsetsWithTimestamps = (_options, callback) => { completions.push(callback) }
  const errors: LibrdKafkaError[] = []

  consumer.offsetsForTimes([{ topic: 'events', partition: 0, offset: 1 }], 25, error => errors.push(error as LibrdKafkaError))
  consumer.offsetsForTimes([{ topic: 'events', partition: 0, offset: 1 }], error => errors.push(error as LibrdKafkaError))
  t.mock.timers.tick(25)
  assert.deepEqual(errors.map(error => error.code), [CODES.ERRORS.ERR__TIMED_OUT])
  t.mock.timers.tick(974)
  assert.equal(errors.length, 1)
  t.mock.timers.tick(1)
  assert.deepEqual(errors.map(error => error.code), [CODES.ERRORS.ERR__TIMED_OUT, CODES.ERRORS.ERR__TIMED_OUT])
  completions[0](null, new Map([['events', new Map([[0, { offset: 4n }]])]]))
  completions[1](null, new Map([['events', new Map([[0, { offset: 4n }]])]]))
  await Promise.resolve()
  assert.equal(errors.length, 2)
})

test('seek resolves beginning and end offsets before applying them', async () => {
  const consumer = new KafkaConsumer({ 'group.id': 'test', 'enable.auto.commit': false })
  const source = new FakeMessagesStream()
  const native = consumer.consumer as unknown as {
    consume (): Promise<FakeMessagesStream>
    listOffsets (options: { timestamp?: bigint }, callback: (error: Error | null, offsets?: Map<string, bigint[]>) => void): void
  }
  native.consume = async () => source
  connectNative(consumer)
  native.listOffsets = (options, callback) => {
    callback(null, new Map([['events', [options.timestamp === -2n ? 3n : 9n]]]))
  }
  consumer.assign([{ topic: 'events', partition: 0, offset: 0 }])
  consumer.consume()
  await new Promise(resolve => setImmediate(resolve))

  await seek(consumer, -2)
  await seek(consumer, -1)

  assert.deepEqual(source.seeks, [
    ['events', 0, 3n, 1],
    ['events', 0, 9n, 2]
  ])
  assert.deepEqual(consumer.position(), [{ topic: 'events', partition: 0, offset: 9 }])
  consumer.unassign()
})

test('seek reports a timeout once without applying a late resolution', t => {
  t.mock.timers.enable({ apis: ['setTimeout'] })
  const consumer = new KafkaConsumer({ 'group.id': 'test', 'enable.auto.commit': false })
  let complete: ((error: Error | null, offsets?: Map<string, bigint[]>) => void) | undefined
  const native = consumer.consumer as unknown as {
    listOffsets (options: unknown, callback: (error: Error | null, offsets?: Map<string, bigint[]>) => void): void
  }
  native.listOffsets = (_options, callback) => { complete = callback }
  consumer.assign([{ topic: 'events', partition: 0, offset: 0 }])
  let callbackCalls = 0
  let result: LibrdKafkaError | null = null

  consumer.seek({ topic: 'events', partition: 0, offset: -2 }, 25, error => {
    callbackCalls++
    result = error as LibrdKafkaError
  })
  t.mock.timers.tick(25)
  assert.equal(result?.code, CODES.ERRORS.ERR__TIMED_OUT)
  complete?.(null, new Map([['events', [3n]]]))
  assert.equal(callbackCalls, 1)
  assert.deepEqual(consumer.position(), [{ topic: 'events', partition: 0, offset: -1001 }])
})

test('resuming all paused assignments wakes the active fetch stream', async () => {
  const consumer = new KafkaConsumer({ 'group.id': 'test', 'enable.auto.commit': false })
  const source = new FakeMessagesStream()
  let consumeOptions: Record<PropertyKey, unknown> | undefined
  const native = consumer.consumer as unknown as {
    consume (options: Record<PropertyKey, unknown>): Promise<FakeMessagesStream>
  }
  native.consume = async options => {
    consumeOptions = options
    return source
  }
  connectNative(consumer)
  const assignments = [
    { topic: 'events', partition: 0, offset: 0 },
    { topic: 'events', partition: 1, offset: 0 }
  ]
  consumer.assign(assignments)
  consumer.pause(assignments)
  consumer.consume()
  await new Promise(resolve => setImmediate(resolve))

  assert.deepEqual(consumeOptions?.[kNodeRdkafkaPausedPartitions], new Set(['events:0', 'events:1']))
  consumer.resume(assignments)
  assert.deepEqual(consumeOptions?.[kNodeRdkafkaPausedPartitions], new Set())
  assert.equal(source.resumeCalls, 1)
  consumer.unassign()
})

test('manual assignments retain every partition for a topic', async () => {
  const native = new NodeRdkafkaConsumerBridge({
    clientId: 'compat-test',
    bootstrapBrokers: ['localhost:9092'],
    groupId: 'test'
  })
  native[kCreateConnectionPool] = () => new FakeConnectionPool() as never
  const stream = native[kCreateMessagesStream]({
    topics: ['events'],
    autocommit: false,
    [kNodeRdkafkaAssignments]: [
      { topic: 'events', partitions: [0] },
      { topic: 'events', partitions: [1] }
    ]
  })

  assert.deepEqual(stream[kAssignmentsForTopic]('events')?.partitions, [0, 1])
  assert.deepEqual(stream[kAssignmentsForOffsetRestore]('events')?.partitions, [0, 1])
  await stream.close()
})

test('native stream applies a dynamic callback-owned assignment filter', async () => {
  const native = new NodeRdkafkaConsumerBridge({
    clientId: 'compat-test',
    bootstrapBrokers: ['localhost:9092'],
    groupId: 'test'
  })
  native[kCreateConnectionPool] = () => new FakeConnectionPool() as never
  const filter = new Set<string>()
  const stream = native[kCreateMessagesStream]({
    topics: ['events'],
    autocommit: false,
    [kNodeRdkafkaAssignments]: [{ topic: 'events', partitions: [0, 1] }],
    [kNodeRdkafkaAssignmentFilter]: filter
  })

  assert.equal(stream[kAssignmentsForTopic]('events'), undefined)
  filter.add('events:1')
  assert.deepEqual(stream[kAssignmentsForTopic]('events'), { topic: 'events', partitions: [1] })
  assert.deepEqual(stream[kAssignmentsForOffsetRestore]('events'), { topic: 'events', partitions: [1] })
  await stream.close()
})

test('assignment replacement prunes paused partitions', async () => {
  const consumer = new KafkaConsumer({ 'group.id': 'test', 'enable.auto.commit': false })
  const source = new FakeMessagesStream()
  let consumeOptions: Record<PropertyKey, unknown> | undefined
  const native = consumer.consumer as unknown as {
    consume (options: Record<PropertyKey, unknown>): Promise<FakeMessagesStream>
  }
  native.consume = async options => {
    consumeOptions = options
    return source
  }
  connectNative(consumer)
  consumer.assign([{ topic: 'events', partition: 0, offset: 0 }])
  consumer.pause([{ topic: 'events', partition: 0 }])
  consumer.assign([{ topic: 'events', partition: 1, offset: 0 }])
  consumer.consume()
  await new Promise(resolve => setImmediate(resolve))

  assert.deepEqual(consumeOptions?.[kNodeRdkafkaPausedPartitions], new Set())
  consumer.unassign()
})

test('disconnect closes the stream and client after a final commit failure', async () => {
  const consumer = new KafkaConsumer({ 'group.id': 'test' })
  const source = new FakeMessagesStream()
  let clientClosed = false
  const native = consumer.consumer as unknown as {
    consume (): Promise<FakeMessagesStream>
    commit (options: unknown, callback: (error: Error | null) => void): void
    close (callback: (error: Error | null) => void): void
  }
  native.consume = async () => source
  connectNative(consumer)
  native.commit = (_options, callback) => callback(new Error('commit failed'))
  native.close = callback => {
    clientClosed = true
    callback(null)
  }
  consumer.assign([{ topic: 'events', partition: 0, offset: 0 }])
  const consumed = consumeOne(consumer)
  source.push(message(0))
  await consumed
  let callbackCalls = 0
  const error = await new Promise<Error | null | undefined>(resolve => {
    consumer.disconnect(error => {
      callbackCalls++
      resolve(error)
    })
  })

  assert.equal(error?.message, 'commit failed')
  assert.equal(callbackCalls, 1)
  assert.equal(source.closed, true)
  assert.equal(clientClosed, true)
})

test('committed reports unknown offsets as OFFSET_INVALID', async () => {
  const consumer = new KafkaConsumer({ 'group.id': 'test' })
  const native = consumer.consumer as unknown as {
    listCommittedOffsets (options: unknown, callback: (error: Error | null, offsets?: Map<string, bigint[]>) => void): void
  }
  native.listCommittedOffsets = (_options, callback) => callback(null, new Map([['events', [-1n]]]))

  const offsets = await new Promise<TopicPartitionOffset[]>((resolve, reject) => {
    consumer.committed([{ topic: 'events', partition: 0 }], 100, (error, offsets) => error ? reject(error) : resolve(offsets ?? []))
  })

  assert.deepEqual(offsets, [{ topic: 'events', partition: 0, offset: -1001 }])
})

test('native stream emits watermarks and de-duplicates partition EOF at an offset', async () => {
  const native = new NodeRdkafkaConsumerBridge({
    clientId: 'compat-test',
    bootstrapBrokers: ['localhost:9092'],
    groupId: 'test'
  })
  native[kCreateConnectionPool] = () => new FakeConnectionPool() as never
  const stream = native[kCreateMessagesStream]({
    topics: ['events'],
    autocommit: false,
    [kNodeRdkafkaPartitionEof]: true
  })
  const watermarks: unknown[] = []
  const eof: unknown[] = []
  stream.on('watermark', value => watermarks.push(value))
  stream.on('partition.eof', value => eof.push(value))
  const response = fetchPartition(3n, 9n)

  stream[kHandleFetchPartitionProgress]('events', 0, response, 8n, 0, 0)
  stream[kHandleFetchPartitionProgress]('events', 0, response, 8n, 0, 0)
  stream[kHandleFetchPartitionProgress]('events', 0, fetchPartition(4n, 10n), 9n, 0, 0)

  assert.deepEqual(watermarks, [
    { topic: 'events', partition: 0, lowOffset: 3, highOffset: 9 },
    { topic: 'events', partition: 0, lowOffset: 3, highOffset: 9 },
    { topic: 'events', partition: 0, lowOffset: 4, highOffset: 10 }
  ])
  assert.deepEqual(eof, [
    { topic: 'events', partition: 0, offset: 9 },
    { topic: 'events', partition: 0, offset: 10 }
  ])
  await stream.close()
})

test('regex resubscription refreshes metadata and recreates the stream', async () => {
  const consumer = new KafkaConsumer({ 'group.id': 'test', 'enable.auto.commit': false })
  const streams: FakeMessagesStream[] = []
  const consumedTopics: string[][] = []
  let metadataTopics = ['logs-a', 'logs-b', 'other']
  const native = consumer.consumer as unknown as {
    metadata (options: unknown, callback: (error: Error | null, metadata?: object) => void): void
    consume (options: { topics: string[] }): Promise<FakeMessagesStream>
  }
  native.metadata = (_options, callback) => callback(null, metadata(metadataTopics))
  connectNative(consumer)
  native.consume = async options => {
    consumedTopics.push(options.topics)
    const stream = new FakeMessagesStream()
    streams.push(stream)
    return stream
  }

  consumer.subscribe([/^logs-/g])
  consumer.consume()
  await new Promise(resolve => setImmediate(resolve))
  metadataTopics = ['logs-a', 'metrics-a', 'metrics-b']
  consumer.subscribe([/^metrics-/y])
  consumer.consume()
  await new Promise(resolve => setImmediate(resolve))

  assert.deepEqual(consumedTopics, [['logs-a', 'logs-b'], ['metrics-a', 'metrics-b']])
  assert.equal(streams[0].closed, true)
  assert.equal(streams[1].closed, false)
  consumer.unsubscribe()
})

test('function rebalance callbacks own assignments while boolean callbacks auto-assign', async () => {
  let shouldAssign = false
  const callbackEvents: Array<{ code: number, assignments: TopicPartition[] }> = []
  const ownedConsumer = new KafkaConsumer({
    'group.id': 'owned',
    'enable.auto.commit': false,
    rebalance_cb (this: KafkaConsumer, error, assignments) {
      callbackEvents.push({ code: error.code, assignments })
      if (error.code === CODES.ERRORS.ERR__ASSIGN_PARTITIONS && shouldAssign) {
        this.assign(assignments)
      }
    }
  })
  const ownedSource = new FakeMessagesStream()
  let ownedOptions: Record<PropertyKey, unknown> | undefined
  const ownedNative = ownedConsumer.consumer as unknown as {
    consume (options: Record<PropertyKey, unknown>): Promise<FakeMessagesStream>
  }
  ownedNative.consume = async options => {
    ownedOptions = options
    return ownedSource
  }
  connectNative(ownedConsumer)
  ownedConsumer.subscribe(['events'])
  ownedConsumer.consume(() => {})
  await new Promise(resolve => setImmediate(resolve))

  ownedConsumer.consumer.emit('consumer:group:join', groupJoin())
  assert.deepEqual(ownedConsumer.assignments(), [])
  assert.deepEqual(ownedOptions?.[kNodeRdkafkaAssignmentFilter], new Set())

  shouldAssign = true
  ownedConsumer.consumer.emit('consumer:group:join', groupJoin())
  assert.deepEqual(ownedConsumer.assignments(), [{ topic: 'events', partition: 0 }])
  assert.deepEqual(ownedOptions?.[kNodeRdkafkaAssignmentFilter], new Set(['events:0']))
  ownedConsumer.consumer.emit('consumer:group:join', groupJoin())
  assert.deepEqual(callbackEvents.slice(-2), [
    { code: CODES.ERRORS.ERR__REVOKE_PARTITIONS, assignments: [{ topic: 'events', partition: 0 }] },
    { code: CODES.ERRORS.ERR__ASSIGN_PARTITIONS, assignments: [{ topic: 'events', partition: 0 }] }
  ])
  ownedConsumer.unsubscribe()

  const automaticConsumer = new KafkaConsumer({ 'group.id': 'automatic', rebalance_cb: true, 'enable.auto.commit': false })
  connectNative(automaticConsumer)
  automaticConsumer.consumer.emit('consumer:group:join', groupJoin())
  assert.deepEqual(automaticConsumer.assignments(), [{ topic: 'events', partition: 0 }])
})

test('native-handle consumer operations enforce node-rdkafka disconnected state', async () => {
  const consumer = new KafkaConsumer({ 'group.id': 'test' })

  assert.throws(() => consumer.commit(), /KafkaConsumer is disconnected/)
  assert.throws(() => consumer.offsetsStore([]), /Client is disconnected/)
  assert.throws(() => consumer.pause([]), /Client is disconnected/)
  assert.throws(() => consumer.resume([]), /Client is disconnected/)
  assert.throws(() => consumer.getWatermarkOffsets('events', 0), /Client is disconnected/)
  assert.throws(() => consumer.subscribe(['events']), (error: LibrdKafkaError) => error.code === CODES.ERRORS.ERR__STATE)
  assert.throws(() => consumer.consume(), /Connect must be called before consume/)

  const error = await new Promise<LibrdKafkaError>(resolve => {
    consumer.consume(1, error => resolve(error as LibrdKafkaError))
  })
  assert.equal(error.code, CODES.ERRORS.ERR__STATE)
})

test('concurrent bounded consumes do not block later requests behind the first count', async () => {
  const consumer = new KafkaConsumer({ 'group.id': 'test', 'enable.auto.commit': false })
  const source = new FakeMessagesStream()
  const native = consumer.consumer as unknown as { consume (): Promise<FakeMessagesStream> }
  native.consume = async () => source
  connectNative(consumer)
  consumer.assign([{ topic: 'events', partition: 0, offset: 0 }])
  consumer.setDefaultConsumeTimeout(10)

  const first = consumeMany(consumer, 2)
  const second = consumeMany(consumer, 2)
  await new Promise(resolve => setImmediate(resolve))
  source.push(message(0))
  source.push(message(1))
  const [firstMessages, secondMessages] = await Promise.all([first, second])

  assert.deepEqual(firstMessages.map(item => item.offset), [0])
  assert.deepEqual(secondMessages.map(item => item.offset), [1])
  consumer.unassign()
})

class FakeMessagesStream extends Readable {
  closed = false
  resumeCalls = 0
  seeks: Array<[string, number, bigint, number]> = []

  constructor () {
    super({ objectMode: true, read () {} })
  }

  [kResumeFetch] (): void {
    this.resumeCalls++
  }

  [kSeekPartition] (topic: string, partition: number, offset: bigint, seekVersion: number): void {
    this.seeks.push([topic, partition, offset, seekVersion])
  }

  async close (): Promise<void> {
    this.closed = true
    this.push(null)
  }
}

class FakeConnectionPool extends EventEmitter {
  close (callback: (error: Error | null) => void): void {
    callback(null)
  }
}

function consumeOne (consumer: KafkaConsumer): Promise<Message> {
  return new Promise((resolve, reject) => {
    consumer.consume(1, (error, messages) => error || !messages?.[0] ? reject(error) : resolve(messages[0]))
  })
}

function consumeMany (consumer: KafkaConsumer, count: number): Promise<Message[]> {
  return new Promise((resolve, reject) => {
    consumer.consume(count, (error, messages) => error ? reject(error) : resolve(messages ?? []))
  })
}

function connectNative (consumer: KafkaConsumer): void {
  const state = consumer as unknown as { connected: boolean }
  const native = consumer.consumer as unknown as { isConnected: () => boolean }
  state.connected = true
  native.isConnected = () => true
}

function groupJoin (): object {
  return {
    groupId: 'test',
    memberId: 'member',
    assignments: [{ topic: 'events', partitions: [0] }]
  }
}

function seek (consumer: KafkaConsumer, offset: number): Promise<void> {
  return new Promise((resolve, reject) => {
    consumer.seek({ topic: 'events', partition: 0, offset }, 100, error => error ? reject(error) : resolve())
  })
}

function message (offset: number): object {
  return {
    topic: 'events',
    partition: 0,
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

function fetchPartition (lowOffset: bigint, highWatermark: bigint): FetchResponsePartition {
  return {
    partitionIndex: 0,
    errorCode: 0,
    highWatermark,
    lastStableOffset: highWatermark,
    logStartOffset: lowOffset,
    abortedTransactions: [],
    preferredReadReplica: -1
  }
}

function metadata (topics: string[]): object {
  return {
    id: 'cluster',
    controllerId: 1,
    lastUpdate: 0,
    brokers: new Map([[1, { host: 'localhost', port: 9092, rack: null }]]),
    topics: new Map(topics.map(name => [name, {
      id: `${name}-id`,
      partitionsCount: 1,
      lastUpdate: 0,
      partitions: [{ leader: 1, leaderEpoch: 0, replicas: [1], isr: [1], offlineReplicas: [] }]
    }]))
  }
}
