import { deepStrictEqual, rejects, strictEqual } from 'node:assert'
import { EventEmitter, once } from 'node:events'
import { test, type TestContext } from 'node:test'
import { setImmediate, setTimeout } from 'node:timers/promises'
import * as Prometheus from 'prom-client'
import type { FetchResponse, FetchResponsePartition } from '../../../src/apis/consumer/fetch-v17.ts'
import type { Callback } from '../../../src/apis/definitions.ts'
import { kCreateConnectionPool, kPrometheus } from '../../../src/clients/base/base.ts'
import type { Metrics } from '../../../src/clients/metrics.ts'
import {
  consumerReceivesChannel,
  defaultBatchSize,
  DeserializationErrorActions,
  IS_CONTROL,
  MessagesStream,
  MessagesStreamFallbackModes,
  MessagesStreamModes,
  noopCallback,
  UserError,
  type CommitOptions,
  type ConsumeOptions,
  type Consumer,
  type FetchOptions,
  type GroupAssignment,
  type KafkaRecord,
  type MessagesBatch,
  type RecordsBatch
} from '../../../src/index.ts'
import { kGetFetchNode } from '../../../src/symbols.ts'
import { createConsumer, createProducer, createTopic } from '../../helpers.ts'

const topic = 'test-topic'
const topicId = 'test-topic-id'

type BatchStream = MessagesStream<Buffer, Buffer, Buffer, Buffer, MessagesBatch>
type TestConsumeOptions = ConsumeOptions<Buffer, Buffer, Buffer, Buffer>
type TestFetchOptions = FetchOptions<Buffer, Buffer, Buffer, Buffer>
interface ReceiveTrace {
  raw: KafkaRecord
}

function recordBatch (firstOffset: number, count: number, attributes = 0, partitionLeaderEpoch = 4): RecordsBatch {
  return {
    firstOffset: BigInt(firstOffset),
    length: 0,
    partitionLeaderEpoch,
    magic: 2,
    crc: 0,
    attributes,
    lastOffsetDelta: count - 1,
    firstTimestamp: 10n,
    maxTimestamp: 10n,
    producerId: -1n,
    producerEpoch: -1,
    firstSequence: -1,
    records: Array.from({ length: count }, (_, offsetDelta) => ({
      length: 0,
      attributes: 0,
      offsetDelta,
      timestampDelta: 0n,
      key: null,
      value: Buffer.from(String(firstOffset + offsetDelta)),
      headers: []
    }))
  }
}

function partitionResponse (partitionIndex: number, ...records: RecordsBatch[]): FetchResponsePartition {
  return {
    partitionIndex,
    errorCode: 0,
    highWatermark: 0n,
    lastStableOffset: 0n,
    logStartOffset: 0n,
    abortedTransactions: null,
    preferredReadReplica: -1,
    records
  }
}

class FakeConsumer extends EventEmitter {
  [kPrometheus]: Metrics | undefined
  assignments: GroupAssignment[]
  topics: Pick<Consumer['topics'], 'current'>
  groupId: string
  memberId: string
  generationId: number
  coordinatorId: number
  leaders: number[]
  committedOffsets: bigint[]
  commits: CommitOptions['offsets']
  fetches: TestFetchOptions[]
  responses: FetchResponsePartition[][]

  constructor () {
    super()
    this.assignments = [{ topic, partitions: [0, 1] }]
    this.topics = { current: [topic] }
    this.groupId = 'test-group'
    this.memberId = 'test-member'
    this.generationId = 1
    this.coordinatorId = 1
    this.leaders = [1, 1]
    this.committedOffsets = [0n, 0n]
    this.commits = []
    this.fetches = []
    this.responses = []
    this[kPrometheus] = undefined
  }

  [kGetFetchNode] (_metadata: unknown, _topic: string, partition: number) {
    return this.leaders[partition]
  }

  [kCreateConnectionPool] () {
    return {
      close (callback: Callback<void>) {
        callback(null)
      }
    }
  }

  metadata (_options: unknown, callback: Callback<unknown>) {
    const partitions = this.leaders.map(leader => ({ leader, leaderEpoch: 7 }))
    callback(null, { topics: new Map([[topic, { id: topicId, partitions }]]) })
  }

  listOffsets (_options: unknown, callback: Callback<unknown>) {
    callback(null, new Map([[topic, [0n, 0n]]]))
  }

  listCommittedOffsets (_options: unknown, callback: Callback<unknown>) {
    callback(null, new Map([[topic, this.committedOffsets]]))
  }

  fetch (options: TestFetchOptions, callback: Callback<FetchResponse>) {
    this.fetches.push(options)
    const partitions = this.responses.shift()
    const delay = partitions ? setImmediate() : setTimeout(5)
    delay.then(() =>
      callback(null, {
        throttleTimeMs: 0,
        errorCode: 0,
        sessionId: 0,
        responses: [{ topicId, partitions: partitions ?? [] }]
      }))
  }

  isActive () {
    return true
  }

  commit ({ offsets }: CommitOptions, callback: Callback<void>) {
    this.commits.push(...offsets)
    callback(null)
  }
}

function createStream (t: TestContext, fake: FakeConsumer, overrides: Partial<TestConsumeOptions> = {}): BatchStream {
  const stream: BatchStream = new MessagesStream(fake as unknown as Consumer, {
    topics: [topic],
    mode: MessagesStreamModes.EARLIEST,
    autocommit: false,
    batch: 2,
    ...overrides
  })
  t.after(() => {
    stream.destroy()
  })
  return stream
}

function readBatches (stream: BatchStream, count: number): Promise<MessagesBatch[]> {
  const { promise, resolve, reject } = Promise.withResolvers<MessagesBatch[]>()
  const batches: MessagesBatch[] = []
  const onData = (batch: MessagesBatch) => {
    batches.push(batch)
    if (batches.length === count) {
      stream.pause()
      stream.removeListener('data', onData)
      stream.removeListener('error', reject)
      resolve(batches)
    }
  }
  stream.once('error', reject)
  stream.on('data', onData)
  stream.resume()
  return promise
}

for (const batch of [100, true] as const) {
  test(`batch: ${batch} splits a partition across wire record batches`, async t => {
    const fake = new FakeConsumer()
    fake.responses.push([
      partitionResponse(0, recordBatch(0, 75), recordBatch(75, 175, 0, 5)),
      partitionResponse(1, recordBatch(0, 2))
    ])
    const stream = createStream(t, fake, { batch, maxFetches: 1 })
    const batches = await Array.fromAsync(stream)
    deepStrictEqual(
      batches.map(b => [b.partition, b.messages.length, b.firstOffset, b.lastOffset, b.leaderEpoch]),
      [
        [0, defaultBatchSize, 0n, 99n, 5],
        [0, 100, 100n, 199n, 5],
        [0, 50, 200n, 249n, 5],
        [1, 2, 0n, 1n, 4]
      ]
    )
    for (const batch of batches) {
      strictEqual(batch.topic, topic)
      for (const message of batch.messages) {
        strictEqual(message.partition, batch.partition)
        strictEqual(message.commit, noopCallback)
      }
    }
    strictEqual(fake.fetches.length, 1)
  })
}

test('batch mode converts the message high-water mark unless maxFetches overrides it', t => {
  const fake = new FakeConsumer()
  for (const [options, expected] of [
    [{ batch: 100 }, 11],
    [{ batch: true, highWaterMark: 50 }, 1],
    [{ batch: 7, highWaterMark: 50 }, 8],
    [{ batch: 10, maxFetches: 3 }, 3],
    [{ batch: undefined, maxFetches: 3 }, 3],
    [{ batch: false }, 1024]
  ] as const) {
    strictEqual(createStream(t, fake, options).readableHighWaterMark, expected)
  }
})

test('batch commit uses the next delivered offset or the end of the fetch', async t => {
  const fake = new FakeConsumer()
  fake.responses.push([partitionResponse(0, recordBatch(0, 5), recordBatch(5, 1, IS_CONTROL))])
  const stream = createStream(t, fake, { maxFetches: 1 })
  const batches = await Array.fromAsync(stream)
  await batches[0].messages[0].commit()
  deepStrictEqual(fake.commits, [])
  for (const batch of batches) {
    await batch.commit()
  }
  deepStrictEqual(
    fake.commits,
    [2n, 4n, 6n].map(offset => ({ topic, partition: 0, offset, leaderEpoch: 7 }))
  )
  strictEqual(batches[2].lastOffset, 4n)
})

test('batch autocommit advances past control records and batch.commit is a no-op', async t => {
  const fake = new FakeConsumer()
  fake.responses.push([
    partitionResponse(0, recordBatch(0, 2), recordBatch(2, 1, IS_CONTROL)),
    partitionResponse(1, recordBatch(0, 1, IS_CONTROL))
  ])
  const stream = createStream(t, fake, { autocommit: true, maxFetches: 1 })
  const batches = await Array.fromAsync(stream)
  strictEqual(batches.length, 1)
  strictEqual(batches[0].commit, noopCallback)
  strictEqual(batches[0].messages[0].commit, noopCallback)
  deepStrictEqual(
    fake.commits.map(commit => [commit.partition, commit.offset]),
    [[0, 3n], [1, 1n]]
  )
})

test('destroying a batch stream does not autocommit an unfinished partition', async t => {
  const fake = new FakeConsumer()
  fake.responses.push([partitionResponse(0, recordBatch(0, 5))])
  const stream = createStream(t, fake, { autocommit: true })
  const closed = once(stream, 'close')
  stream.once('data', () => stream.destroy())
  await closed
  deepStrictEqual(fake.commits, [])
})

for (const action of [DeserializationErrorActions.SKIP, DeserializationErrorActions.CONTINUE]) {
  test(`${action} excludes failed batch records but keeps their error context commit`, async t => {
    const fake = new FakeConsumer()
    fake.responses.push([partitionResponse(0, recordBatch(0, 4))])
    const committed = Promise.withResolvers<void>()
    const stream = createStream(t, fake, {
      maxFetches: 1,
      deserializers: {
        value (value) {
          if (value?.toString() === '2') {
            throw Error('Invalid test record')
          }
          return value
        }
      },
      onDeserializationError (context) {
        strictEqual(context.offset, 2n)
        committed.resolve(context.commit())
        // The error handler can commit before the preceding batch is pushed.
        deepStrictEqual(fake.commits.map(commit => commit.offset), [3n])
        return action
      }
    })
    const [first, last] = await Array.fromAsync(stream)
    deepStrictEqual(
      [first, last].map(b => b.messages.map(m => m.offset)),
      [[0n, 1n], [3n]]
    )
    await committed.promise
  })
}

test('diagnostics wrap the batch push and the metrics count individual records', async t => {
  const fake = new FakeConsumer()
  const registry = new Prometheus.Registry()
  fake[kPrometheus] = { registry, client: Prometheus }
  const events: string[] = []
  for (const phase of ['start', 'end', 'asyncStart', 'asyncEnd'] as const) {
    const subscriber = (context: unknown) => {
      events.push(`${phase}:${(context as ReceiveTrace).raw.offsetDelta}`)
    }
    consumerReceivesChannel[phase].subscribe(subscriber)
    t.after(() => consumerReceivesChannel[phase].unsubscribe(subscriber))
  }

  fake.responses.push([partitionResponse(0, recordBatch(0, 3))])
  const stream = createStream(t, fake, { maxFetches: 1 })
  stream.on('data', batch => events.push(`push:${batch.firstOffset}`))
  await once(stream, 'end')
  deepStrictEqual(events, [
    'start:0',
    'end:0',
    'start:1',
    'end:1',
    'start:2',
    'asyncStart:0',
    'asyncStart:1',
    'push:0',
    'asyncEnd:0',
    'asyncEnd:1',
    'end:2',
    'asyncStart:2',
    'push:2',
    'asyncEnd:2'
  ])
  const metric = registry.getSingleMetric('kafka_consumed_messages')!
  strictEqual((await metric.get()).values[0].value, 3)
})

test('the before-deserialization hook enriches batch messages and toJSON converts offsets', async t => {
  const fake = new FakeConsumer()
  fake.responses.push([partitionResponse(0, recordBatch(0, 1))])
  const stream = createStream(t, fake, {
    maxFetches: 1,
    async beforeDeserialization (_data, _type, message) {
      message.metadata = { fromHook: true }
    }
  })
  const [batch] = await Array.fromAsync(stream)
  strictEqual(batch.messages[0].metadata.fromHook, true)
  deepStrictEqual(batch.toJSON(), {
    topic,
    partition: 0,
    firstOffset: '0',
    lastOffset: '0',
    leaderEpoch: 4,
    messages: batch.messages.map(message => message.toJSON())
  })
  strictEqual(JSON.parse(JSON.stringify(batch)).messages[0].offset, '0')
})

test('batch mode reports a thrown data listener for every message in the batch', async t => {
  const fake = new FakeConsumer()
  fake.responses.push([partitionResponse(0, recordBatch(0, 3))])
  const cause = new Error('data listener failed')
  const errors: unknown[] = []
  const subscriber = (context: unknown) => {
    errors.push((context as ReceiveTrace & { error: unknown }).error)
  }
  consumerReceivesChannel.error.subscribe(subscriber)
  t.after(() => consumerReceivesChannel.error.unsubscribe(subscriber))

  const stream = createStream(t, fake)
  stream.once('data', () => {
    throw cause
  })
  const [error] = await once(stream, 'error')
  strictEqual(error.code, UserError.code)
  strictEqual(error.cause, cause)
  deepStrictEqual(errors, [cause, cause])
})

test('batch consumption does not fetch while paused and resumes at the next offset', async t => {
  const fake = new FakeConsumer()
  fake.responses.push([partitionResponse(0, recordBatch(0, 1))], [partitionResponse(0, recordBatch(1, 1))])
  const stream = createStream(t, fake)
  strictEqual((await readBatches(stream, 1))[0].firstOffset, 0n)
  await setImmediate()
  strictEqual(fake.fetches.length, 1)
  strictEqual((await readBatches(stream, 1))[0].firstOffset, 1n)
})

test('batch consumption restores committed offsets and metadata after a rebalance', async t => {
  const fake = new FakeConsumer()
  fake.responses.push([partitionResponse(0, recordBatch(0, 4))])
  const stream = createStream(t, fake, {
    mode: MessagesStreamModes.COMMITTED,
    fallbackMode: MessagesStreamFallbackModes.EARLIEST
  })
  const [first] = await readBatches(stream, 1)
  strictEqual(first.firstOffset, 0n)
  deepStrictEqual(first.messages[0].metadata.consumer, {
    groupId: 'test-group',
    memberId: 'test-member',
    generationId: 1,
    coordinatorId: 1
  })

  // Drain the buffered data from the first fetch before simulating a rejoin.
  strictEqual((await readBatches(stream, 1))[0].firstOffset, 2n)
  await setImmediate()
  fake.generationId = 2
  fake.committedOffsets[0] = 5n
  fake.responses.push([partitionResponse(0, recordBatch(4, 3))])
  const offsets = once(stream, 'offsets')
  fake.emit('consumer:group:join')
  await offsets
  const [next] = await readBatches(stream, 1)
  strictEqual(next.firstOffset, 5n)
  deepStrictEqual(next.messages[0].metadata.consumer, {
    groupId: 'test-group',
    memberId: 'test-member',
    generationId: 2,
    coordinatorId: 1
  })
})

test('maxFetches ignores responses from other brokers after EOF', async t => {
  const fake = new FakeConsumer()
  fake.leaders[1] = 2
  fake.responses.push([partitionResponse(0, recordBatch(0, 4))], [partitionResponse(1, recordBatch(0, 4))])
  const stream = createStream(t, fake, { maxFetches: 1 })
  const batches: MessagesBatch[] = []
  stream.on('data', batch => batches.push(batch))
  stream.once('data', () => stream.pause())
  const firstFetch = once(stream, 'fetch')
  await firstFetch
  await once(stream, 'fetch')
  const ended = once(stream, 'end')
  stream.resume()
  await ended
  deepStrictEqual(
    batches.map(b => [b.partition, b.firstOffset]),
    [
      [0, 0n],
      [0, 2n]
    ]
  )
  strictEqual(fake.fetches.length, 2)
})

test('strict consumption rejects invalid batch sizes', async t => {
  const consumer = createConsumer(t, { strict: true })
  for (const batch of [0, -1, 1.5, NaN, Infinity]) {
    await rejects(consumer.consume({ topics: [topic], batch }), { code: UserError.code, message: /\/options\/batch/ })
  }
})

for (const autocommit of [true, false]) {
  test(`broker: batch consumption in COMMITTED mode with autocommit ${autocommit}`, { timeout: 15000 }, async t => {
    const topic = await createTopic(t, true)
    const producer = createProducer(t)
    await producer.send({ messages: Array.from({ length: 5 }, (_, i) => ({ topic, value: Buffer.from(String(i)) })) })
    const consumer = createConsumer(t, { strict: true })
    const options = {
      topics: [topic],
      mode: MessagesStreamModes.COMMITTED,
      fallbackMode: MessagesStreamFallbackModes.EARLIEST,
      maxWaitTime: 100,
      batch: 2,
      autocommit
    }
    const stream = await consumer.consume(options)
    const committed = autocommit ? once(stream, 'autocommit') : undefined
    const [first] = await readBatches(stream, 1)
    deepStrictEqual(
      first.messages.map(message => message.offset),
      [0n, 1n]
    )
    if (committed) {
      const [error] = await committed
      strictEqual(error, null)
    } else {
      await first.commit()
    }
    await stream.close()

    const offsets = await consumer.listCommittedOffsets({ topics: [{ topic, partitions: [0] }] })
    strictEqual(offsets.get(topic)?.[0], autocommit ? 5n : 2n)

    // Use the callback overload as well: its data listener must receive a batch.
    if (!autocommit) {
      const nextOffset = await new Promise<bigint>((resolve, reject) => {
        consumer.consume(options, (error, next) => {
          if (error) {
            reject(error)
            return
          }
          next!.once('error', reject)
          next!.once('data', batch => {
            next!.pause()
            resolve(batch.firstOffset)
          })
        })
      })
      strictEqual(nextOffset, 2n)
    }
  })
}
