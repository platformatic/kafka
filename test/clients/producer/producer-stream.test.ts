import { deepStrictEqual, ok, strictEqual } from 'node:assert'
import { once } from 'node:events'
import { Readable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { test } from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import {
  instancesChannel,
  MessagesStreamModes,
  ProducerStream,
  ProducerStreamReportModes,
  stringDeserializers,
  stringSerializers,
  type Callback,
  type Consumer,
  type Message,
  type ProduceResult,
  type SendOptions
} from '../../../src/index.ts'
import {
  createConsumer,
  createCreationChannelVerifier,
  createProducer,
  createTestGroupId,
  createTopic
} from '../../helpers.ts'

async function consumeN (consumer: Consumer<string, string, string, string>, topic: string, n: number) {
  const stream = await consumer.consume({
    topics: [topic],
    mode: MessagesStreamModes.EARLIEST,
    maxWaitTime: 1000,
    maxFetches: 1
  })

  const messages: Message<string, string, string, string>[] = []
  for await (const message of stream) {
    messages.push(message)
    if (messages.length >= n) {
      break
    }
  }

  await stream.close()

  return messages
}

test('asStream should create a ProducerStream and publish creation event', async t => {
  const created = createCreationChannelVerifier(
    instancesChannel,
    (data: { type: string }) => data.type === 'producer-stream'
  )

  const producer = createProducer<string, string, string, string>(t, { serializers: stringSerializers })

  const stream = producer.asStream()

  ok(stream instanceof ProducerStream)

  deepStrictEqual(created(), { type: 'producer-stream', instance: stream })

  await stream.close()
})

test('asStream should expose the underlying producer', async t => {
  const producer = createProducer<string, string, string, string>(t, { serializers: stringSerializers })
  const sink = producer.asStream()

  strictEqual(sink.producer, producer)

  await sink.close()
})

test('asStream should flush messages when batchSize is reached', async t => {
  const topic = await createTopic(t, true)
  const producer = createProducer<string, string, string, string>(t, { serializers: stringSerializers })

  const sink = producer.asStream({
    batchSize: 2,
    batchTime: 1000
  })

  await pipeline(
    Readable.from(
      [
        { topic, key: 'k1', value: 'v1' },
        { topic, key: 'k2', value: 'v2' },
        { topic, key: 'k3', value: 'v3' }
      ],
      { objectMode: true }
    ),
    sink
  )

  const consumer = createConsumer<string, string, string, string>(t, {
    groupId: createTestGroupId(),
    deserializers: stringDeserializers
  })

  const messages = await consumeN(consumer, topic, 3)

  strictEqual(messages.length, 3)
  deepStrictEqual(
    messages.map(m => m.value),
    ['v1', 'v2', 'v3']
  )
})

test('asStream should flush by timer and emit per-batch reports', async t => {
  const topic = await createTopic(t, true)
  const producer = createProducer<string, string, string, string>(t, { serializers: stringSerializers })

  const sink = producer.asStream({
    batchSize: 100,
    batchTime: 20,
    reportMode: ProducerStreamReportModes.BATCH
  })

  sink.write({ topic, key: 'timer', value: 'flush' })

  const [report] = await once(sink, 'delivery-report')

  strictEqual(report.count, 1)
  ok(report.result)

  await sink.close()

  const consumer = createConsumer<string, string, string, string>(t, {
    groupId: createTestGroupId(),
    deserializers: stringDeserializers
  })

  const messages = await consumeN(consumer, topic, 1)
  strictEqual(messages[0].value, 'flush')
})

test('asStream should emit per-message reports', async t => {
  const producer = createProducer<string, string, string, string>(t, { serializers: stringSerializers })
  const sink = producer.asStream({
    batchSize: 2,
    batchTime: 0,
    reportMode: ProducerStreamReportModes.MESSAGE
  })

  t.mock.method(producer, 'send', (
    _options: SendOptions<string, string, string, string>,
    callback: Callback<ProduceResult>
  ) => {
    setImmediate(() => callback(null, { offsets: [{ topic: 'a', partition: 0, offset: 0n }] }))
  })

  const reports: Array<{ index: number }> = []
  sink.on('delivery-report', report => reports.push(report))

  sink.write({ topic: 'a', key: 'k1', value: 'v1' })
  sink.write({ topic: 'a', key: 'k2', value: 'v2' })

  await once(sink, 'flush')
  await sink.close()

  strictEqual(reports.length, 2)
  strictEqual(reports[0].index, 0)
  strictEqual(reports[1].index, 1)
})

test('asStream should handle producer send errors', async t => {
  const producer = createProducer<string, string, string, string>(t, { serializers: stringSerializers })
  const sink = producer.asStream({
    batchSize: 1,
    batchTime: 0
  })

  const mockedError = new Error('mocked send error')
  t.mock.method(producer, 'send', (
    _options: SendOptions<string, string, string, string>,
    callback: Callback<ProduceResult>
  ) => {
    setImmediate(() => callback(mockedError))
  })

  sink.write({ topic: 'a', key: 'k1', value: 'v1' })

  const [error] = await once(sink, 'error')
  strictEqual(error, mockedError)
})

test('asStream close should surface flush errors while finalizing', async t => {
  const producer = createProducer<string, string, string, string>(t, { serializers: stringSerializers })
  const sink = producer.asStream({
    batchSize: 1,
    batchTime: 0
  })

  const mockedError = new Error('mocked delayed send error')
  t.mock.method(producer, 'send', (
    _options: SendOptions<string, string, string, string>,
    callback: Callback<ProduceResult>
  ) => {
    setTimeout(() => callback(mockedError), 10)
  })

  sink.write({ topic: 'a', key: 'k1', value: 'v1' })

  const closeError = await new Promise<Error | null>(resolve => {
    sink.close(error => {
      resolve(error ?? null)
    })
  })

  strictEqual(closeError, mockedError)
})

test('asStream should flush queued batches recursively', async t => {
  const producer = createProducer<string, string, string, string>(t, { serializers: stringSerializers })
  const sink = producer.asStream({
    batchSize: 2,
    batchTime: 1000,
    reportMode: ProducerStreamReportModes.BATCH
  })

  t.mock.method(producer, 'send', (
    _options: SendOptions<string, string, string, string>,
    callback: Callback<ProduceResult>
  ) => {
    setImmediate(() => callback(null, { offsets: [] }))
  })

  const flushes: number[] = []
  sink.on('flush', report => {
    flushes.push(report.count)
  })

  sink.write({ topic: 'a', key: 'k1', value: 'v1' })
  sink.write({ topic: 'a', key: 'k2', value: 'v2' })
  sink.write({ topic: 'a', key: 'k3', value: 'v3' })
  sink.write({ topic: 'a', key: 'k4', value: 'v4' })

  await once(sink, 'flush')
  await once(sink, 'flush')
  await sink.close()

  deepStrictEqual(flushes, [2, 2])
})

test('asStream should not schedule timer when batchTime is negative', async t => {
  const producer = createProducer<string, string, string, string>(t, { serializers: stringSerializers })
  const sink = producer.asStream({
    batchSize: 10,
    batchTime: -1,
    reportMode: ProducerStreamReportModes.NONE
  })

  let sendCalls = 0
  t.mock.method(producer, 'send', (
    _options: SendOptions<string, string, string, string>,
    callback: Callback<ProduceResult>
  ) => {
    sendCalls++
    setImmediate(() => callback(null, { offsets: [] }))
  })

  let reports = 0
  sink.on('delivery-report', () => {
    reports++
  })

  sink.write({ topic: 'a', key: 'k1', value: 'v1' })

  await sleep(30)
  strictEqual(sendCalls, 0)

  await new Promise<void>((resolve, reject) => {
    sink.close(error => {
      if (error) {
        reject(error)
        return
      }

      resolve()
    })
  })

  strictEqual(sendCalls, 1)
  strictEqual(reports, 0)
})

test('asStream using _writev should flush immediately when buffered chunks reach batchSize', async t => {
  const producer = createProducer<string, string, string, string>(t, { serializers: stringSerializers })
  const sink = producer.asStream({
    batchSize: 2,
    batchTime: 1000,
    reportMode: ProducerStreamReportModes.BATCH
  })

  const sentBatches: Array<SendOptions<string, string, string, string>> = []
  t.mock.method(producer, 'send', (
    options: SendOptions<string, string, string, string>,
    callback: Callback<ProduceResult>
  ) => {
    sentBatches.push(options)
    setImmediate(() => callback(null, { offsets: [] }))
  })

  await new Promise<void>((resolve, reject) => {
    sink._writev(
      [{ chunk: { topic: 'a', key: 'k1', value: 'v1' } }, { chunk: { topic: 'a', key: 'k2', value: 'v2' } }],
      error => {
        if (error) {
          reject(error)
          return
        }

        resolve()
      }
    )
  })

  await once(sink, 'flush')
  await sink.close()

  strictEqual(sentBatches.length, 1)
  deepStrictEqual(
    sentBatches[0].messages.map(message => message.value),
    ['v1', 'v2']
  )
})

test('asStream using _writev should schedule a timer-based flush when buffered chunks are below batchSize', async t => {
  const producer = createProducer<string, string, string, string>(t, { serializers: stringSerializers })
  const sink = producer.asStream({
    batchSize: 3,
    batchTime: 10,
    reportMode: ProducerStreamReportModes.BATCH
  })

  let sendCalls = 0
  t.mock.method(producer, 'send', (
    _options: SendOptions<string, string, string, string>,
    callback: Callback<ProduceResult>
  ) => {
    sendCalls++
    setImmediate(() => callback(null, { offsets: [] }))
  })

  await new Promise<void>((resolve, reject) => {
    sink._writev(
      [{ chunk: { topic: 'a', key: 'k1', value: 'v1' } }, { chunk: { topic: 'a', key: 'k2', value: 'v2' } }],
      error => {
        if (error) {
          reject(error)
          return
        }

        resolve()
      }
    )
  })

  strictEqual(sendCalls, 0)

  await once(sink, 'flush')
  await sink.close()

  strictEqual(sendCalls, 1)
})

test('asStream using _write should apply backpressure when highWaterMark is 1', async t => {
  const producer = createProducer<string, string, string, string>(t, { serializers: stringSerializers })
  const sink = producer.asStream({
    highWaterMark: 1,
    batchSize: 1,
    batchTime: 1000
  })

  t.mock.method(producer, 'send', (
    _options: SendOptions<string, string, string, string>,
    callback: Callback<ProduceResult>
  ) => {
    setTimeout(() => callback(null, { offsets: [] }), 20)
  })

  let flushed = false
  sink.once('flush', () => {
    flushed = true
  })

  let callbackCalled = false
  const callbackCompleted = new Promise<void>((resolve, reject) => {
    sink._write({ topic: 'a', key: 'k1', value: 'v1' }, 'utf8', error => {
      if (error) {
        reject(error)
        return
      }

      strictEqual(flushed, true)
      callbackCalled = true
      resolve()
    })
  })

  strictEqual(callbackCalled, false)

  await callbackCompleted
  await sink.close()

  strictEqual(callbackCalled, true)
})

test('asStream using _writev should apply backpressure when highWaterMark is 1', async t => {
  const producer = createProducer<string, string, string, string>(t, { serializers: stringSerializers })
  const sink = producer.asStream({
    highWaterMark: 1,
    batchSize: 2,
    batchTime: 1000
  })

  t.mock.method(producer, 'send', (
    _options: SendOptions<string, string, string, string>,
    callback: Callback<ProduceResult>
  ) => {
    setTimeout(() => callback(null, { offsets: [] }), 20)
  })

  let flushed = false
  sink.once('flush', () => {
    flushed = true
  })

  let callbackCalled = false
  const callbackCompleted = new Promise<void>((resolve, reject) => {
    sink._writev(
      [{ chunk: { topic: 'a', key: 'k1', value: 'v1' } }, { chunk: { topic: 'a', key: 'k2', value: 'v2' } }],
      error => {
        if (error) {
          reject(error)
          return
        }

        strictEqual(flushed, true)
        callbackCalled = true
        resolve()
      }
    )
  })

  strictEqual(callbackCalled, false)

  await callbackCompleted
  await sink.close()

  strictEqual(callbackCalled, true)
})

