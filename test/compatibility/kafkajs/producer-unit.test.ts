import { deepStrictEqual, equal, ok, rejects, throws } from 'node:assert'
import { afterEach, describe, test } from 'node:test'
import KafkaJS from 'kafkajs'
import {
  CompressionCodecs,
  CompressionTypes,
  Kafka,
  KafkaJSInvariantViolation,
  KafkaJSMetadataNotLoaded,
  KafkaJSNoBrokerAvailableError,
  KafkaJSNonRetriableError,
  KafkaJSTopicMetadataNotLoaded,
  logLevel,
  Partitioners
} from '../../../src/compatibility/kafkajs/index.ts'
import { createLogger } from '../../../src/compatibility/kafkajs/logger.ts'
import { normalizeMessage } from '../../../src/compatibility/kafkajs/partitioners.ts'
import { KafkaJSProducerBridge } from '../../../src/compatibility/kafkajs/producer-native.ts'
import { kKafkaJSProduceTimeout } from '../../../src/compatibility/kafkajs/symbols.ts'
import { kGetProduceTimeout } from '../../../src/clients/producer/producer.ts'

const KafkaJSRuntime = KafkaJS as typeof KafkaJS & {
  KafkaJSInvariantViolation: new (error: string) => Error
  KafkaJSNoBrokerAvailableError: new () => Error
}

const originalLogLevel = process.env.KAFKAJS_LOG_LEVEL

afterEach(() => {
  if (originalLogLevel === undefined) {
    delete process.env.KAFKAJS_LOG_LEVEL
  } else {
    process.env.KAFKAJS_LOG_LEVEL = originalLogLevel
  }
})

function nativeOptions (producer: object): Record<string, unknown> {
  const native = (producer as { native: object }).native
  const optionsSymbol = Object.getOwnPropertySymbols(native).find(
    symbol => symbol.description === 'plt.kafka.base.options'
  )
  ok(optionsSymbol)
  return (native as Record<symbol, Record<string, unknown>>)[optionsSymbol]
}

describe('KafkaJS producer compatibility', () => {
  test('uses the compatibility producer subclass', () => {
    const producer = new Kafka({ brokers: ['localhost:9092'] }).producer()
    ok((producer as unknown as { native: unknown }).native instanceof KafkaJSProducerBridge)
  })

  test('uses KafkaJS producer defaults and inherits retry fields', () => {
    const kafka = new Kafka({
      brokers: ['localhost:9092'],
      retry: { retries: 9, initialRetryTime: 11, factor: 0, multiplier: 3, maxRetryTime: 100 }
    })
    const inherited = nativeOptions(kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner }))
    equal(inherited.connectTimeout, 1000)
    equal(inherited.requestTimeout, 30_000)
    equal(inherited.metadataMaxAge, 300_000)
    equal(inherited.retries, 9)
    equal((inherited.retryDelay as (client: object, operation: string, attempt: number) => number)({}, 'send', 2), 33)

    const overridden = nativeOptions(
      kafka.producer({
        createPartitioner: Partitioners.DefaultPartitioner,
        retry: { retries: 2 }
      })
    )
    equal(overridden.retries, 2)
    equal((overridden.retryDelay as (client: object, operation: string, attempt: number) => number)({}, 'send', 2), 33)
  })

  test('matches KafkaJS validation before connect', async () => {
    const expected = new KafkaJS.Kafka({ brokers: ['localhost:9092'] }).producer({
      createPartitioner: KafkaJS.Partitioners.DefaultPartitioner
    })
    const actual = new Kafka({ brokers: ['localhost:9092'] }).producer({
      createPartitioner: Partitioners.DefaultPartitioner
    })

    await rejects(actual.send({ topic: '', messages: [{ value: 'value' }] }), error => {
      return error instanceof KafkaJSNonRetriableError && error.message === 'Invalid topic'
    })
    await rejects(actual.send({ topic: 'topic', messages: [{ value: undefined }] } as never), error => {
      return (
        error instanceof KafkaJSNonRetriableError &&
        error.message === 'Invalid message without value for topic "topic": {}'
      )
    })
    await rejects(actual.send({ topic: 'topic', messages: [{ value: 'value' }] }), {
      name: 'KafkaJSError',
      message: 'The producer is disconnected'
    })
    await rejects(expected.send({ topic: 'topic', messages: [{ value: 'value' }] }), {
      name: 'KafkaJSError',
      message: 'The producer is disconnected'
    })
  })

  test('reports idempotence and enforces idempotent configuration', async () => {
    const transactional = new Kafka({ brokers: ['localhost:9092'] }).producer({
      transactionalId: 'transaction',
      createPartitioner: Partitioners.DefaultPartitioner
    })
    equal(transactional.isIdempotent(), false)

    throws(
      () =>
        new Kafka({ brokers: ['localhost:9092'] }).producer({
          idempotent: true,
          retry: { retries: 0 },
          createPartitioner: Partitioners.DefaultPartitioner
        }),
      /Idempotent producer must allow retries/
    )

    const producer = new Kafka({ brokers: ['localhost:9092'] }).producer({
      idempotent: true,
      createPartitioner: Partitioners.DefaultPartitioner
    })
    ;(producer as unknown as { state: string }).state = 'connected'
    await rejects(producer.send({ topic: 'topic', acks: 1, messages: [{ value: 'value' }] }), /EoS guarantees/)
  })

  test('validates event names and isolates rejected listeners', async () => {
    const entries: string[] = []
    const producer = new Kafka({
      brokers: ['localhost:9092'],
      logCreator: () => entry => entries.push(entry.log.message)
    }).producer({ createPartitioner: Partitioners.DefaultPartitioner })
    throws(() => producer.on('invalid' as never, () => {}), /producer\.events\.CONNECT/)
    producer.on(producer.events.CONNECT, async () => {
      throw new Error('listener failed')
    })
    ;(producer as unknown as { emit: (name: string) => void }).emit(producer.events.CONNECT)
    await new Promise(resolve => setImmediate(resolve))
    ok(entries.includes('Failed to execute listener: listener failed'))
  })

  test('passes KafkaJS record timeout to the native Produce request', async () => {
    const producer = new Kafka({ brokers: ['localhost:9092'] }).producer({
      createPartitioner: Partitioners.DefaultPartitioner
    })
    ;(producer as unknown as { state: string }).state = 'connected'
    const native = (producer as unknown as {
      native: {
        metadata: () => Promise<void>
        send: (options: { [kKafkaJSProduceTimeout]?: number }) => Promise<{ offsets: [] }>
      }
    }).native
    native.metadata = async () => {}
    native.send = async options => {
      equal(options[kKafkaJSProduceTimeout], 5)
      return { offsets: [] }
    }
    await producer.send({ topic: 'topic', timeout: 5, messages: [{ value: 'value' }] })

    const bridge = new KafkaJSProducerBridge({
      clientId: 'compat-test',
      bootstrapBrokers: ['localhost:9092'],
      timeout: 30_000
    })
    equal(
      bridge[kGetProduceTimeout]({
        messages: [],
        [kKafkaJSProduceTimeout]: 5
      }),
      5
    )

    let timeoutEvents = 0
    producer.on(producer.events.REQUEST_TIMEOUT, () => timeoutEvents++)
    ;(producer as unknown as {
      diagnosticContext: unknown
      onRequestTimeoutDiagnostic: (diagnostic: unknown) => void
    }).onRequestTimeoutDiagnostic({
      connection: { context: (producer as unknown as { diagnosticContext: unknown }).diagnosticContext },
      error: { code: 'PLT_KFK_TIMEOUT' }
    })
    equal(timeoutEvents, 1)
  })

  test('preserves Produce response metadata without fabricating legacy fields', async () => {
    const producer = new Kafka({ brokers: ['localhost:9092'] }).producer({
      createPartitioner: Partitioners.DefaultPartitioner
    })
    ;(producer as unknown as { state: string }).state = 'connected'
    const native = (producer as unknown as {
      native: { metadata: () => Promise<void>; send: () => Promise<unknown> }
    }).native
    native.metadata = async () => {}
    native.send = async () => ({
      offsets: [{
        topic: 'topic',
        partition: 1,
        offset: 4n,
        errorCode: 0,
        logAppendTime: -1n,
        logStartOffset: 0n
      }]
    })

    deepStrictEqual(await producer.send({ topic: 'topic', messages: [{ value: 'value' }] }), [{
      topicName: 'topic',
      partition: 1,
      errorCode: 0,
      baseOffset: '4',
      logAppendTime: '-1',
      logStartOffset: '0'
    }])
  })

  test('preserves repeated headers as ordered native entries', () => {
    const message = normalizeMessage({
      value: 'value',
      headers: { repeated: ['first', Buffer.from('second')], single: 'value' }
    })
    deepStrictEqual(
      Array.from(message.headers ?? [], ([key, value]) => [key.toString(), value.toString()]),
      [
        ['repeated', 'first'],
        ['repeated', 'second'],
        ['single', 'value']
      ]
    )
  })

  test('rejects invalid custom partitioner results', () => {
    const producer = new Kafka({ brokers: ['localhost:9092'] }).producer({ createPartitioner: () => () => -1 })
    const partitioner = nativeOptions(producer).partitioner as (
      message: { topic: string; metadata: { kafkaJSMessage: { value: string } } },
      key: undefined,
      context: { metadata: { topics: Map<string, { partitions: unknown[]; partitionsCount: number }> } }
    ) => number
    throws(
      () =>
        partitioner({ topic: 'topic', metadata: { kafkaJSMessage: { value: 'value' } } }, undefined, {
          metadata: { topics: new Map([['topic', { partitions: [], partitionsCount: 2 }]]) }
        }),
      /Invalid partitioner result: -1/
    )
  })

  test('passes explicit partitions to custom KafkaJS partitioners', () => {
    let receivedPartition: number | undefined
    const producer = new Kafka({ brokers: ['localhost:9092'] }).producer({
      createPartitioner: () => ({ message }) => {
        receivedPartition = message.partition
        return 1
      }
    })
    const partitioner = nativeOptions(producer).partitioner as (
      message: { topic: string; metadata: { kafkaJSMessage: { partition?: number } } },
      key: undefined,
      context: { metadata: { topics: Map<string, { partitions: unknown[]; partitionsCount: number }> } }
    ) => number
    const message = normalizeMessage({ value: 'value', partition: 0 }, true)

    equal(message.partition, undefined)
    equal(
      partitioner(
        { topic: 'topic', ...message },
        undefined,
        { metadata: { topics: new Map([['topic', { partitions: [], partitionsCount: 2 }]]) } }
      ),
      1
    )
    equal(receivedPartition, 0)
  })

  test('matches error defaults, inheritance and messages', () => {
    const actualTopic = new KafkaJSTopicMetadataNotLoaded('missing', { topic: 'topic' })
    const expectedTopic = new KafkaJS.KafkaJSTopicMetadataNotLoaded('missing', { topic: 'topic' })
    equal(actualTopic instanceof KafkaJSMetadataNotLoaded, true)
    equal(actualTopic instanceof KafkaJSNonRetriableError, false)
    deepStrictEqual(
      {
        name: actualTopic.name,
        message: actualTopic.message,
        retriable: actualTopic.retriable,
        topic: actualTopic.topic
      },
      {
        name: expectedTopic.name,
        message: expectedTopic.message,
        retriable: expectedTopic.retriable,
        topic: expectedTopic.topic
      }
    )
    equal(new KafkaJSNoBrokerAvailableError().message, new KafkaJSRuntime.KafkaJSNoBrokerAvailableError().message)
    equal(
      new KafkaJSInvariantViolation('broken').message,
      new KafkaJSRuntime.KafkaJSInvariantViolation('broken').message
    )
  })

  test('matches logger namespace and environment filtering', () => {
    process.env.KAFKAJS_LOG_LEVEL = 'error'
    const entries: Array<{ namespace: string; message: string }> = []
    const logger = createLogger(
      logLevel.DEBUG,
      () => entry =>
        entries.push({
          namespace: entry.namespace,
          message: entry.log.message
        })
    )
    logger.namespace('Producer', logLevel.DEBUG).warn('hidden')
    logger.namespace('Producer').error('visible')
    deepStrictEqual(entries, [{ namespace: 'Producer', message: 'visible' }])
  })

  test('exposes working codecs backed by the native compression implementation', async () => {
    const input = Buffer.from('compressible data '.repeat(20))
    for (const type of [CompressionTypes.GZIP, CompressionTypes.Snappy, CompressionTypes.LZ4]) {
      const codec = CompressionCodecs[type]()
      const compressed = await codec.compress({ buffer: input })
      deepStrictEqual(await codec.decompress(compressed), input)
    }
  })
})
