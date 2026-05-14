import { deepStrictEqual, ok, strictEqual } from 'node:assert'
import { pbkdf2Sync, randomBytes, randomUUID } from 'node:crypto'
import { mkdir, writeFile } from 'node:fs/promises'
import { dirname, join } from 'node:path'
import { monitorEventLoopDelay, performance } from 'node:perf_hooks'
import type { TestContext } from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import {
  Admin,
  alterUserScramCredentialsV0,
  Connection,
  Consumer,
  type ConsumerOptions,
  type Deserializers,
  type Message,
  MessagesStreamModes,
  parseBroker,
  Producer,
  type ProducerOptions,
  type Serializers,
  stringDeserializers,
  stringSerializers
} from '../../src/index.ts'

export const regressionBootstrapServers = ['localhost:9011', 'localhost:9012', 'localhost:9013']
export const regressionSingleBootstrapServers = ['localhost:9001']
export const regressionSaslBootstrapServers = ['localhost:9002']
export const regressionSaslKerberosBootstrapServers = ['localhost:9003']

const defaultRetryDelay = 500
let topicCounter = 0

export interface ConsumedValue {
  id: number
  partition?: number
  payload?: string
}

export interface ResourceSample {
  at: number
  heapUsed: number
  rss: number
  userCpuMicros: number
  systemCpuMicros: number
  eventLoopDelayMean: number
  eventLoopDelayMax: number
}

export interface ResourceStabilityOptions {
  maxHeapGrowthBytes?: number
  maxRssGrowthBytes?: number
  minSamples?: number
}

export interface BenchmarkResult {
  name: string
  startedAt: string
  durationMs: number
  messages: number
  messagesPerSecond: number
  bytes?: number
  bytesPerSecond?: number
  samples?: ResourceSample[]
  runs?: BenchmarkResult[]
}

export function createGroupId (): string {
  return `regression-group-${randomUUID()}`
}

export function createTopicName (): string {
  return `regression-topic-${randomUUID()}-${++topicCounter}`
}

export function createProducer<K = string, V = string, HK = string, HV = string> (
  t: TestContext,
  options: Partial<ProducerOptions<K, V, HK, HV>> = {}
): Producer<K, V, HK, HV> {
  const serializers = options.registry ? {} : { serializers: stringSerializers as unknown as Serializers<K, V, HK, HV> }
  const producer = new Producer<K, V, HK, HV>({
    clientId: `regression-producer-${randomUUID()}`,
    bootstrapBrokers: regressionBootstrapServers,
    autocreateTopics: false,
    retryDelay: defaultRetryDelay,
    ...serializers,
    ...options
  })

  t.after(() => producer.close())
  return producer
}

export function createConsumer<K = string, V = string, HK = string, HV = string> (
  t: TestContext,
  options: Partial<ConsumerOptions<K, V, HK, HV>> = {}
): Consumer<K, V, HK, HV> {
  const deserializers = options.registry
    ? {}
    : { deserializers: stringDeserializers as unknown as Deserializers<K, V, HK, HV> }
  const consumer = new Consumer<K, V, HK, HV>({
    clientId: `regression-consumer-${randomUUID()}`,
    bootstrapBrokers: regressionBootstrapServers,
    groupId: createGroupId(),
    timeout: 1000,
    sessionTimeout: 6000,
    rebalanceTimeout: 6000,
    heartbeatInterval: 1000,
    retryDelay: defaultRetryDelay,
    ...deserializers,
    ...options
  })

  t.after(() => consumer.close(true))
  return consumer
}

export async function createTopic (
  t: TestContext,
  partitions = 1,
  bootstrapBrokers = regressionBootstrapServers,
  replicas = 1
): Promise<string> {
  const topic = createTopicName()
  const admin = new Admin({ clientId: `regression-admin-${randomUUID()}`, bootstrapBrokers })

  t.after(() => admin.close())
  await admin.createTopics({ topics: [topic], partitions, replicas })
  await sleep(500)
  await admin.metadata({ topics: [topic] })

  return topic
}

export function assertNoDuplicateIds (messages: Array<Message<string, ConsumedValue, string, string>>): void {
  const seen = new Set<number>()

  for (const message of messages) {
    strictEqual(seen.has(message.value.id), false, `duplicate message id ${message.value.id}`)
    seen.add(message.value.id)
  }
}

export function assertContiguousIds (
  messages: Array<Message<string, ConsumedValue, string, string>>,
  expected: number
): void {
  const ids = messages.map(message => message.value.id).sort((a, b) => a - b)
  strictEqual(ids.length, expected)
  deepStrictEqual(
    ids,
    Array.from({ length: expected }, (_, index) => index)
  )
}

export async function assertCommittedOffset (
  consumer: Consumer<string, ConsumedValue, string, string>,
  topic: string,
  partition: number,
  expected: bigint
): Promise<void> {
  const committed = await consumer.listCommittedOffsets({ topics: [{ topic, partitions: [partition] }] })
  deepStrictEqual(committed.get(topic), [expected])
}

export async function collectMessages (
  stream: AsyncIterable<Message<string, ConsumedValue, string, string>> & { close?: () => Promise<void> },
  expected: number,
  timeoutMs = 30_000
): Promise<Array<Message<string, ConsumedValue, string, string>>> {
  const messages: Array<Message<string, ConsumedValue, string, string>> = []
  const startedAt = performance.now()

  for await (const message of stream) {
    messages.push(message)

    if (messages.length >= expected) {
      await stream.close?.()
      break
    }

    ok(performance.now() - startedAt < timeoutMs, `timed out after ${messages.length}/${expected} messages`)
  }

  strictEqual(messages.length, expected)
  return messages
}

export async function consumeJsonMessages (
  t: TestContext,
  topic: string,
  expected: number,
  options: Partial<ConsumerOptions<string, ConsumedValue, string, string>> = {}
): Promise<{
  consumer: Consumer<string, ConsumedValue, string, string>
  messages: Array<Message<string, ConsumedValue, string, string>>
}> {
  const consumer = createConsumer<string, ConsumedValue, string, string>(t, {
    deserializers: {
      key: data => data?.toString() ?? '',
      value: data => JSON.parse(data?.toString() ?? '{}') as ConsumedValue,
      headerKey: data => data?.toString() ?? '',
      headerValue: data => data?.toString() ?? ''
    },
    ...options
  })

  await consumer.topics.trackAll(topic)
  await consumer.joinGroup()

  const stream = await consumer.consume({
    topics: [topic],
    mode: MessagesStreamModes.EARLIEST,
    autocommit: false,
    maxWaitTime: 1000
  })
  const messages = await collectMessages(stream, expected)

  return { consumer, messages }
}

export async function produceJsonMessages (
  t: TestContext,
  topic: string,
  count: number,
  options: Partial<ProducerOptions<string, string, string, string>> = {}
): Promise<void> {
  const producer = createProducer(t, options)
  const messages = Array.from({ length: count }, (_, id) => ({
    topic,
    key: String(id),
    value: JSON.stringify({ id, payload: `payload-${id}` })
  }))

  await producer.send({ messages })
}

export function createResourceSampler (intervalMs = 250): { stop: () => ResourceSample[] } {
  const delay = monitorEventLoopDelay({ resolution: 20 })
  const samples: ResourceSample[] = []
  const startedAt = performance.now()
  delay.enable()

  const timer = setInterval(() => {
    const memory = process.memoryUsage()
    const cpu = process.cpuUsage()

    samples.push({
      at: performance.now() - startedAt,
      heapUsed: memory.heapUsed,
      rss: memory.rss,
      userCpuMicros: cpu.user,
      systemCpuMicros: cpu.system,
      eventLoopDelayMean: Number.isFinite(delay.mean) ? delay.mean : 0,
      eventLoopDelayMax: delay.max
    })
  }, intervalMs)

  timer.unref()

  return {
    stop () {
      clearInterval(timer)
      delay.disable()
      return samples
    }
  }
}

export function assertResourceStability (samples: ResourceSample[], options: ResourceStabilityOptions = {}): void {
  const minSamples = options.minSamples ?? 2

  ok(samples.length >= minSamples, `resource sampler collected ${samples.length}/${minSamples} required samples`)

  const first = samples[0]!
  const last = samples.at(-1)!
  const maxHeapGrowthBytes = options.maxHeapGrowthBytes ?? 64 * 1024 * 1024
  const maxRssGrowthBytes = options.maxRssGrowthBytes ?? 128 * 1024 * 1024

  ok(
    last.heapUsed <= first.heapUsed + maxHeapGrowthBytes,
    `heap grew beyond the regression envelope: first=${first.heapUsed} last=${last.heapUsed}`
  )
  ok(
    last.rss <= first.rss + maxRssGrowthBytes,
    `rss grew beyond the regression envelope: first=${first.rss} last=${last.rss}`
  )

  if (samples.length < 4) {
    return
  }

  const midpoint = Math.floor(samples.length / 2)
  const firstHalf = samples.slice(0, midpoint)
  const secondHalf = samples.slice(midpoint)
  const firstHeapMedian = medianSampleValue(firstHalf, 'heapUsed')
  const secondHeapMedian = medianSampleValue(secondHalf, 'heapUsed')
  const firstRssMedian = medianSampleValue(firstHalf, 'rss')
  const secondRssMedian = medianSampleValue(secondHalf, 'rss')

  ok(
    secondHeapMedian <= firstHeapMedian + maxHeapGrowthBytes,
    `heap median drifted beyond the regression envelope: first=${firstHeapMedian} second=${secondHeapMedian}`
  )
  ok(
    secondRssMedian <= firstRssMedian + maxRssGrowthBytes,
    `rss median drifted beyond the regression envelope: first=${firstRssMedian} second=${secondRssMedian}`
  )
}

function medianSampleValue (samples: ResourceSample[], key: 'heapUsed' | 'rss'): number {
  const values = samples.map(sample => sample[key]).sort((a, b) => a - b)
  const middle = Math.floor(values.length / 2)

  if (values.length % 2 === 1) {
    return values[middle]!
  }

  return (values[middle - 1]! + values[middle]!) / 2
}

export async function writeRegressionArtifact (name: string, result: unknown): Promise<string> {
  const artifactPath = join('regression', 'artifacts', `${name}.json`)
  await mkdir(dirname(artifactPath), { recursive: true })
  await writeFile(artifactPath, `${JSON.stringify(result, null, 2)}\n`)
  return artifactPath
}

export async function createScramUsers (): Promise<void> {
  const broker = parseBroker(regressionSaslBootstrapServers[0]!)
  const connection = new Connection('regression-scram-admin')
  await connection.connect(broker.host, broker.port + 10000)

  try {
    const password = 'admin'
    const salt = randomBytes(16)
    const iterations = 4096
    const mechanisms = [
      { algorithm: 'sha256', mechanism: 1, length: 32 },
      { algorithm: 'sha512', mechanism: 2, length: 64 }
    ]

    for (const { algorithm, mechanism, length } of mechanisms) {
      await alterUserScramCredentialsV0.api.async(
        connection,
        [],
        [
          {
            name: 'admin',
            mechanism,
            iterations,
            salt,
            saltedPassword: pbkdf2Sync(password, salt, iterations, length, algorithm)
          }
        ]
      )
    }
  } finally {
    await connection.close()
  }
}

export const createRegressionGroupId = createGroupId
export const createRegressionTopicName = createTopicName
export const createRegressionProducer = createProducer
export const createRegressionConsumer = createConsumer
export const createRegressionTopic = createTopic
export const createRegressionScramUsers = createScramUsers
