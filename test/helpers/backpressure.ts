import { randomUUID } from 'node:crypto'
import { Duplex } from 'node:stream'
import type { TestContext } from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import {
  Admin,
  Consumer,
  type Message,
  type MessagesStream,
  MessagesStreamModes,
  Producer,
  jsonSerializer,
  stringDeserializer,
  stringSerializer
} from '../../src/index.ts'
import { kafkaSingleBootstrapServers } from '../helpers.ts'

type CallbackFunction = (error?: Error | null) => void
type MessageWithTopicAndPartition = { topic: string; partition: number }

export type DeserializedMessage = Message<string, Record<string, unknown>, string, string>

/**
 * Replicates MQT's KafkaMessageBatchStream: a Duplex stream that batches Kafka messages
 * based on size and timeout constraints.
 *
 * - Accumulates messages across all partitions up to batchSize
 * - Groups messages by topic:partition when flushing
 * - Implements backpressure
 * - Auto-flushes on timeout for partial batches
 */
export class MessageBatchStream<TMessage extends MessageWithTopicAndPartition> extends Duplex {
  private readonly batchSize: number
  private readonly timeout: number

  private messages: TMessage[]
  private pendingBatches: TMessage[][]
  private existingTimeout: NodeJS.Timeout | undefined

  private pendingCallback: CallbackFunction | undefined
  private isBackPressured: boolean = false

  constructor (options: { batchSize: number; timeoutMilliseconds: number; readableHighWaterMark?: number }) {
    super({ objectMode: true, readableHighWaterMark: options.readableHighWaterMark })
    this.batchSize = options.batchSize
    this.timeout = options.timeoutMilliseconds
    this.messages = []
    this.pendingBatches = []
  }

  override _read (): void {
    this.isBackPressured = false
    this.flushPendingBatches()

    // Release held callback when downstream pulls — this is the backpressure
    // release mechanism that allows pipeline to resume writing.
    if (this.pendingCallback) {
      const cb = this.pendingCallback
      this.pendingCallback = undefined
      cb()
    }
  }

  override _write (message: TMessage, _encoding: BufferEncoding, callback: CallbackFunction): void {
    let canContinue = true

    try {
      this.messages.push(message)

      if (this.messages.length >= this.batchSize) {
        canContinue = this.flushMessages()
      } else {
        this.existingTimeout ??= setTimeout(() => this.flushMessages(), this.timeout)
      }
    } finally {
      // Hold the callback when backpressured — this causes pipeline's writable
      // buffer to fill, eventually making write() return false, which triggers
      // pipe() to call pause() on the source consumer stream.
      if (!canContinue) {
        this.pendingCallback = callback
      } else {
        callback()
      }
    }
  }

  override _final (callback: CallbackFunction): void {
    clearTimeout(this.existingTimeout)
    this.existingTimeout = undefined
    // Remaining messages are not committed, next consumer will process them
    this.messages = []
    this.pendingBatches = []
    this.push(null)
    callback()
  }

  private flushMessages (): boolean {
    clearTimeout(this.existingTimeout)
    this.existingTimeout = undefined

    if (this.messages.length === 0) return true

    if (this.isBackPressured) {
      this.existingTimeout = setTimeout(() => this.flushMessages(), this.timeout)
      return false
    }

    const messageBatch = this.messages.splice(0, this.messages.length)

    // Group by topic:partition — each group is pushed as a separate readable object
    const messagesByTopicPartition: Record<string, TMessage[]> = {}
    for (const message of messageBatch) {
      const key = `${message.topic}:${message.partition}`
      if (!messagesByTopicPartition[key]) messagesByTopicPartition[key] = []
      messagesByTopicPartition[key].push(message)
    }

    let canContinue = true
    for (const messagesForKey of Object.values(messagesByTopicPartition)) {
      canContinue = this.push(messagesForKey)
    }

    if (!canContinue) this.isBackPressured = true

    return canContinue
  }

  private flushPendingBatches (): void {
    // No-op — kept for _read() compatibility. Backpressure is handled
    // by holding the _write() callback in the new implementation.
  }
}

// CDC-like message factories (~150 bytes each) — same as the load test
export const messageFactories = [
  (id: number) => ({ op: 'c', after: { id }, before: null }),
  (id: number) => ({ op: 'c', after: { entry_id: id }, before: null }),
  (id: number) => ({ op: 'u', after: { entry_id: id }, before: null }),
  (id: number) => ({ op: 'c', after: { key_id: id }, before: null }),
  (id: number) => ({ op: 'c', after: { form_id: id }, before: null }),
  (id: number) => ({ op: 'c', after: { id, entry_id: id }, before: null }),
  (id: number) => ({ op: 'c', after: { trans_id: id }, before: null }),
  (id: number) => ({ op: 'u', after: { id: String(id), team_id: 1 }, before: { id: String(id), team_id: 0 } }),
  (id: number) => ({
    op: 'u',
    after: { project_id: String(id), is_default: 1 },
    before: { project_id: String(id), is_default: 0 }
  }),
  (id: number) => ({ op: 'c', after: { entry_id: id, form_id: id }, before: null }),
  (id: number) => ({ op: 'c', after: { from_trans_id: id, to_trans_id: id + 100_000 }, before: null }),
  (id: number) => ({ op: 'c', after: { key_id: id, tag_name: `tag-${id}` }, before: null }),
  (id: number) => ({ op: 'c', after: { id, status: 'active' }, before: null }),
  (id: number) => ({ op: 'c', after: { id, score: id * 0.1 }, before: null }),
  (id: number) => ({ op: 'c', after: { id, name: `entity-${id}` }, before: null })
]

export interface BackpressureTestSetupOptions {
  topicCount: number
  messagesPerTopic: number
  consumerHighWaterMark: number
  publishBatchSize?: number
  publishConcurrency?: number
  topicPrefix?: string
  messageValueFactory?: (id: number, topicIndex: number) => object
  bootstrapBrokers?: string[]
  partitionsPerTopic?: number
  replicas?: number
}

export interface BackpressureTestSetup {
  topics: string[]
  totalMessages: number
  consumerStream: MessagesStream<string, Record<string, unknown>, string, string>
  consumer: Consumer<string, Record<string, unknown>, string, string>
  producer: Producer<string, object, string, string>
}

/**
 * Sets up a full backpressure test scenario: creates topics, publishes interleaved
 * messages, and returns a consumer stream ready for piping.
 *
 * The consumer starts BEFORE publishing so that it fetches partial results as they
 * arrive, creating cross-topic batches that trigger backpressure.
 */
export async function setupBackpressureTest (
  t: TestContext,
  options: BackpressureTestSetupOptions
): Promise<BackpressureTestSetup> {
  const {
    topicCount,
    messagesPerTopic,
    consumerHighWaterMark,
    publishBatchSize = 500,
    publishConcurrency = 4,
    topicPrefix = `bp-${randomUUID().slice(0, 8)}`,
    messageValueFactory,
    bootstrapBrokers = kafkaSingleBootstrapServers,
    partitionsPerTopic = 1,
    replicas = 1
  } = options

  const totalMessages = topicCount * messagesPerTopic

  // Create topics
  const topics: string[] = []
  for (let i = 0; i < topicCount; i++) {
    topics.push(`${topicPrefix}-${i}`)
  }

  const admin = new Admin({
    clientId: `bp-admin-${randomUUID().slice(0, 8)}`,
    bootstrapBrokers
  })
  t.after(() => admin.close())

  for (const topic of topics) {
    try {
      await admin.createTopics({ topics: [topic], partitions: partitionsPerTopic, replicas })
    } catch {
      // May already exist
    }
  }
  await sleep(500) // Wait for topics to propagate

  // Producer setup
  const producer = new Producer<string, object, string, string>({
    clientId: `bp-producer-${randomUUID().slice(0, 8)}`,
    bootstrapBrokers,
    timeout: 120_000,
    requestTimeout: 120_000,
    serializers: {
      key: stringSerializer,
      value: jsonSerializer,
      headerKey: stringSerializer,
      headerValue: stringSerializer
    }
  })
  t.after(() => producer.close())

  // Build messages interleaved across topics (round-robin).
  // Each publish batch contains messages from all topics.
  const allMessages: { topic: string; key: string; value: object }[] = []
  const idBase = Math.round(Math.random() * 1_000_000)

  for (let i = 0; i < messagesPerTopic; i++) {
    for (let ti = 0; ti < topics.length; ti++) {
      const value = messageValueFactory
        ? messageValueFactory(idBase + i, ti)
        : messageFactories[ti % messageFactories.length]!(idBase + i)

      allMessages.push({
        topic: topics[ti]!,
        key: `key-${idBase + i}`,
        value
      })
    }
  }

  // Start consumer BEFORE publishing — the consumer must be running when
  // messages start arriving so that it fetches partial results, creating
  // the conditions for backpressure (cross-topic batches from partial fetches).
  const consumer = new Consumer<string, Record<string, unknown>, string, string>({
    clientId: `bp-consumer-${randomUUID().slice(0, 8)}`,
    groupId: `bp-group-${randomUUID()}`,
    bootstrapBrokers,
    deserializers: {
      key: stringDeserializer,
      value: data => {
        if (!data) return {} as Record<string, unknown>
        try {
          return JSON.parse(Buffer.isBuffer(data) ? data.toString() : String(data))
        } catch {
          return {} as Record<string, unknown>
        }
      },
      headerKey: stringDeserializer,
      headerValue: stringDeserializer
    },
    sessionTimeout: 20_000,
    rebalanceTimeout: 40_000,
    heartbeatInterval: 3_000
  })
  t.after(() => consumer.close(true))

  for (const topic of topics) {
    await consumer.topics.trackAll(topic)
  }
  await consumer.joinGroup()

  const consumerStream = await consumer.consume({
    topics,
    mode: MessagesStreamModes.EARLIEST,
    maxWaitTime: 1000,
    autocommit: true,
    highWaterMark: consumerHighWaterMark
  })

  // Publish AFTER consumer is running with bounded concurrency so messages
  // arrive while the consumer is already fetching without overwhelming the
  // brokers with dozens of simultaneous produce requests.
  const publishPromises: Promise<unknown>[] = []
  for (let i = 0; i < allMessages.length; i += publishBatchSize) {
    const batch = allMessages.slice(i, i + publishBatchSize)
    publishPromises.push(producer.send({ messages: batch }))

    if (publishPromises.length >= publishConcurrency) {
      await Promise.all(publishPromises)
      publishPromises.length = 0
    }
  }

  await Promise.all(publishPromises)

  return { topics, totalMessages, consumerStream, consumer, producer }
}
