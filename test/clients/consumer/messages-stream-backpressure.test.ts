import { ok, strictEqual } from 'node:assert'
import { randomUUID } from 'node:crypto'
import { pipeline } from 'node:stream/promises'
import { Duplex } from 'node:stream'
import { test } from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import {
  Admin,
  Consumer,
  type Message,
  MessagesStreamModes,
  Producer,
  jsonSerializer,
  stringDeserializer,
  stringSerializer
} from '../../../src/index.ts'
import { kafkaSingleBootstrapServers } from '../../helpers.ts'

/**
 * Backpressure stall reproduction test.
 *
 * Reproduces the fetch loop stall that occurs when MessagesStream is piped
 * through a batch-accumulating Duplex (like MQT's KafkaMessageBatchStream)
 * with async handler processing.
 *
 */

type CallbackFunction = (error?: Error | null) => void
type MessageWithTopicAndPartition = { topic: string; partition: number }

/**
 * Replicates MQT's KafkaMessageBatchStream: a Duplex stream that batches Kafka messages
 * based on size and timeout constraints.
 *
 * - Accumulates messages across all partitions up to batchSize
 * - Groups messages by topic:partition when flushing
 * - Implements backpressure
 * - Auto-flushes on timeout for partial batches
 */
class MessageBatchStream<TMessage extends MessageWithTopicAndPartition> extends Duplex {
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

type DeserializedMessage = Message<string, Record<string, unknown>, string, string>

// Same message factories as the load test — small CDC-like messages (~150 bytes)
const messageFactories = [
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

test('should not stall under backpressure with pipeline and batch stream', { timeout: 120_000 }, async t => {
  const topicCount = 15
  const messagesPerTopic = 1000
  const totalMessages = topicCount * messagesPerTopic
  const batchSize = 500
  const handlerDelayMs = 5
  const consumerHighWaterMark = 1024
  const PUBLISH_BATCH = 500

  // Create topics
  const topicPrefix = `bp-stall-${randomUUID().slice(0, 8)}`
  const topics: string[] = []
  for (let i = 0; i < topicCount; i++) {
    topics.push(`${topicPrefix}-${i}`)
  }

  // Create topics with exact names matching the load test
  const admin = new Admin({
    clientId: 'backpressure-test-admin',
    bootstrapBrokers: kafkaSingleBootstrapServers
  })
  t.after(() => admin.close())

  for (const topic of topics) {
    try {
      await admin.createTopics({ topics: [topic], partitions: 1, replicas: 1 })
    } catch {
      // May already exist
    }
  }
  await sleep(500) // Wait for topics to propagate

  // Producer setup
  const producer = new Producer<string, object, string, string>({
    clientId: `bp-producer-${randomUUID()}`,
    bootstrapBrokers: kafkaSingleBootstrapServers,
    serializers: {
      key: stringSerializer,
      value: jsonSerializer,
      headerKey: stringSerializer,
      headerValue: stringSerializer
    }
  })
  t.after(() => producer.close())

  // Build all messages upfront, interleaved across topics (round-robin).
  // Each batch of 500 contains messages from all 15 topics.
  const allMessages: { topic: string; key: string; value: object }[] = []
  const idBase = Math.round(Math.random() * 1_000_000)

  for (let i = 0; i < messagesPerTopic; i++) {
    for (let ti = 0; ti < topics.length; ti++) {
      const factory = messageFactories[ti % messageFactories.length]!
      allMessages.push({
        topic: topics[ti]!,
        key: `key-${idBase + i}`,
        value: factory(idBase + i)
      })
    }
  }

  // ── Start consumer BEFORE publishing ──
  // The consumer must be running when messages start arriving so that it
  // fetches partial results as they appear, creating the conditions for
  // backpressure (cross-topic batches from partial fetches).
  const groupId = `bp-stall-test-${randomUUID()}`
  const consumer = new Consumer<string, Record<string, unknown>, string, string>({
    clientId: `bp-consumer-${randomUUID()}`,
    groupId,
    bootstrapBrokers: kafkaSingleBootstrapServers,
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

  // Consumer stream config matching production defaults:
  // - highWaterMark: 1024 (default) — push() returns false when buffer exceeds this
  // - maxWaitTime: 1000ms — broker accumulates messages for up to 1s before responding
  const consumerStream = await consumer.consume({
    topics,
    mode: MessagesStreamModes.EARLIEST,
    maxWaitTime: 1000,
    autocommit: true,
    highWaterMark: consumerHighWaterMark
  })

  // Batch stream config matching typical production setup:
  // batchSize=500, timeoutMilliseconds=2000, readableHighWaterMark=32
  const batchStream = new MessageBatchStream<DeserializedMessage>({
    batchSize,
    timeoutMilliseconds: 2000,
    readableHighWaterMark: 32
  })

  // pipeline(consumerStream, batchStream) — same as MQT's AbstractKafkaConsumer.start()
  const pipelinePromise = pipeline(consumerStream, batchStream).catch(error => {
    console.error('Pipeline error:', error)
  })

  // Consume with async handler — same as MQT's handleSyncStreamBatch
  let consumed = 0
  let batchesProcessed = 0
  const consumePromise = (async () => {
    for await (const messageBatch of batchStream) {
      const batch = messageBatch as DeserializedMessage[]
      batchesProcessed++

      // Matches real handler: iterate messages synchronously
      for (const message of batch) {
        consumed++
        // Sync work: schema validation (simulated by object access)
        // eslint-disable-next-line no-void
        void message.value
      }

      // Async flush: 3 parallel Redis SADD calls (simulated with sleep)
      await sleep(handlerDelayMs)

      // Commit last message in batch
      const lastMessage = batch[batch.length - 1]!
      await lastMessage.commit()
    }
  })()

  // ── Publish AFTER consumer is running ──
  // Fire all batches concurrently (Promise.all) so messages arrive gradually
  // while the consumer is already fetching.
  const publishPromises: Promise<unknown>[] = []

  for (let i = 0; i < allMessages.length; i += PUBLISH_BATCH) {
    const batch = allMessages.slice(i, i + PUBLISH_BATCH)
    publishPromises.push(producer.send({ messages: batch }))
  }

  await Promise.all(publishPromises)

  // Monitor progress with stall detection — same as load test
  const consumeStart = performance.now()
  const timeoutMs = 60_000
  let lastConsumed = 0
  let stallTicks = 0

  const result = await new Promise<'ok' | 'stall' | 'timeout'>(resolve => {
    const check = setInterval(() => {
      const elapsed = performance.now() - consumeStart

      if (consumed >= totalMessages) {
        clearInterval(check)
        resolve('ok')
        return
      }

      if (elapsed > timeoutMs) {
        clearInterval(check)
        resolve('timeout')
        return
      }

      // Stall detection: no progress for 5 seconds
      if (consumed === lastConsumed && consumed > 0) {
        stallTicks++
        if (stallTicks >= 10) {
          clearInterval(check)
          resolve('stall')
          return
        }
      } else {
        stallTicks = 0
      }
      lastConsumed = consumed
    }, 500)
  })

  // Cleanup
  await consumerStream.close()
  await Promise.all([pipelinePromise, consumePromise]).catch(() => {})

  // The bug manifests as a stall or timeout — the fetch loop dies and messages
  // remain unconsumed in Kafka. With the fix applied, all messages are consumed.
  strictEqual(
    result,
    'ok',
    `Expected all messages consumed but got ${result}: ${consumed}/${totalMessages} (${batchesProcessed} batches)`
  )
  ok(consumed >= totalMessages, `Expected at least ${totalMessages} messages but got ${consumed}`)
})
