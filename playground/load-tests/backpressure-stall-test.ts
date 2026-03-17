/**
 * Backpressure stall reproduction test.
 *
 * Reproduces the fetch loop stall that occurs when MessagesStream is piped
 * through a batch-accumulating Duplex (like MQT's KafkaMessageBatchStream)
 * with async handler processing.
 *
 * The stall happens because:
 * 1. pushRecords pushes messages → push() returns false → canPush=false
 *    → process.nextTick(#fetch) is NOT scheduled (canPush gate in #pushRecords)
 * 2. _read() fires → #fetch() → but pipeline already called pause() → #paused=true → returns
 * 3. resume() → #paused=false → super.resume() → _read() doesn't reliably fire
 *    because state.reading is already true from the failed _read() in step 2
 * 4. Fetch loop is dead. Messages remain unconsumed in Kafka.
 *
 * Setup mirrors a real-world production scenario:
 * - 15 topics, 1000 messages per topic (15,000 total)
 * - Consumer starts BEFORE publishing (fetches partial results as messages arrive)
 * - Messages published interleaved across topics (round-robin), in parallel batches of 500
 * - Small messages (~150 bytes, matching real CDC payloads — no padding)
 * - pipeline(consumerStream, batchStream) — same as MQT's AbstractKafkaConsumer
 * - for-await on batchStream with async handler processing per batch
 * - Single broker (all partitions on one node → single fetch returns all topics)
 */

import { randomUUID } from 'node:crypto'
import { pipeline } from 'node:stream/promises'
import { setTimeout } from 'node:timers/promises'
import {
  Admin,
  Consumer,
  MessagesStreamModes,
  Producer,
  jsonSerializer,
  stringDeserializer,
  stringSerializer,
  type Message
} from '../../src/index.ts'
import { config } from './config.ts'
import { MessageBatchStream } from './message-batch-stream.ts'
import { setTimeout as sleep } from 'node:timers/promises'

export interface BackpressureStallTestOptions {
  topicCount: number
  messagesPerTopic: number
  batchSize: number
  handlerDelayMs: number
  timeoutSeconds: number
  consumerHighWaterMark: number
}

type DeserializedMessage = Message<string, Record<string, unknown>, string, string>

export async function runBackpressureStallTest (options: BackpressureStallTestOptions): Promise<void> {
  const { topicCount, messagesPerTopic, batchSize, handlerDelayMs, timeoutSeconds, consumerHighWaterMark } = options
  const totalMessages = topicCount * messagesPerTopic

  console.log(`\n=== Backpressure Stall Test ===`)
  console.log(`  Topics: ${topicCount}`)
  console.log(`  Messages per topic: ${messagesPerTopic}`)
  console.log(`  Total: ${totalMessages}`)
  console.log(`  Batch size: ${batchSize}`)
  console.log(`  Handler delay: ${handlerDelayMs}ms`)
  console.log(`  Consumer highWaterMark: ${consumerHighWaterMark}`)
  console.log(`  Timeout: ${timeoutSeconds}s\n`)

  // Create topics
  const admin = new Admin({
    clientId: 'backpressure-test-admin',
    bootstrapBrokers: config.kafka.bootstrapBrokers
  })

  const topicPrefix = `bp-stall-${randomUUID().slice(0, 8)}`
  const topics: string[] = []
  for (let i = 0; i < topicCount; i++) {
    topics.push(`${topicPrefix}-${i}`)
  }

  for (const topic of topics) {
    try {
      await admin.createTopics({ topics: [topic], partitions: 1, replicas: 1 })
    } catch {
      // May already exist
    }
  }
  await admin.close()
  await setTimeout(500) // Wait for topics to propagate

  // Producer setup
  const producer = new Producer<string, object, string, string>({
    clientId: `bp-producer-${randomUUID()}`,
    bootstrapBrokers: config.kafka.bootstrapBrokers,
    serializers: {
      key: stringSerializer,
      value: jsonSerializer,
      headerKey: stringSerializer,
      headerValue: stringSerializer
    }
  })

  // Small CDC-like messages (no padding). With small messages (~150 bytes),
  // all 15000 fit in maxBytes (10MB). The consumer gets partial fetches
  // because it's already running while messages are still being published —
  // each fetch returns whatever's available at that moment (~300 msgs/topic).
  // With batchSize=500 and ~300 msgs/topic, batches span multiple topics →
  // multiple readable entries per flush → backpressure triggers.
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

  // Build all messages upfront, interleaved across topics (round-robin).
  // Each batch of 500 contains messages from all 15 topics.
  const PUBLISH_BATCH = 500
  const allMessages: { topic: string; key: string; value: object }[] = []
  const idBase = Math.round(Math.random() * 1_000_000)

  for (let i = 0; i < messagesPerTopic; i++) {
    for (let t = 0; t < topics.length; t++) {
      const factory = messageFactories[t % messageFactories.length]!
      allMessages.push({
        topic: topics[t]!,
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
    bootstrapBrokers: config.kafka.bootstrapBrokers,
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

      // Matches real handler: iterate messages synchronously to extract IDs
      for (const message of batch) {
        consumed++
        // Sync work: extract IDs, schema validation (simulated by object access)
        const value = message.value
        const _id = value?.id ?? value?.entry_id ?? value?.key_id ?? value?.form_id
      }

      // Async flush: 3 parallel Redis SADD calls (simulated with sleep)
      await sleep(handlerDelayMs)

      // Commit last message in batch
      const lastMessage = batch[batch.length - 1]!
      await lastMessage.commit()
    }
  })()

  console.log('Consumer started, now publishing...')

  // ── Publish AFTER consumer is running ──
  // Fire all batches concurrently (Promise.all) so messages arrive gradually
  // while the consumer is already fetching.
  const publishStart = performance.now()
  const publishPromises: Promise<unknown>[] = []

  for (let i = 0; i < allMessages.length; i += PUBLISH_BATCH) {
    const batch = allMessages.slice(i, i + PUBLISH_BATCH)
    publishPromises.push(producer.send({ messages: batch }))
  }

  await Promise.all(publishPromises)
  const publishMs = performance.now() - publishStart
  console.log(`Published ${totalMessages} messages in ${publishMs.toFixed(0)}ms`)

  // Monitor progress with stall detection
  const consumeStart = performance.now()
  const timeoutMs = timeoutSeconds * 1000
  let lastConsumed = 0
  let stallTicks = 0

  const result = await new Promise<'ok' | 'stall' | 'timeout'>(resolve => {
    const check = setInterval(() => {
      const elapsed = performance.now() - consumeStart
      const pct = ((consumed / totalMessages) * 100).toFixed(1)
      const rate = elapsed > 0 ? Math.round((consumed / elapsed) * 1000) : 0

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
          console.log(`\n  !!! STALL DETECTED at ${consumed}/${totalMessages} (${pct}%) — no progress for 5s`)
          resolve('stall')
          return
        }
      } else {
        stallTicks = 0
      }
      lastConsumed = consumed

      process.stdout.write(
        `\r  Consumed: ${consumed}/${totalMessages} (${pct}%) ${rate} msg/s  batches=${batchesProcessed}`
      )
    }, 500)
  })

  const totalMs = performance.now() - consumeStart
  console.log(`\n\n  Result: ${result.toUpperCase()}`)
  console.log(`  Consumed: ${consumed}/${totalMessages} (${((consumed / totalMessages) * 100).toFixed(1)}%)`)
  console.log(`  Duration: ${(totalMs / 1000).toFixed(1)}s`)
  console.log(`  Batches: ${batchesProcessed}`)
  if (consumed > 0) {
    console.log(`  Throughput: ${Math.round((consumed / totalMs) * 1000)} msg/s`)
  }

  // Cleanup
  await consumerStream.close()
  await Promise.all([pipelinePromise, consumePromise]).catch(() => {})
  await producer.close()
  await consumer.close()

  if (result === 'stall' || result === 'timeout') {
    console.log('\n  FAILED — fetch loop stalled under backpressure')
    process.exit(1)
  } else {
    console.log('\n  PASSED — all messages consumed')
    process.exit(0)
  }
}
