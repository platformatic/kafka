import { randomUUID } from 'node:crypto'
import { pipeline } from 'node:stream/promises'
import { setTimeout } from 'node:timers/promises'
import type { z } from 'zod/v4'
import {
  Admin,
  Consumer,
  MessagesStreamModes,
  Producer,
  jsonDeserializer,
  jsonSerializer,
  stringDeserializer,
  stringSerializer,
  type Message,
} from '../../src/index.ts'
import { config } from './config.ts'
import { MessageBatchStream } from './message-batch-stream.ts'
import { MetricsCollector } from './metrics-collector.ts'
import {
  DIRECT_EVENT_SCHEMA,
  DIRECT_ORDER_SCHEMA,
  TOPICS,
  type DirectEvent,
  type DirectOrder,
} from './schemas.ts'

export interface BatchLoadTestOptions {
  rate: number
  duration: number
  batchSize: number
  consumerBatchSize: number
  consumerBatchTimeoutMs: number
}

type DeserializedMessage = Message<string, Record<string, unknown>, string, string>

// Replicates MQT's handler config: schema + handler function per topic
interface HandlerConfig {
  schema: z.ZodType
  handler: (messages: DeserializedMessage[], metrics: MetricsCollector) => void
}

const handlers: Record<string, HandlerConfig> = {
  [TOPICS.EVENTS]: {
    schema: DIRECT_EVENT_SCHEMA,
    handler: (messages, metrics) => {
      console.log(
        `[${new Date().toISOString()}] [batch-handler] direct-events batch received: ${messages.length} messages`,
      )
      for (const message of messages) {
        const value = message.value as unknown as DirectEvent
        const loadtestTs =
          typeof value.payload?.loadtest_ts === 'number' ? value.payload.loadtest_ts : undefined
        metrics.recordConsumed(TOPICS.EVENTS, loadtestTs)
      }
    },
  },
  [TOPICS.ORDERS]: {
    schema: DIRECT_ORDER_SCHEMA,
    handler: (messages, metrics) => {
      console.log(
        `[${new Date().toISOString()}] [batch-handler] direct-orders batch received: ${messages.length} messages`,
      )
      for (const _message of messages) {
        metrics.recordConsumed(TOPICS.ORDERS)
      }
    },
  },
}

function generateEvent(index: number): DirectEvent {
  return {
    id: randomUUID(),
    event_type: `load_test_${index % 5}`,
    payload: { loadtest_ts: Date.now(), index, data: `event-payload-${index}` },
    created_at: new Date().toISOString(),
  }
}

function generateOrder(index: number): DirectOrder {
  return {
    id: randomUUID(),
    customer_id: `customer-${(index % 100).toString().padStart(3, '0')}`,
    amount: (Math.random() * 1000).toFixed(2),
    status: ['pending', 'confirmed', 'shipped', 'delivered'][index % 4]!,
    created_at: new Date().toISOString(),
  }
}

async function ensureTopics(): Promise<void> {
  const admin = new Admin({
    clientId: 'load-test-admin',
    bootstrapBrokers: config.kafka.bootstrapBrokers,
  })

  for (const topic of [TOPICS.EVENTS, TOPICS.ORDERS]) {
    try {
      await admin.createTopics({ topics: [topic], partitions: 3, replicas: 1 })
    } catch {
      // Topic may already exist
    }
  }

  await admin.close()
}

// Replicates MQT's handleSyncStreamBatch + consume + parseMessages flow
async function handleSyncStreamBatch(
  batchStream: MessageBatchStream<DeserializedMessage>,
  metrics: MetricsCollector,
): Promise<void> {
  for await (const messageBatch of batchStream) {
    const batch = messageBatch as DeserializedMessage[]
    const topic = batch[0]!.topic

    const handlerConfig = handlers[topic]
    if (!handlerConfig) {
      const lastMessage = batch[batch.length - 1]!
      await lastMessage.commit()
      continue
    }

    // Zod validation per message (replicates MQT's parseMessages with safeParse)
    const validMessages: DeserializedMessage[] = []
    for (const message of batch) {
      if (!message.value) continue

      const parseResult = handlerConfig.schema.safeParse(message.value)
      if (!parseResult.success) {
        console.error(`[validation] Invalid message on ${topic}:`, parseResult.error.message)
        continue
      }
      validMessages.push(message)
    }

    if (validMessages.length > 0) {
      // Call batch handler with validated messages (replicates MQT's tryToConsume)
      handlerConfig.handler(validMessages, metrics)
    }

    // Commit last message in batch (replicates MQT's commit for batch)
    const lastMessage = batch[batch.length - 1]!
    await lastMessage.commit()
  }
}

export async function runDirectBatchLoadTest(options: BatchLoadTestOptions): Promise<void> {
  const { rate, duration, batchSize, consumerBatchSize, consumerBatchTimeoutMs } = options

  console.log(
    `Starting direct Kafka batch load test: ${rate} msgs/sec, ${duration}s duration, publish batch=${batchSize}, consumer batch=${consumerBatchSize}, timeout=${consumerBatchTimeoutMs}ms`,
  )

  await ensureTopics()

  const metrics = new MetricsCollector()
  const groupId = `direct-batch-load-test-${randomUUID()}`

  const producer = new Producer({
    clientId: `direct-producer-${randomUUID()}`,
    bootstrapBrokers: config.kafka.bootstrapBrokers,
    serializers: {
      key: stringSerializer,
      value: jsonSerializer,
      headerKey: stringSerializer,
      headerValue: stringSerializer,
    },
  })

  const consumer = new Consumer<string, Record<string, unknown>, string, string>({
    clientId: `direct-batch-consumer-${randomUUID()}`,
    groupId,
    bootstrapBrokers: config.kafka.bootstrapBrokers,
    deserializers: {
      key: stringDeserializer,
      value: jsonDeserializer,
      headerKey: stringDeserializer,
      headerValue: stringDeserializer,
    },
    autocommit: false,
    maxWaitTime: 5,
  })

  console.log('Initializing Kafka batch consumer and producer...')

  const consumerStream = await consumer.consume({
    topics: [TOPICS.EVENTS, TOPICS.ORDERS],
    mode: MessagesStreamModes.LATEST,
  })

  // Create batch stream and pipe consumer stream through it (replicates MQT pattern)
  const messageBatchStream = new MessageBatchStream<DeserializedMessage>({
    batchSize: consumerBatchSize,
    timeoutMilliseconds: consumerBatchTimeoutMs,
  })

  // Use pipeline for backpressure management (same as MQT)
  pipeline(consumerStream, messageBatchStream).catch((error) => {
    console.error('Pipeline error:', error)
  })

  // Start batch consumption loop in background (replicates MQT's handleSyncStreamBatch)
  handleSyncStreamBatch(messageBatchStream, metrics).catch((error) => {
    console.error('Batch consumer stream error:', error)
  })

  console.log('Batch consumer and producer ready.')

  const reportInterval = setInterval(() => metrics.report(), config.reportIntervalMs)

  const totalMessages = rate * duration
  console.log(`Publishing ${totalMessages.toLocaleString()} total messages at ${rate}/sec`)

  const loadStartTime = Date.now()
  let totalPublished = 0

  while (totalPublished < totalMessages) {
    const remaining = totalMessages - totalPublished
    const currentBatch = Math.min(batchSize, remaining)
    const eventCount = Math.ceil(currentBatch / 2)
    const orderCount = currentBatch - eventCount

    try {
      const messages: Array<{
        topic: string
        key: string
        value: DirectEvent | DirectOrder
      }> = []

      for (let i = 0; i < eventCount; i++) {
        messages.push({
          topic: TOPICS.EVENTS,
          key: randomUUID(),
          value: generateEvent(totalPublished + i),
        })
      }
      for (let i = 0; i < orderCount; i++) {
        messages.push({
          topic: TOPICS.ORDERS,
          key: randomUUID(),
          value: generateOrder(totalPublished + eventCount + i),
        })
      }

      await producer.send({ messages })
      totalPublished += currentBatch
      metrics.recordProduced(currentBatch)
    } catch (err) {
      console.error('Publish error:', err)
    }

    if (rate > 0) {
      const elapsed = Date.now() - loadStartTime
      const expectedElapsed = (totalPublished / rate) * 1000
      const sleepMs = expectedElapsed - elapsed
      if (sleepMs > 0) {
        await setTimeout(sleepMs)
      }
    }
  }

  console.log(`\nPublishing complete. ${totalPublished.toLocaleString()} messages published.`)
  console.log(`Waiting up to ${config.drainTimeoutMs / 1000}s for consumer to drain...`)

  const drainStart = Date.now()
  while (metrics.backlog > 0 && Date.now() - drainStart < config.drainTimeoutMs) {
    await setTimeout(500)
  }

  clearInterval(reportInterval)
  metrics.printFinalReport()

  console.log('Shutting down...')
  await new Promise((resolve) => messageBatchStream.end(resolve))
  await Promise.all([consumer.close(), producer.close()])
  console.log('Done.')
}
