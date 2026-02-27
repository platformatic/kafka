import { randomUUID } from 'node:crypto'
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
  type MessagesStream,
} from '../../src/index.ts'
import { config } from './config.ts'
import { MetricsCollector } from './metrics-collector.ts'
import {
  DIRECT_EVENT_SCHEMA,
  DIRECT_ORDER_SCHEMA,
  TOPICS,
  type DirectEvent,
  type DirectOrder,
} from './schemas.ts'

export interface LoadTestOptions {
  rate: number
  duration: number
  batchSize: number
}

type DeserializedMessage = Message<string, Record<string, unknown>, string, string>

// Replicates MQT's handler config: schema + handler function per topic
interface HandlerConfig {
  schema: z.ZodType
  handler: (message: DeserializedMessage, metrics: MetricsCollector) => void
}

const handlers: Record<string, HandlerConfig> = {
  [TOPICS.EVENTS]: {
    schema: DIRECT_EVENT_SCHEMA,
    handler: (message, metrics) => {
      const value = message.value as unknown as DirectEvent
      const loadtestTs =
        typeof value.payload?.loadtest_ts === 'number' ? value.payload.loadtest_ts : undefined
      metrics.recordConsumed(TOPICS.EVENTS, loadtestTs)
    },
  },
  [TOPICS.ORDERS]: {
    schema: DIRECT_ORDER_SCHEMA,
    handler: (_message, metrics) => {
      metrics.recordConsumed(TOPICS.ORDERS)
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

// Replicates MQT's handleSyncStream + consume + parseMessages flow
async function handleSyncStream(
  stream: MessagesStream<string, Record<string, unknown>, string, string>,
  metrics: MetricsCollector,
): Promise<void> {
  for await (const message of stream) {
    const handlerConfig = handlers[message.topic]
    if (!handlerConfig) {
      await message.commit()
      continue
    }

    // Zod validation (replicates MQT's parseMessages with safeParse)
    if (!message.value) {
      await message.commit()
      continue
    }

    const parseResult = handlerConfig.schema.safeParse(message.value)
    if (!parseResult.success) {
      console.error(`[validation] Invalid message on ${message.topic}:`, parseResult.error.message)
      await message.commit()
      continue
    }

    // Call handler with validated message (replicates MQT's tryToConsume)
    handlerConfig.handler(message, metrics)

    // Manual commit (replicates MQT's commitMessage)
    await message.commit()
  }
}

export async function runDirectLoadTest(options: LoadTestOptions): Promise<void> {
  const { rate, duration, batchSize } = options

  console.log(
    `Starting direct Kafka load test: ${rate} msgs/sec, ${duration}s duration, batch=${batchSize}`,
  )

  await ensureTopics()

  const metrics = new MetricsCollector()
  const groupId = `direct-load-test-${randomUUID()}`

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
    clientId: `direct-consumer-${randomUUID()}`,
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

  console.log('Initializing Kafka consumer and producer...')

  const stream = await consumer.consume({
    topics: [TOPICS.EVENTS, TOPICS.ORDERS],
    mode: MessagesStreamModes.LATEST,
  })

  // Start consumption loop in background (replicates MQT's handleSyncStream)
  handleSyncStream(stream, metrics).catch((error) => {
    console.error('Consumer stream error:', error)
  })

  console.log('Consumer and producer ready.')

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

    const elapsed = Date.now() - loadStartTime
    const expectedElapsed = (totalPublished / rate) * 1000
    const sleepMs = expectedElapsed - elapsed
    if (sleepMs > 0) {
      await setTimeout(sleepMs)
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
  await Promise.all([consumer.close(), producer.close()])
  console.log('Done.')
}
