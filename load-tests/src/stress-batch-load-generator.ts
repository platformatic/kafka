import { randomUUID } from 'node:crypto'
import { pipeline } from 'node:stream/promises'
import { setTimeout } from 'node:timers/promises'
import type { z } from 'zod/v4'
import {
  Admin,
  Consumer,
  MessagesStreamModes,
  Producer,
  jsonSerializer,
  stringDeserializer,
  stringSerializer,
  type Message,
} from '../../src/index.ts'
import { config } from './config.ts'
import { MessageBatchStream } from './message-batch-stream.ts'
import { MetricsCollector } from './metrics-collector.ts'
import {
  STRESS_EVENT_SCHEMA,
  STRESS_LOG_SCHEMA,
  STRESS_METRIC_SCHEMA,
  STRESS_ORDER_SCHEMA,
  STRESS_TOPICS,
  type StressEvent,
  type StressLog,
  type StressMetric,
  type StressOrder,
} from './schemas.ts'
import { simulateWork, type WorkSimulatorOptions } from './work-simulator.ts'

// Replicates MQT's safeJsonDeserializer: returns undefined for invalid JSON instead of throwing
const safeJsonDeserializer = (data?: string | Buffer): object | undefined => {
  if (!data) return undefined
  if (!Buffer.isBuffer(data) && typeof data !== 'string') return undefined
  try {
    return JSON.parse(Buffer.isBuffer(data) ? data.toString('utf-8') : data)
  } catch {
    return undefined
  }
}

export interface StressBatchLoadTestOptions {
  rate: number
  duration: number
  batchSize: number
  consumerBatchSize: number
  consumerBatchTimeoutMs: number
  handlerDelayMs: number
  cpuWorkMs: number
  retryRate: number
  partitions: number
  payloadKb: number
  topics: number
  maxWaitTime: number
  maxBytes: number
}

type DeserializedMessage = Message<string, Record<string, unknown>, string, string>

interface HandlerConfig {
  schema: z.ZodType
  handler: (messages: DeserializedMessage[], metrics: MetricsCollector) => Promise<void>
}

function buildHandlers(workOptions: WorkSimulatorOptions): Record<string, HandlerConfig> {
  return {
    [STRESS_TOPICS.EVENTS]: {
      schema: STRESS_EVENT_SCHEMA,
      handler: async (messages, metrics) => {
        for (const message of messages) {
          const value = message.value as unknown as StressEvent
          await simulateWork(value as unknown as Record<string, unknown>, workOptions)
          const loadtestTs =
            typeof value.payload?.loadtest_ts === 'number' ? value.payload.loadtest_ts : undefined
          metrics.recordConsumed(STRESS_TOPICS.EVENTS, loadtestTs)
        }
      },
    },
    [STRESS_TOPICS.ORDERS]: {
      schema: STRESS_ORDER_SCHEMA,
      handler: async (messages, metrics) => {
        for (const message of messages) {
          const value = message.value as unknown as StressOrder
          await simulateWork(value as unknown as Record<string, unknown>, workOptions)
          metrics.recordConsumed(STRESS_TOPICS.ORDERS)
        }
      },
    },
    [STRESS_TOPICS.METRICS]: {
      schema: STRESS_METRIC_SCHEMA,
      handler: async (messages, metrics) => {
        for (const message of messages) {
          const value = message.value as unknown as StressMetric
          await simulateWork(value as unknown as Record<string, unknown>, workOptions)
          const loadtestTs =
            typeof value.tags?.loadtest_ts === 'number' ? value.tags.loadtest_ts : undefined
          metrics.recordConsumed(STRESS_TOPICS.METRICS, loadtestTs)
        }
      },
    },
    [STRESS_TOPICS.LOGS]: {
      schema: STRESS_LOG_SCHEMA,
      handler: async (messages, metrics) => {
        for (const message of messages) {
          const value = message.value as unknown as StressLog
          await simulateWork(value as unknown as Record<string, unknown>, workOptions)
          const loadtestTs =
            typeof value.context?.loadtest_ts === 'number' ? value.context.loadtest_ts : undefined
          metrics.recordConsumed(STRESS_TOPICS.LOGS, loadtestTs)
        }
      },
    },
  }
}

function generatePaddedPayload(sizeKb: number): string {
  const targetBytes = sizeKb * 1024
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
  let result = ''
  for (let i = 0; i < targetBytes; i++) {
    result += chars[i % chars.length]
  }
  return result
}

function generateStressEvent(index: number, padding: string): StressEvent {
  return {
    id: randomUUID(),
    event_type: `stress_test_${index % 5}`,
    payload: { loadtest_ts: Date.now(), index, data: padding },
    created_at: new Date().toISOString(),
  }
}

function generateStressOrder(index: number, padding: string): StressOrder {
  return {
    id: randomUUID(),
    customer_id: `customer-${(index % 100).toString().padStart(3, '0')}`,
    amount: (Math.random() * 1000).toFixed(2),
    status: ['pending', 'confirmed', 'shipped', 'delivered'][index % 4]!,
    created_at: new Date().toISOString() + padding.slice(0, 1),
  }
}

function generateStressMetric(index: number, padding: string): StressMetric {
  return {
    id: randomUUID(),
    metric_name: `stress_metric_${index % 10}`,
    value: Math.random() * 1000,
    tags: { loadtest_ts: Date.now(), index, data: padding },
    created_at: new Date().toISOString(),
  }
}

function generateStressLog(index: number, padding: string): StressLog {
  return {
    id: randomUUID(),
    level: ['debug', 'info', 'warn', 'error'][index % 4]!,
    message: `Stress log message ${index} ${padding.slice(0, 100)}`,
    context: { loadtest_ts: Date.now(), index, data: padding },
    created_at: new Date().toISOString(),
  }
}

function getActiveTopics(topicCount: number): string[] {
  const allTopics = [
    STRESS_TOPICS.EVENTS,
    STRESS_TOPICS.ORDERS,
    STRESS_TOPICS.METRICS,
    STRESS_TOPICS.LOGS,
  ]
  return allTopics.slice(0, Math.min(topicCount, allTopics.length))
}

async function ensureTopics(topics: string[], partitions: number): Promise<void> {
  const admin = new Admin({
    clientId: 'stress-batch-test-admin',
    bootstrapBrokers: config.kafka.bootstrapBrokers,
  })

  for (const topic of topics) {
    try {
      await admin.createTopics({ topics: [topic], partitions, replicas: 1 })
    } catch {
      // Topic may already exist
    }
  }

  await admin.close()
}

async function handleSyncStreamBatch(
  batchStream: MessageBatchStream<DeserializedMessage>,
  handlers: Record<string, HandlerConfig>,
  metrics: MetricsCollector,
  errorCount: { value: number },
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
      try {
        await handlerConfig.handler(validMessages, metrics)
      } catch (err) {
        errorCount.value++
        console.error(`[handler-error] #${errorCount.value} on ${topic}:`, err)
      }
    }

    const lastMessage = batch[batch.length - 1]!
    await lastMessage.commit()
  }
}

export async function runStressBatchLoadTest(options: StressBatchLoadTestOptions): Promise<void> {
  const {
    rate,
    duration,
    batchSize,
    consumerBatchSize,
    consumerBatchTimeoutMs,
    handlerDelayMs,
    cpuWorkMs,
    retryRate,
    partitions,
    payloadKb,
    topics: topicCount,
    maxWaitTime,
    maxBytes,
  } = options

  const activeTopics = getActiveTopics(topicCount)

  console.log(
    `Starting stress batch load test: ${rate} msgs/sec, ${duration}s, batch=${batchSize}, ` +
    `consumer-batch=${consumerBatchSize}, timeout=${consumerBatchTimeoutMs}ms, ` +
    `delay=${handlerDelayMs}ms, cpu=${cpuWorkMs}ms, retry=${(retryRate * 100).toFixed(0)}%, ` +
    `partitions=${partitions}, payload=${payloadKb}KB, topics=${activeTopics.length}, ` +
    `maxWaitTime=${maxWaitTime}ms, maxBytes=${(maxBytes / 1024 / 1024).toFixed(1)}MB`,
  )

  await ensureTopics(activeTopics, partitions)

  const metrics = new MetricsCollector()
  const errorCount = { value: 0 }
  const groupId = `stress-batch-load-test-${randomUUID()}`
  const padding = generatePaddedPayload(payloadKb)

  const workOptions: WorkSimulatorOptions = {
    handlerDelayMs,
    cpuWorkMs,
    retryRate,
  }
  const handlers = buildHandlers(workOptions)

  const producer = new Producer({
    clientId: `stress-batch-producer-${randomUUID()}`,
    bootstrapBrokers: config.kafka.bootstrapBrokers,
    serializers: {
      key: stringSerializer,
      value: jsonSerializer,
      headerKey: stringSerializer,
      headerValue: stringSerializer,
    },
  })

  // Use realistic fetch parameters matching MQT defaults:
  // - maxWaitTime: 5000ms (broker accumulates data before responding → large multi-chunk DynamicBuffers)
  // - maxBytes: 10MB (allows large fetch responses that arrive in many TCP packets)
  const consumer = new Consumer<string, Record<string, unknown>, string, string>({
    clientId: `stress-batch-consumer-${randomUUID()}`,
    groupId,
    bootstrapBrokers: config.kafka.bootstrapBrokers,
    deserializers: {
      key: stringDeserializer,
      value: safeJsonDeserializer,
      headerKey: stringDeserializer,
      headerValue: stringDeserializer,
    },
    autocommit: false,
    maxWaitTime,
    maxBytes,
  })

  console.log('Initializing Kafka batch consumer and producer...')

  // Replicate MQT's init sequence: joinGroup() before consume()
  await consumer.joinGroup()

  const consumerStream = await consumer.consume({
    topics: activeTopics,
    mode: MessagesStreamModes.LATEST,
  })

  const messageBatchStream = new MessageBatchStream<DeserializedMessage>({
    batchSize: consumerBatchSize,
    timeoutMilliseconds: consumerBatchTimeoutMs,
  })

  // Listen for stream errors to detect DynamicBuffer bug
  consumerStream.on('error', (error) => {
    errorCount.value++
    console.error(`[stream-error] #${errorCount.value}:`, error)
    if (error.cause) {
      console.error('[stream-error] cause:', error.cause)
    }
    if ('errors' in error && Array.isArray(error.errors)) {
      for (const aggregateError of error.errors) {
        console.error('[stream-error] aggregate:', aggregateError)
      }
    }
  })

  pipeline(consumerStream, messageBatchStream).catch((error) => {
    errorCount.value++
    console.error(`[pipeline-error] #${errorCount.value}:`, error)
  })

  handleSyncStreamBatch(messageBatchStream, handlers, metrics, errorCount).catch((error) => {
    errorCount.value++
    console.error(`[consumer-error] #${errorCount.value}:`, error)
  })

  console.log('Batch consumer and producer ready.')

  const reportInterval = setInterval(() => {
    metrics.report()
    if (errorCount.value > 0) {
      console.log(`  Stream errors: ${errorCount.value}`)
    }
  }, config.reportIntervalMs)

  const totalMessages = rate * duration
  console.log(`Publishing ${totalMessages.toLocaleString()} total messages at ${rate}/sec`)

  const loadStartTime = Date.now()
  let totalPublished = 0

  const generators = [
    (i: number) => ({ topic: STRESS_TOPICS.EVENTS, value: generateStressEvent(i, padding) }),
    (i: number) => ({ topic: STRESS_TOPICS.ORDERS, value: generateStressOrder(i, padding) }),
    (i: number) => ({ topic: STRESS_TOPICS.METRICS, value: generateStressMetric(i, padding) }),
    (i: number) => ({ topic: STRESS_TOPICS.LOGS, value: generateStressLog(i, padding) }),
  ].slice(0, activeTopics.length)

  while (totalPublished < totalMessages) {
    const remaining = totalMessages - totalPublished
    const currentBatch = Math.min(batchSize, remaining)

    try {
      const messages: Array<{
        topic: string
        key: string
        value: StressEvent | StressOrder | StressMetric | StressLog
      }> = []

      for (let i = 0; i < currentBatch; i++) {
        const generatorIndex = i % generators.length
        const generated = generators[generatorIndex]!(totalPublished + i)
        messages.push({
          topic: generated.topic,
          key: randomUUID(),
          value: generated.value,
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

  if (errorCount.value > 0) {
    console.log(`\n*** ${errorCount.value} stream error(s) detected during test ***`)
  }

  console.log('Shutting down...')
  await new Promise((resolve) => messageBatchStream.end(resolve))
  await Promise.all([consumer.close(), producer.close()])
  console.log('Done.')
}
