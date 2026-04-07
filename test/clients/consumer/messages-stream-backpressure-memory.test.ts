import { ok } from 'node:assert'
import { randomUUID } from 'node:crypto'
import { test } from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import {
  Admin,
  Consumer,
  MessagesStreamModes,
  Producer,
  jsonSerializer,
  stringDeserializer,
  stringSerializer
} from '../../../src/index.ts'
import { kafkaSingleBootstrapServers } from '../../helpers.ts'

/**
 * Backpressure memory test.
 *
 * Verifies that the consumer stream does not buffer an unbounded number of
 * messages when consumed via `for await` with slow processing. Without the
 * canPush gate (#260 fix), the stream ignores push() returning false and
 * keeps fetching, causing readableLength to grow to the total message count.
 *
 * Uses large messages (16 KB each) and a small maxBytes (1 MB) to force
 * many separate Kafka fetches — the fix prevents scheduling the next fetch
 * when the buffer is already over highWaterMark.
 */

test('should respect backpressure and not buffer all messages in memory', { timeout: 120_000 }, async t => {
  const highWaterMark = 1
  const messageCount = 5000
  const messageSize = 16_384 // 16 KB per message
  const maxBytes = 1_048_576 // 1 MB max fetch — forces many small fetches
  const prefix = `bp-mem-${randomUUID().slice(0, 8)}`
  const topic = `${prefix}-0`

  // Create topic
  const admin = new Admin({
    clientId: `${prefix}-admin`,
    bootstrapBrokers: kafkaSingleBootstrapServers
  })
  t.after(() => admin.close())

  try {
    await admin.createTopics({ topics: [topic], partitions: 1, replicas: 1 })
  } catch {
    // May already exist
  }
  await sleep(500)

  // Produce large messages before consumer starts
  const producer = new Producer<string, object, string, string>({
    clientId: `${prefix}-producer`,
    bootstrapBrokers: kafkaSingleBootstrapServers,
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

  const payload = 'x'.repeat(messageSize)
  const batchSize = 50
  for (let i = 0; i < messageCount; i += batchSize) {
    const messages = []
    for (let j = i; j < Math.min(i + batchSize, messageCount); j++) {
      messages.push({ topic, key: `key-${j}`, value: { data: payload, id: j } })
    }
    await producer.send({ messages })
  }

  // Consumer setup with tight highWaterMark
  const consumer = new Consumer<string, Record<string, unknown>, string, string>({
    clientId: `${prefix}-consumer`,
    groupId: `${prefix}-group-${randomUUID()}`,
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
    sessionTimeout: 60_000,
    heartbeatInterval: 3_000
  })
  t.after(() => consumer.close(true))

  await consumer.topics.trackAll(topic)
  await consumer.joinGroup()

  const consumerStream = await consumer.consume({
    topics: [topic],
    mode: MessagesStreamModes.EARLIEST,
    maxWaitTime: 1000,
    maxBytes,
    autocommit: true,
    highWaterMark
  })

  // Track the maximum readableLength observed during consumption
  let maxReadableLength = 0
  const origPush = consumerStream.push.bind(consumerStream)
  consumerStream.push = function (...args: Parameters<typeof origPush>) {
    const result = origPush(...args)
    const len = consumerStream.readableLength
    if (len > maxReadableLength) {
      maxReadableLength = len
    }
    return result
  } as typeof origPush

  // Consume with for-await and slow processing — this is the pattern from #260.
  // Unlike pipeline(), for-await does not call pause()/resume(), so backpressure
  // relies entirely on the canPush gate checking push()'s return value.
  //
  // We consume for a fixed duration rather than waiting for all messages,
  // since the test is about buffer growth, not total throughput.
  let consumed = 0
  const minConsumed = 500 // Enough to observe multiple fetch cycles

  for await (const _message of consumerStream) {
    consumed++
    // Simulate slow processing — each message takes 5ms
    await sleep(5)

    if (consumed >= minConsumed) {
      break
    }
  }

  await consumerStream.close()

  ok(consumed >= minConsumed, `Expected at least ${minConsumed} messages consumed but got ${consumed}`)

  // Without the fix, maxReadableLength grows to nearly messageCount (e.g. ~5000)
  // because the stream keeps fetching despite push() returning false.
  // With backpressure working correctly, it should stay well below that —
  // bounded by the number of messages that fit in a single fetch response
  // (maxBytes / messageSize ≈ 64 messages per fetch).
  const maxAcceptableBuffered = Math.floor(messageCount / 2)
  ok(
    maxReadableLength < maxAcceptableBuffered,
    `Backpressure not respected: readableLength reached ${maxReadableLength} ` +
      `(limit: ${maxAcceptableBuffered}, total: ${messageCount}). ` +
      'The stream buffered too many messages instead of pausing fetches.'
  )
})
