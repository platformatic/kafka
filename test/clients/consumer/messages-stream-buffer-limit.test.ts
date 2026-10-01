import { ok, strictEqual } from 'node:assert'
import { randomUUID } from 'node:crypto'
import { test } from 'node:test'
import { setImmediate as nextTurn } from 'node:timers/promises'
import {
  Admin,
  Consumer,
  MessagesStreamModes,
  Producer,
  stringDeserializers,
  stringSerializers
} from '../../../src/index.ts'
import { kafkaSingleBootstrapServers } from '../../helpers.ts'

test('should stop expanding a fetched batch at highWaterMark and drain it in order', { timeout: 30_000 }, async t => {
  const topic = `buffer-limit-${randomUUID()}`
  const bootstrapBrokers = kafkaSingleBootstrapServers
  const admin = new Admin({ clientId: topic, bootstrapBrokers })
  t.after(() => admin.close())
  await admin.createTopics({ topics: [topic], partitions: 1, replicas: 1 })

  const producer = new Producer({ clientId: topic, bootstrapBrokers, serializers: stringSerializers })
  t.after(() => producer.close())
  const count = 2000
  await producer.send({ messages: Array.from({ length: count }, (_, i) => ({ topic, value: String(i) })) })

  let deserialized = 0
  const consumer = new Consumer({
    clientId: topic,
    groupId: topic,
    bootstrapBrokers,
    deserializers: {
      ...stringDeserializers,
      value (value) {
        deserialized++
        return value?.toString()
      }
    }
  })
  t.after(() => consumer.close(true))
  const stream = await consumer.consume({
    topics: [topic],
    mode: MessagesStreamModes.EARLIEST,
    autocommit: false,
    highWaterMark: 16
  })

  let received = 0
  let maximumBuffered = 0
  for await (const message of stream) {
    strictEqual(message.value, String(received))
    strictEqual(message.offset, BigInt(received))
    received++
    maximumBuffered = Math.max(maximumBuffered, stream.readableLength)
    ok(
      stream.readableLength <= stream.readableHighWaterMark,
      `readableLength ${stream.readableLength} exceeds ${stream.readableHighWaterMark}`
    )
    ok(deserialized - received <= stream.readableHighWaterMark, 'deserialization must stop with the readable buffer')
    if (received === count) break
    await nextTurn()
  }
  strictEqual(received, count)
  t.diagnostic(
    `received=${received}, maximumBuffered=${maximumBuffered}, highWaterMark=${stream.readableHighWaterMark}`
  )
})
