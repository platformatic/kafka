import { strictEqual } from 'node:assert'
import { test } from 'node:test'
import type { CallbackWithPromise } from '../../src/apis/callbacks.ts'
import { MessagesStreamModes } from '../../src/index.ts'
import {
  createConsumer,
  createProducer,
  createTopic,
  produceJsonMessages,
  type ConsumedValue
} from '../helpers/index.ts'

async function withTimeout<T> (promise: Promise<T>, timeoutMs: number): Promise<T> {
  // Shutdown regressions usually manifest as hung promises, so every close path in
  // this file is wrapped with an explicit timeout.
  let timer: NodeJS.Timeout | undefined
  const timeout = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => reject(new Error(`timed out after ${timeoutMs}ms`)), timeoutMs)
  })

  try {
    return await Promise.race([promise, timeout])
  } finally {
    clearTimeout(timer)
  }
}

function closeConsumer (consumer: { close: (force: boolean, callback: CallbackWithPromise<void>) => void }): Promise<void> {
  // Use the callback form so TypeScript does not pick the void overload for force close.
  return new Promise((resolve, reject) => {
    consumer.close(true, error => {
      if (error) reject(error)
      else resolve()
    })
  })
}

test('regression: producers and consumers close cleanly while work is in flight', { timeout: 60_000 }, async t => {
  // Keep a paused stream and an active producer request around, then verify every
  // resource still closes promptly.
  const topic = await createTopic(t, 1)
  await produceJsonMessages(t, topic, 50)

  const producer = createProducer(t)
  const consumer = createConsumer<string, ConsumedValue, string, string>(t, {
    deserializers: {
      key: data => data?.toString() ?? '',
      value: data => JSON.parse(data?.toString() ?? '{}') as ConsumedValue,
      headerKey: data => data?.toString() ?? '',
      headerValue: data => data?.toString() ?? ''
    }
  })

  await consumer.topics.trackAll(topic)
  await consumer.joinGroup()

  const stream = await consumer.consume({ topics: [topic], mode: MessagesStreamModes.EARLIEST, autocommit: true })
  const producePromise = producer.send({
    messages: Array.from({ length: 50 }, (_, id) => ({ topic, key: `close-${id}`, value: JSON.stringify({ id: id + 50 }) }))
  })

  let received = 0
  stream.on('data', () => {
    received++
    // Pausing after a few messages leaves the stream in a realistic half-consumed
    // state while close/teardown is exercised.
    if (received === 10) stream.pause()
  })

  await withTimeout(producePromise, 10_000)
  await withTimeout(stream.close(), 10_000)
  await withTimeout(closeConsumer(consumer), 10_000)
  await withTimeout(producer.close(), 10_000)

  strictEqual(stream.destroyed, true)
})
