import { ok, strictEqual } from 'node:assert'
import { performance } from 'node:perf_hooks'
import { test } from 'node:test'
import { MessagesStreamModes } from '../../src/index.ts'
import {
  assertContiguousIds,
  assertNoDuplicateIds,
  collectMessages,
  createConsumer,
  createTopic,
  produceJsonMessages,
  type ConsumedValue
} from '../helpers/index.ts'

test('regression #99: minBytes waits for maxWaitTime without dropping or duplicating messages', { timeout: 60_000 }, async t => {
  // Use a minBytes value larger than the available payload so the broker should
  // wait for maxWaitTime before returning the fetch response.
  const total = 5
  const topic = await createTopic(t, 1)
  await produceJsonMessages(t, topic, total)

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

  const startedAt = performance.now()
  const stream = await consumer.consume({
    topics: [topic],
    mode: MessagesStreamModes.EARLIEST,
    autocommit: false,
    minBytes: 1024 * 1024,
    maxWaitTime: 500
  })
  const messages = await collectMessages(stream, total)
  const elapsed = performance.now() - startedAt

  // The timing assertion catches bandwidth-amplification regressions where fetches
  // return immediately despite minBytes, while id assertions preserve correctness.
  ok(elapsed >= 400, `fetch returned before minBytes/maxWaitTime budget: ${elapsed}ms`)
  assertNoDuplicateIds(messages)
  assertContiguousIds(messages, total)
  strictEqual(messages.length, total)
})
