// Related issue: https://github.com/platformatic/kafka/issues/300

import { ok, strictEqual } from 'node:assert'
import { setTimeout as sleep } from 'node:timers/promises'
import { test } from 'node:test'
import { MessagesStreamModes, stringDeserializers } from '../../src/index.ts'
import { createConsumer, createTopic } from '../helpers.ts'

// consumer.#revokePartitions pauses and resumes every active stream
// on every rebalance. Until _construct's first #refreshOffsets
// finishes, #offsetsToFetch is empty; if a pause/resume cycle lands
// in that window, resume()'s nextTick(#fetch) reads #offsetsToFetch,
// gets undefined for every assigned partition, and crashes the
// consumer with "Cannot mix BigInt and other types". Calling
// pause/resume directly here is equivalent.

test('survives pause/resume before initial offsets refresh completes', async t => {
  const topic = await createTopic(t, true, 3)

  const consumer = createConsumer(t, {
    deserializers: stringDeserializers,
    groupProtocol: 'consumer',
    retries: 1,
    retryDelay: 50
  })
  await consumer.topics.trackAll(topic)

  const stream = await consumer.consume({
    topics: [topic],
    mode: MessagesStreamModes.LATEST,
    maxWaitTime: 200
  })
  t.after(() => stream.close())
  stream.on('data', () => {})

  // The construct refresh hasn't finished yet, so the offsets map is empty
  strictEqual(stream.offsetsToFetch.size, 0)

  stream.pause()
  stream.resume()

  await sleep(2000)

  strictEqual(stream.errored, null)
  ok(stream.offsetsToFetch.size > 0)
})
