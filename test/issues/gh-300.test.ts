// Related issue: https://github.com/platformatic/kafka/issues/300

import { deepStrictEqual, ok, strictEqual } from 'node:assert'
import { test } from 'node:test'
import { MessagesStreamModes, stringDeserializers } from '../../src/index.ts'
import { createConsumer, createTopic, isKafka, mockMethod, waitFor } from '../helpers.ts'

const skipConsumerGroupProtocol = { skip: isKafka(['7.5.0', '7.6.0', '7.7.0', '7.8.0', '7.9.0']) }

// consumer.#revokePartitions pauses and resumes every active stream
// on every rebalance. Until _construct's first #refreshOffsets
// finishes, #offsetsToFetch is empty; if a pause/resume cycle lands
// in that window, resume()'s nextTick(#fetch) reads #offsetsToFetch,
// gets undefined for every assigned partition, and crashes the
// consumer with "Cannot mix BigInt and other types". Calling
// pause/resume directly here is equivalent.

for (const groupProtocol of ['classic', 'consumer'] as const) {
  const options = groupProtocol === 'consumer' ? skipConsumerGroupProtocol : {}

  test(`survives pause/resume before initial offsets refresh completes (${groupProtocol})`, options, async t => {
    const topic = await createTopic(t, true, 3)

    const consumer = createConsumer(t, {
      deserializers: stringDeserializers,
      groupProtocol,
      // The retry budget only needs to be small enough to keep the initial refresh
      // in flight while we pause/resume. retries: 1 was tight enough that a single
      // slow ListOffsets under CI load failed the whole refresh.
      retries: 5,
      retryDelay: 100
    })
    await consumer.topics.trackAll(topic)

    let releaseInitialRefresh!: () => void
    const initialRefreshReleased = new Promise<void>(resolve => {
      releaseInitialRefresh = resolve
    })
    const initialRefreshStarted = new Promise<void>(resolve => {
      mockMethod(consumer, 'listOffsets', 1, undefined, undefined, (original, ...args) => {
        resolve()
        initialRefreshReleased.then(() => original(...args))
        return false
      })
    })

    const stream = await consumer.consume({
      topics: [topic],
      mode: MessagesStreamModes.LATEST,
      maxWaitTime: 200
    })
    // createConsumer registered its own `consumer.close(true)` hook before this one and hooks run in
    // registration order, so the consumer is closed while this stream is still consuming. That is
    // deliberate: Consumer.close(true) must close the active streams itself.
    t.after(() => stream.close())
    stream.on('data', () => {})

    // The stream has no other error listener, so without this one any stream error would be
    // rethrown by Node.js as an unhandled 'error' event. The test runner reports that as an
    // uncaught exception rather than as a failed assertion, which hides what actually went wrong.
    const errors: Error[] = []
    stream.on('error', error => errors.push(error))

    await initialRefreshStarted

    // The construct refresh is deliberately held in flight, so the offsets map is empty.
    strictEqual(
      stream.offsetsToFetch.size,
      0,
      'the initial offsets refresh should still be in flight right after consume()'
    )

    stream.pause()
    stream.resume()

    releaseInitialRefresh()
    await waitFor(() => ok(stream.offsetsToFetch.size > 0), { timeout: 10_000 })

    deepStrictEqual(
      errors.map(error => error.message),
      [],
      'the stream must not emit any error'
    )
    strictEqual(stream.errored, null, `the stream must not be destroyed, got ${stream.errored}`)
    ok(stream.offsetsToFetch.size > 0, 'the initial offsets refresh should have completed by now')
  })
}
