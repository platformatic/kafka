import { ok } from 'node:assert'
import { test } from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import { setupBackpressureTest } from '../../helpers/backpressure.ts'

/**
 * Verifies the fetch loop respects the #paused guard.
 *
 * After pause() is called, the fetch loop must stop issuing new Kafka fetch
 * requests. Verified by counting 'fetch' events and checking readableLength
 * during a paused period with data continuously arriving in Kafka.
 *
 * Uses a single-broker setup with small payloads for fast, reliable CI runs.
 * The single broker is sufficient: with the #paused guard removed, the test
 * observed 1003 fetch events in 10 seconds even with one broker.
 */

test('fetch loop must stop when stream is paused', { timeout: 60_000 }, async t => {
  const { consumerStream, producer, topics } = await setupBackpressureTest(t, {
    topicCount: 3,
    messagesPerTopic: 500,
    consumerHighWaterMark: 1024
  })

  // Start flowing with a data listener so the fetch loop activates
  const receivedState = { count: 0 }
  const onData = () => {
    receivedState.count++
  }
  consumerStream.on('data', onData)

  // Wait for messages to flow — confirms the fetch loop is active
  while (receivedState.count < 100) {
    await sleep(100)
  }

  // Pause the stream
  consumerStream.removeListener('data', onData)
  consumerStream.pause()

  // Wait for in-flight fetch responses to complete. A fetch cycle involves:
  // metadata (async) -> build requests -> send fetch (up to maxWaitTime=1000ms)
  // -> response callback. 5 seconds covers the worst case of a metadata callback
  // already in the event loop triggering one more fetch cycle.
  await sleep(5000)

  // Now start counting: fetch events and buffer growth during pause
  let fetchesDuringPause = 0
  const onFetch = () => {
    fetchesDuringPause++
  }
  consumerStream.on('fetch', onFetch)

  const readableLengthBefore = consumerStream.readableLength

  // Keep Kafka fed so a leaking fetch loop would have data to return
  const publishState = { stopped: false }
  const publishLoop = (async () => {
    let seq = 0
    while (!publishState.stopped) {
      try {
        await producer.send({
          messages: topics.map(topic => ({
            topic,
            key: `paused-${seq++}`,
            value: { id: seq } as object
          }))
        })
      } catch {
        // Ignore errors during shutdown
      }
      await sleep(100)
    }
  })()

  // Monitor for 10 seconds while paused
  const monitorDurationMs = 10_000
  await sleep(monitorDurationMs)

  const readableLengthAfter = consumerStream.readableLength

  // Cleanup
  consumerStream.removeListener('fetch', onFetch)
  publishState.stopped = true
  await publishLoop
  await consumerStream.close()

  // Assert 1: No (or very few) fetch events while paused.
  ok(
    fetchesDuringPause <= 3,
    'Fetch loop continued firing while stream was paused: ' +
      `${fetchesDuringPause} fetch events in ${monitorDurationMs / 1000}s. ` +
      'Expected near-zero when #paused guard is active.'
  )

  // Assert 2: readableLength must not have grown.
  const bufferGrowth = readableLengthAfter - readableLengthBefore
  ok(
    bufferGrowth <= 0,
    'readableLength grew while stream was paused: ' +
      `from ${readableLengthBefore} to ${readableLengthAfter} (+${bufferGrowth}). ` +
      'Fetch loop pushed records into a paused stream.'
  )
})
