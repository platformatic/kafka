import { ok } from 'node:assert'
import { test } from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import { setupBackpressureTest } from '../../helpers/backpressure.ts'
import { mockMethod, waitFor } from '../../helpers.ts'

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
  const { consumer, consumerStream, producer, topics } = await setupBackpressureTest(t, {
    topicCount: 3,
    messagesPerTopic: 500,
    consumerHighWaterMark: 1024
  })

  let fetchCalls = 0
  let inFlightFetches = 0
  mockMethod(consumer, 'fetch', () => true, undefined, undefined, (original, ...args) => {
    fetchCalls++
    inFlightFetches++
    const callback = args.at(-1)
    args[args.length - 1] = (error: Error | null, response: unknown) => {
      inFlightFetches--
      callback(error, response)
    }
    original(...args)
    return true
  })

  // Start flowing with a data listener so the fetch loop activates
  const receivedState = { count: 0 }
  const onData = () => {
    receivedState.count++
  }
  consumerStream.on('data', onData)

  // Wait for messages to flow — confirms the fetch loop is active
  await waitFor(() => {
    if (receivedState.count < 100) {
      throw new Error(`received ${receivedState.count}/100 messages`)
    }
  }, { interval: 100, timeout: 30_000 })

  // Pause the stream
  consumerStream.removeListener('data', onData)
  consumerStream.pause()

  // Exclude responses already in flight when pause() was called. The invariant is that no new
  // fetch request is started after the stream enters the paused state.
  await waitFor(() => {
    if (inFlightFetches > 0) {
      throw new Error(`${inFlightFetches} fetches are still in flight`)
    }
  }, { interval: 100, timeout: 10_000 })
  const fetchCallsBeforePause = fetchCalls

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

  const monitorDurationMs = 10_000
  let readableLengthAfter = readableLengthBefore

  try {
    await sleep(monitorDurationMs)
    readableLengthAfter = consumerStream.readableLength
  } finally {
    publishState.stopped = true
    await producer.close().catch(() => {})
    await publishLoop.catch(() => {})
    await consumerStream.close().catch(() => {})
  }

  const fetchesDuringPause = fetchCalls - fetchCallsBeforePause

  // Assert 1: No (or very few) fetch events while paused.
  ok(
    fetchesDuringPause === 0,
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
