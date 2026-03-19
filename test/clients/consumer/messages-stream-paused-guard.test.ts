import { ok } from 'node:assert'
import { test } from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import { setupBackpressureTest } from '../../helpers/backpressure.ts'
import { kafkaBootstrapServers } from '../../helpers.ts'

/**
 * Verifies the fetch loop respects the #paused guard.
 *
 * After pause() is called, the fetch loop must stop issuing new Kafka fetch
 * requests. Verified by counting 'fetch' events and checking readableLength
 * during a paused period with data continuously arriving in Kafka.
 *
 * Uses a 3-broker cluster with multiple partitions to cover the interaction
 * between the #paused guard and the per-broker #inflightNodes serialization.
 */

// ~1 KB payload per message
const PADDING = 'x'.repeat(900)

test('fetch loop must stop when stream is paused', { timeout: 120_000 }, async t => {
  const { consumerStream, producer, topics } = await setupBackpressureTest(t, {
    topicCount: 10,
    messagesPerTopic: 2000,
    consumerHighWaterMark: 1024,
    messageValueFactory: id => ({ id, padding: PADDING }),
    bootstrapBrokers: kafkaBootstrapServers,
    partitionsPerTopic: 3,
    replicas: 3
  })

  // Start flowing with a data listener so the fetch loop activates
  const receivedState = { count: 0 }
  const onData = () => {
    receivedState.count++
  }
  consumerStream.on('data', onData)

  // Wait for messages to flow — confirms the fetch loop is active
  while (receivedState.count < 500) {
    await sleep(100)
  }

  // Pause the stream
  consumerStream.removeListener('data', onData)
  consumerStream.pause()

  // Wait a fixed period for in-flight fetch responses to complete.
  // With 3 brokers, at most 3 in-flight fetches. Each completes within
  // maxWaitTime (1000ms) + network RTT. 5 seconds is generous.
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
            value: { seq, padding: PADDING } as object
          }))
        })
      } catch {
        // Ignore errors during shutdown
      }
      await sleep(50)
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
  // A small tolerance (3) accounts for edge cases: a metadata callback
  // already in-flight when we started counting.
  ok(
    fetchesDuringPause <= 3,
    'Fetch loop continued firing while stream was paused: ' +
      `${fetchesDuringPause} fetch events in ${monitorDurationMs / 1000}s. ` +
      'Expected near-zero when #paused guard is active.'
  )

  // Assert 2: readableLength must not have grown.
  // No readers + no fetches = static buffer.
  const bufferGrowth = readableLengthAfter - readableLengthBefore
  ok(
    bufferGrowth <= 0,
    'readableLength grew while stream was paused: ' +
      `from ${readableLengthBefore} to ${readableLengthAfter} (+${bufferGrowth}). ` +
      'Fetch loop pushed records into a paused stream.'
  )
})
