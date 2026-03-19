import { ok } from 'node:assert'
import { pipeline } from 'node:stream/promises'
import { test } from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import { type DeserializedMessage, MessageBatchStream, setupBackpressureTest } from '../helpers/backpressure.ts'
import { kafkaBootstrapServers } from '../helpers.ts'

/**
 * General memory leak test for the consumer stream under sustained backpressure.
 *
 * Uses the real pipeline + MessageBatchStream pattern with a slow async handler
 * to create sustained backpressure, then monitors heapUsed over time using the
 * undici fetch-leak.js "stabilization" pattern.
 *
 * This test catches the broad class of memory leaks that can occur during
 * backpressured consumption:
 *  - Retained references to already-consumed Message objects (e.g., closures
 *    in commit() holding entire batches, diagnostic channel references)
 *  - Growing internal maps (#offsetsToFetch, #offsetsToCommit, #inflightNodes,
 *    #partitionsEpochs) that accumulate entries without pruning
 *  - Closure allocations from the process.nextTick(#fetch) scheduling loop
 *  - Buffer leaks in the downstream MessageBatchStream
 *
 * Note: this test does NOT specifically catch the removal of the #paused guard
 * in #fetch() — that is covered by the behavioral test in
 * messages-stream-paused-guard.test.ts. The #inflightNodes per-broker
 * serialization bounds the observable effect with only 3 CI brokers, making
 * heap-based detection unreliable for that specific guard.
 *
 * Uses a 3-broker cluster with multiple partitions for realistic distribution.
 */

// ~1 KB payload per message
const PADDING = 'x'.repeat(900)

function forceGC (): void {
  // Double GC — Node may need multiple passes for closures, weak refs, etc.
  // Pattern from undici tls-cert-leak.js.
  global.gc!()
  global.gc!()
}

test('heap must stabilize under sustained backpressure', { timeout: 180_000 }, async t => {
  const batchSize = 500
  const handlerDelayMs = 200

  const { totalMessages, consumerStream, producer, topics } = await setupBackpressureTest(t, {
    topicCount: 15,
    messagesPerTopic: 2000,
    consumerHighWaterMark: 1024,
    messageValueFactory: id => ({ id, padding: PADDING }),
    bootstrapBrokers: kafkaBootstrapServers,
    partitionsPerTopic: 3,
    replicas: 3
  })

  const batchStream = new MessageBatchStream<DeserializedMessage>({
    batchSize,
    timeoutMilliseconds: 2000,
    readableHighWaterMark: 32
  })

  const pipelinePromise = pipeline(consumerStream, batchStream).catch(() => {})

  // Consume with slow async handler to sustain backpressure
  let consumed = 0
  const consumePromise = (async () => {
    for await (const messageBatch of batchStream) {
      const batch = messageBatch as DeserializedMessage[]

      for (const message of batch) {
        consumed++
        // eslint-disable-next-line no-void
        void message.value
      }

      await sleep(handlerDelayMs)

      const lastMessage = batch[batch.length - 1]!
      await lastMessage.commit()
    }
  })()

  // Let the pipeline warm up and enter steady-state backpressure
  forceGC()
  await sleep(5000)

  // Background publisher keeps Kafka fed so the fetch loop has data to
  // return throughout the monitoring window. Without this, the pre-published
  // corpus gets exhausted and the test can't detect leaks from ongoing fetches.
  const publishState = { stopped: false }
  const publishLoop = (async () => {
    let seq = 0
    while (!publishState.stopped) {
      try {
        await producer.send({
          messages: topics.map(topic => ({
            topic,
            key: `monitor-${seq++}`,
            value: { seq, padding: PADDING } as object
          }))
        })
      } catch {
        // Ignore errors during shutdown
      }
      await sleep(50)
    }
  })()

  // Heap stabilization check (undici fetch-leak.js pattern):
  // Take a baseline, then sample every interval. If heapUsed ever drops
  // or stays equal compared to the previous sample, memory has stabilized.
  // If it grows monotonically for maxConsecutiveGrowth samples, it's leaking.
  const sampleCount = 15
  const sampleIntervalMs = 1500
  const maxConsecutiveGrowth = 10

  const heapSamples: number[] = []

  // Baseline — not counted toward growth detection
  forceGC()
  heapSamples.push(process.memoryUsage().heapUsed)

  let consecutiveGrowth = 0
  let stabilized = false

  for (let i = 0; i < sampleCount; i++) {
    await sleep(sampleIntervalMs)

    forceGC()
    const heap = process.memoryUsage().heapUsed
    heapSamples.push(heap)

    const prevHeap = heapSamples[heapSamples.length - 2]!
    if (heap <= prevHeap) {
      stabilized = true
      consecutiveGrowth = 0
    } else {
      consecutiveGrowth++
    }
  }

  // Cleanup
  publishState.stopped = true
  await publishLoop
  await consumerStream.close()
  await Promise.all([pipelinePromise, consumePromise]).catch(() => {})

  ok(
    stabilized || consecutiveGrowth < maxConsecutiveGrowth,
    'Heap usage grew monotonically under backpressure — possible memory leak: ' +
      `${consecutiveGrowth} consecutive increases without stabilizing. ` +
      `Samples (MB): [${heapSamples.map(s => (s / (1024 * 1024)).toFixed(1)).join(', ')}]. ` +
      `Consumed ${consumed}/${totalMessages}+ messages during monitoring.`
  )
})
