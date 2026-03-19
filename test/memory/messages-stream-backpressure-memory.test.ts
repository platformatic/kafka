import { ok } from 'node:assert'
import { pipeline } from 'node:stream/promises'
import { test } from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import { type DeserializedMessage, MessageBatchStream, setupBackpressureTest } from '../helpers/backpressure.ts'

/**
 * Memory leak test under sustained backpressure.
 *
 * Verifies that when a consumer stream is piped through a slow downstream,
 * memory usage stays bounded and does not balloon from buffered fetch responses.
 *
 * Uses the same MessageBatchStream + pipeline pattern as the stall reproduction
 * test, but with:
 *  - ~1 KB message payloads (vs ~150 B) to make buffer growth measurable
 *  - 5 000 messages/topic × 15 topics = 75 000 messages (~75 MB in Kafka)
 *  - 200 ms handler delay → ~2 500 msg/s throughput → ~50 000 consumed in 20 s
 *  - Corpus cannot be drained during the 20 s monitoring window
 *
 * If the fetch loop were leaking (e.g. fetching while paused and pushing
 * into an unbounded readable buffer), the remaining ~25 000+ messages
 * at ~1 KB each would cause visible, monotonic RSS growth.
 */

// ~1 KB payload per message — large enough to surface buffer growth over RSS noise
const PADDING = 'x'.repeat(900)

test('should not balloon memory under sustained backpressure', { timeout: 180_000 }, async t => {
  const batchSize = 500
  const handlerDelayMs = 200

  const { totalMessages, consumerStream } = await setupBackpressureTest(t, {
    topicCount: 15,
    messagesPerTopic: 5000,
    consumerHighWaterMark: 1024,
    messageValueFactory: id => ({ id, padding: PADDING })
  })

  const batchStream = new MessageBatchStream<DeserializedMessage>({
    batchSize,
    timeoutMilliseconds: 2000,
    readableHighWaterMark: 32
  })

  const pipelinePromise = pipeline(consumerStream, batchStream).catch(() => {})

  // Consume with slow async handler to keep backpressure sustained
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

  // Let fetches start and backpressure build up
  if (global.gc) global.gc()
  await sleep(3000)

  // Sample heapUsed (not RSS) over time while backpressure is sustained.
  // heapUsed after GC directly measures retained JS objects — the buffered
  // messages we're looking for. RSS includes shared libs, code segments,
  // and mmap'd memory that add noise masking real leaks.
  const samples: number[] = []
  const sampleCount = 10
  const sampleIntervalMs = 2000
  const monitoringDurationMs = sampleCount * sampleIntervalMs

  for (let i = 0; i < sampleCount; i++) {
    await sleep(sampleIntervalMs)

    if (global.gc) global.gc()

    samples.push(process.memoryUsage().heapUsed)
  }

  // Cleanup
  await consumerStream.close()
  await Promise.all([pipelinePromise, consumePromise]).catch(() => {})

  // Analyze memory trend: compare average of first third vs last third.
  //
  // With 75 MB of ~1 KB messages in Kafka, a slow handler (~2 500 msg/s),
  // and 20 s of monitoring, a leaking fetch loop would buffer tens of MB
  // of deserialized Message objects in the readable buffer — showing clear
  // monotonic heapUsed growth.
  //
  // Healthy behavior: heapUsed stays roughly flat (bounded by highWaterMark
  // + a few in-flight fetch responses). Typical heapUsed baseline during
  // this test is 30-60 MB; a leak would add 25+ MB on top.
  const thirdLen = Math.floor(samples.length / 3)
  const firstThird = samples.slice(0, thirdLen)
  const lastThird = samples.slice(samples.length - thirdLen)

  const avgFirst = firstThird.reduce((a, b) => a + b, 0) / firstThird.length
  const avgLast = lastThird.reduce((a, b) => a + b, 0) / lastThird.length

  const growthRatio = avgLast / avgFirst
  const absoluteGrowthMB = (avgLast - avgFirst) / (1024 * 1024)

  // Both conditions must hold. Using AND prevents one generous threshold
  // from masking a violation of the other:
  // - Ratio: heap should not more than double (accounts for GC timing jitter
  //   on a small baseline)
  // - Absolute: heap should not grow by more than 30 MB (a leak buffering
  //   25 000+ × ~1 KB messages would exceed this; normal churn stays well under)
  const maxAllowedGrowthRatio = 2.0
  const maxAllowedAbsoluteGrowthMB = 30
  const memoryOk = growthRatio <= maxAllowedGrowthRatio && absoluteGrowthMB <= maxAllowedAbsoluteGrowthMB

  ok(
    memoryOk,
    'Memory appears to be leaking under backpressure: ' +
      `heapUsed grew ${(growthRatio * 100 - 100).toFixed(1)}% (${absoluteGrowthMB.toFixed(1)} MB) ` +
      `over ${monitoringDurationMs / 1000}s. ` +
      `Samples (MB): [${samples.map(s => (s / (1024 * 1024)).toFixed(1)).join(', ')}]. ` +
      `Consumed ${consumed}/${totalMessages} messages during monitoring.`
  )
})
