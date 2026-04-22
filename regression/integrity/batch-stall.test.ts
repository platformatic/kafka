import { ok, strictEqual } from 'node:assert'
import { pipeline } from 'node:stream/promises'
import { test } from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import { type DeserializedMessage, MessageBatchStream, setupBackpressureTest } from '../../test/helpers/backpressure.ts'
import { assertResourceStability, createResourceSampler, writeRegressionArtifact } from '../helpers/index.ts'

async function consumeSlowBatches (batchStream: MessageBatchStream<DeserializedMessage>, progress: { consumed: number; batches: number }) {
  for await (const messageBatch of batchStream) {
    const batch = messageBatch as DeserializedMessage[]
    progress.consumed += batch.length
    progress.batches++
    await sleep(10)
    await batch.at(-1)!.commit()
  }
}

test('regression #228: long-running batch consumer does not stop making progress', { timeout: 120_000 }, async t => {
  // This is a longer-running variant of the backpressure regression that favors
  // batch handler latency over raw volume, matching historical stall reports.
  const { totalMessages, consumerStream } = await setupBackpressureTest(t, {
    topicCount: 3,
    messagesPerTopic: 300,
    consumerHighWaterMark: 256,
    publishBatchSize: 100,
    publishConcurrency: 2
  })
  const batchStream = new MessageBatchStream<DeserializedMessage>({
    batchSize: 75,
    timeoutMilliseconds: 1000,
    readableHighWaterMark: 4
  })
  const progress = { consumed: 0, batches: 0 }
  const sampler = createResourceSampler(100)
  const pipelinePromise = pipeline(consumerStream, batchStream).catch(() => {})
  const consumePromise = consumeSlowBatches(batchStream, progress)
  let lastConsumed = 0
  let stalledIntervals = 0

  while (progress.consumed < totalMessages) {
    await sleep(1000)

    if (progress.consumed === lastConsumed) {
      stalledIntervals++
    } else {
      stalledIntervals = 0
    }

    strictEqual(stalledIntervals < 5, true, `batch consumer stalled at ${progress.consumed}/${totalMessages}`)
    lastConsumed = progress.consumed
  }

  await consumerStream.close()
  await Promise.all([pipelinePromise, consumePromise]).catch(() => {})
  const samples = sampler.stop()

  await writeRegressionArtifact('batch-stall-resource-stability', { samples, ...progress, totalMessages })

  strictEqual(progress.consumed >= totalMessages, true)
  ok(progress.batches > 1)
  assertResourceStability(samples, { minSamples: 2 })
})
