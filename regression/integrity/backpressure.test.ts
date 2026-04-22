import { ok, strictEqual } from 'node:assert'
import { pipeline } from 'node:stream/promises'
import { test } from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import { type DeserializedMessage, MessageBatchStream, setupBackpressureTest } from '../../test/helpers/backpressure.ts'
import { assertResourceStability, createResourceSampler, writeRegressionArtifact } from '../helpers/index.ts'

async function consumeBatches (batchStream: MessageBatchStream<DeserializedMessage>, onBatch: (batch: DeserializedMessage[]) => Promise<void>) {
  for await (const messageBatch of batchStream) {
    await onBatch(messageBatch as DeserializedMessage[])
  }
}

test('regression #260: consumer pipeline backpressure keeps making progress without unbounded buffering', { timeout: 120_000 }, async t => {
  // This mirrors the production shape that previously stalled: a consumer stream
  // piped into a batch-accumulating Duplex with slower async batch handling.
  const { totalMessages, consumerStream } = await setupBackpressureTest(t, {
    topicCount: 4,
    messagesPerTopic: 200,
    consumerHighWaterMark: 128,
    publishBatchSize: 100,
    publishConcurrency: 2
  })
  const batchStream = new MessageBatchStream<DeserializedMessage>({
    batchSize: 100,
    timeoutMilliseconds: 1000,
    readableHighWaterMark: 8
  })
  const sampler = createResourceSampler(100)
  const pipelinePromise = pipeline(consumerStream, batchStream).catch(() => {})

  let consumed = 0
  let maxReadableLength = 0
  const consumePromise = consumeBatches(batchStream, async batch => {
    // Track the largest readable queue observed so the test fails on buffering
    // regressions, not only on complete stalls.
    maxReadableLength = Math.max(maxReadableLength, batchStream.readableLength)
    consumed += batch.length
    await sleep(2)
    await batch.at(-1)!.commit()
  })

  const startedAt = Date.now()
  let lastConsumed = 0
  let noProgressIntervals = 0

  while (true) {
    if (consumed >= totalMessages || Date.now() - startedAt >= 60_000) {
      break
    }

    await sleep(500)

    // Treat repeated no-progress intervals as a dead fetch loop. A healthy stream
    // can slow down under backpressure, but it must continue moving.
    if (consumed === lastConsumed && consumed > 0) {
      noProgressIntervals++
    } else {
      noProgressIntervals = 0
    }

    strictEqual(noProgressIntervals < 10, true, `stalled at ${consumed}/${totalMessages}`)
    lastConsumed = consumed
  }

  await consumerStream.close()
  await Promise.all([pipelinePromise, consumePromise]).catch(() => {})
  const samples = sampler.stop()

  await writeRegressionArtifact('backpressure-resource-stability', { samples, consumed, totalMessages })

  // The two invariants are completion and bounded buffering while backpressured.
  strictEqual(consumed >= totalMessages, true)
  ok(maxReadableLength <= 8, `readable buffer grew to ${maxReadableLength}`)
  assertResourceStability(samples, { minSamples: 2 })
})
