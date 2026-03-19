import { ok, strictEqual } from 'node:assert'
import { pipeline } from 'node:stream/promises'
import { test } from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import { type DeserializedMessage, MessageBatchStream, setupBackpressureTest } from '../../helpers/backpressure.ts'

/**
 * Backpressure stall reproduction test.
 *
 * Reproduces the fetch loop stall that occurs when MessagesStream is piped
 * through a batch-accumulating Duplex (like MQT's KafkaMessageBatchStream)
 * with async handler processing.
 */

test('should not stall under backpressure with pipeline and batch stream', { timeout: 120_000 }, async t => {
  const batchSize = 500
  const handlerDelayMs = 5

  const { totalMessages, consumerStream } = await setupBackpressureTest(t, {
    topicCount: 15,
    messagesPerTopic: 1000,
    consumerHighWaterMark: 1024
  })

  // Batch stream config matching typical production setup:
  // batchSize=500, timeoutMilliseconds=2000, readableHighWaterMark=32
  const batchStream = new MessageBatchStream<DeserializedMessage>({
    batchSize,
    timeoutMilliseconds: 2000,
    readableHighWaterMark: 32
  })

  // pipeline(consumerStream, batchStream) — same as MQT's AbstractKafkaConsumer.start()
  const pipelinePromise = pipeline(consumerStream, batchStream).catch(error => {
    console.error('Pipeline error:', error)
  })

  // Consume with async handler — same as MQT's handleSyncStreamBatch
  let consumed = 0
  let batchesProcessed = 0
  const consumePromise = (async () => {
    for await (const messageBatch of batchStream) {
      const batch = messageBatch as DeserializedMessage[]
      batchesProcessed++

      // Matches real handler: iterate messages synchronously
      for (const message of batch) {
        consumed++
        // Sync work: schema validation (simulated by object access)
        // eslint-disable-next-line no-void
        void message.value
      }

      // Async flush: 3 parallel Redis SADD calls (simulated with sleep)
      await sleep(handlerDelayMs)

      // Commit last message in batch
      const lastMessage = batch[batch.length - 1]!
      await lastMessage.commit()
    }
  })()

  // Monitor progress with stall detection
  const consumeStart = performance.now()
  const timeoutMs = 60_000
  let lastConsumed = 0
  let stallTicks = 0

  const result = await new Promise<'ok' | 'stall' | 'timeout'>(resolve => {
    const check = setInterval(() => {
      const elapsed = performance.now() - consumeStart

      if (consumed >= totalMessages) {
        clearInterval(check)
        resolve('ok')
        return
      }

      if (elapsed > timeoutMs) {
        clearInterval(check)
        resolve('timeout')
        return
      }

      // Stall detection: no progress for 5 seconds
      if (consumed === lastConsumed && consumed > 0) {
        stallTicks++
        if (stallTicks >= 10) {
          clearInterval(check)
          resolve('stall')
          return
        }
      } else {
        stallTicks = 0
      }
      lastConsumed = consumed
    }, 500)
  })

  // Cleanup
  await consumerStream.close()
  await Promise.all([pipelinePromise, consumePromise]).catch(() => {})

  // The bug manifests as a stall or timeout — the fetch loop dies and messages
  // remain unconsumed in Kafka. With the fix applied, all messages are consumed.
  strictEqual(
    result,
    'ok',
    `Expected all messages consumed but got ${result}: ${consumed}/${totalMessages} (${batchesProcessed} batches)`
  )
  ok(consumed >= totalMessages, `Expected at least ${totalMessages} messages but got ${consumed}`)
})
