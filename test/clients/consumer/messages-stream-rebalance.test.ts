import { strictEqual } from 'node:assert'
import { EventEmitter } from 'node:events'
import { test } from 'node:test'
import type { CallbackWithPromise } from '../../../src/apis/callbacks.ts'
import { kCreateConnectionPool, kPrometheus } from '../../../src/clients/base/base.ts'
import { MessagesStream, MessagesStreamFallbackModes, MessagesStreamModes } from '../../../src/index.ts'

test('should not fetch while offsets are refreshing after a group rejoin', async () => {
  // Minimal in-memory consumer mock used to isolate MessagesStream scheduling logic
  // from real network/Kafka interactions.
  const consumer = new EventEmitter() as any

  // Runtime state expected by MessagesStream internals.
  consumer.assignments = [{ topic: 'test-topic', partitions: [0] }]
  consumer.topics = { current: ['test-topic'] }
  consumer.groupId = 'test-group'
  consumer.memberId = 'test-member'
  consumer.generationId = 1
  consumer.coordinatorId = 1

  // Test-only counters/flags to observe sequencing.
  consumer.metadataCalls = 0
  consumer.delayNextCommittedOffsets = false
  consumer.onDelayedCommittedOffsets = null

  // Metrics are not relevant for this unit test.
  consumer[kPrometheus] = undefined

  // MessagesStream creates a dedicated pool per stream and closes it on destroy.
  // We provide the smallest possible compatible implementation.
  consumer[kCreateConnectionPool] = () => {
    return {
      close (callback: CallbackWithPromise<void>) {
        callback(null)
      }
    }
  }

  // metadata() is called by #fetch before building fetch requests.
  // We count invocations to verify that fetch is blocked while offsets refresh is inflight.
  consumer.metadata = (_: object, callback: CallbackWithPromise<any>) => {
    consumer.metadataCalls++

    callback(null, {
      topics: new Map([
        ['test-topic', { id: 'test-topic-id', partitions: [{ leader: 1, leaderEpoch: 0 }] }]
      ])
    })
  }

  // Earliest/latest offsets are not the focus here; any valid map is enough.
  consumer.listOffsets = (_: object, callback: CallbackWithPromise<any>) => {
    callback(null, new Map([['test-topic', [0n]]]))
  }

  // This method is the important one for the race.
  // We deliberately delay one call to keep #refreshOffsets inflight and then
  // check that #fetch does not proceed during that window.
  consumer.listCommittedOffsets = (_: object, callback: CallbackWithPromise<any>) => {
    if (consumer.delayNextCommittedOffsets) {
      consumer.delayNextCommittedOffsets = false

      // Signal that the delayed refresh has started and we are in the critical window.
      consumer.onDelayedCommittedOffsets?.()

      // Resolve later so the test can attempt a read while refresh is still running.
      setTimeout(() => callback(null, new Map([['test-topic', [10n]]])), 50)
      return
    }

    callback(null, new Map([['test-topic', [10n]]]))
  }

  // Keep fetch pending forever to avoid follow-up fetch loops that would add noise
  // to metadataCalls and make the assertion timing less deterministic.
  consumer.fetch = (_: object, _callback: CallbackWithPromise<any>) => {
    // Intentionally left pending to avoid scheduling follow-up fetches
  }

  const stream = new MessagesStream(consumer, {
    topics: ['test-topic'],
    mode: MessagesStreamModes.COMMITTED,
    fallbackMode: MessagesStreamFallbackModes.EARLIEST,
    maxWaitTime: 1000,
    maxBytes: 1024,
    autocommit: false
  })

  // Kick off one normal read cycle so we can capture a baseline metadata count.
  stream.resume()
  await new Promise(resolve => setTimeout(resolve, 10))
  stream.pause()

  const baselineMetadataCalls = consumer.metadataCalls

  // Promise that resolves exactly when the delayed committed-offset refresh begins.
  const refreshStarted = new Promise<void>(resolve => {
    consumer.onDelayedCommittedOffsets = resolve
  })

  // Simulate rebalance/join event. This should trigger #refreshOffsets first.
  consumer.delayNextCommittedOffsets = true
  consumer.emit('consumer:group:join')

  // Wait until refresh is definitely inflight.
  await refreshStarted

  // Try to read while refresh is still running.
  // The regression would call metadata() here (fetch not gated by refresh).
  stream.resume()
  await new Promise(resolve => setTimeout(resolve, 10))

  // No new metadata call means fetch remained blocked while refresh was inflight.
  strictEqual(consumer.metadataCalls, baselineMetadataCalls)

  // After delayed refresh finishes, fetch should resume and metadata should increase.
  await new Promise(resolve => setTimeout(resolve, 70))

  strictEqual(consumer.metadataCalls > baselineMetadataCalls, true)

  stream.destroy()
})
