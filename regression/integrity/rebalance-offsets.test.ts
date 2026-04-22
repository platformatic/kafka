import { strictEqual } from 'node:assert'
import { EventEmitter } from 'node:events'
import { test } from 'node:test'
import { setImmediate as tick } from 'node:timers/promises'
import type { CallbackWithPromise } from '../../src/apis/callbacks.ts'
import { kCreateConnectionPool, kPrometheus } from '../../src/clients/base/base.ts'
import { MessagesStream, MessagesStreamFallbackModes, MessagesStreamModes } from '../../src/index.ts'

test('regression #223: rebalance does not fetch while committed offsets are refreshing', async () => {
  // This mock isolates MessagesStream scheduling from Kafka. The bug was a race
  // where a group join could start fetching before refreshed offsets were loaded.
  const consumer = new EventEmitter() as any

  consumer.assignments = [{ topic: 'regression-topic', partitions: [0] }]
  consumer.topics = { current: ['regression-topic'] }
  consumer.groupId = 'regression-group'
  consumer.memberId = 'member-1'
  consumer.generationId = 1
  consumer.coordinatorId = 1
  consumer.fetchCalls = 0
  consumer.delayNextCommittedOffsets = false
  consumer.onDelayedCommittedOffsets = null
  consumer.onFetch = null
  consumer[kPrometheus] = undefined
  consumer[kCreateConnectionPool] = () => ({ close: (callback: CallbackWithPromise<void>) => callback(null) })

  consumer.metadata = (_: object, callback: CallbackWithPromise<any>) => {
    callback(null, {
      topics: new Map([['regression-topic', { id: 'regression-topic-id', partitions: [{ leader: 1, leaderEpoch: 0 }] }]])
    })
  }
  consumer.listOffsets = (_: object, callback: CallbackWithPromise<any>) => callback(null, new Map([['regression-topic', [0n]]]))
  let delayedCommittedOffsetsCallback: CallbackWithPromise<any> | null = null
  consumer.listCommittedOffsets = (_: object, callback: CallbackWithPromise<any>) => {
    if (!consumer.delayNextCommittedOffsets) {
      callback(null, new Map([['regression-topic', [10n]]]))
      return
    }

    consumer.delayNextCommittedOffsets = false
    consumer.onDelayedCommittedOffsets?.()
    delayedCommittedOffsetsCallback = callback
  }
  consumer.fetch = (_: object, _callback: CallbackWithPromise<any>) => {
    consumer.fetchCalls++
    consumer.onFetch?.()
  }

  const stream = new MessagesStream(consumer, {
    topics: ['regression-topic'],
    mode: MessagesStreamModes.COMMITTED,
    fallbackMode: MessagesStreamFallbackModes.EARLIEST,
    maxWaitTime: 1000,
    maxBytes: 1024,
    autocommit: false
  })

  const baselineFetchCalls = consumer.fetchCalls
  const refreshStarted = new Promise<void>(resolve => {
    consumer.onDelayedCommittedOffsets = resolve
  })

  consumer.delayNextCommittedOffsets = true
  consumer.emit('consumer:group:join')
  await refreshStarted

  stream.resume()
  await tick()
  // No new fetch means the stream did not fetch while offset refresh was inflight.
  strictEqual(consumer.fetchCalls, baselineFetchCalls)

  const fetchAfterRefresh = new Promise<void>(resolve => {
    consumer.onFetch = resolve
  })
  delayedCommittedOffsetsCallback!(null, new Map([['regression-topic', [10n]]]))
  await fetchAfterRefresh
  // Once offset refresh completes, fetch scheduling should resume normally.
  strictEqual(consumer.fetchCalls, baselineFetchCalls + 1)

  stream.destroy()
})
