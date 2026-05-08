import { strictEqual } from 'node:assert'
import { EventEmitter } from 'node:events'
import { test, type TestContext } from 'node:test'
import type { CallbackWithPromise } from '../../../src/apis/callbacks.ts'
import type { FetchResponse } from '../../../src/apis/consumer/fetch-v17.ts'
import { kCreateConnectionPool, kOptions, kPrometheus } from '../../../src/clients/base/base.ts'
import { type Consumer, MessagesStream, MessagesStreamFallbackModes, MessagesStreamModes } from '../../../src/index.ts'
import { kClearPreferredReadReplicas, kGetFetchNode, kUpdatePreferredReadReplicas } from '../../../src/symbols.ts'
import { createConsumer } from '../../helpers.ts'

const topic = 'test-topic'
const topicId = 'test-topic-id'

function createMetadata (replicas = [1, 2]) {
  return {
    brokers: new Map([
      [1, { nodeId: 1, host: 'broker-1', port: 9092, rack: 'rack-a' }],
      [2, { nodeId: 2, host: 'broker-2', port: 9092, rack: 'rack-b' }]
    ]),
    topics: new Map([
      [
        topic,
        {
          id: topicId,
          partitions: [
            {
              leader: 1,
              leaderEpoch: 0,
              replicas,
              isr: replicas,
              offlineReplicas: []
            }
          ],
          partitionsCount: 1,
          lastUpdate: Date.now()
        }
      ]
    ])
  }
}

function createFetchResponse (preferredReadReplica: number): FetchResponse {
  return {
    throttleTimeMs: 0,
    errorCode: 0,
    sessionId: 0,
    responses: [
      {
        topicId,
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 0,
            highWatermark: 0n,
            lastStableOffset: 0n,
            logStartOffset: 0n,
            abortedTransactions: [],
            preferredReadReplica,
            records: []
          }
        ]
      }
    ]
  }
}

// Wraps a real `Consumer` so we can drive `MessagesStream` against the real
// rack-aware routing methods (`kGetFetchNode`, `kUpdatePreferredReadReplicas`,
// `kClearPreferredReadReplicas`) without a live cluster. Only the network-facing
// methods (`metadata`, `listOffsets`, `fetch`) are stubbed; the preferred-replica
// algorithm is the same one used in production.
function createConsumerMock (
  t: TestContext,
  fetch: (options: any, callback: CallbackWithPromise<FetchResponse>) => void,
  initialMetadata: ReturnType<typeof createMetadata> = createMetadata()
): Consumer {
  const consumer = createConsumer(t)

  consumer.assignments = [{ topic, partitions: [0] }]
  consumer.topics.track(topic)
  consumer.memberId = 'test-member'
  consumer.generationId = 1

  let testMetadata = initialMetadata
  Object.defineProperty(consumer, 'testMetadata', {
    configurable: true,
    get: () => testMetadata,
    set: value => {
      testMetadata = value
    }
  })

  ;(consumer as any).metadata = (_: object, callback: CallbackWithPromise<any>) => {
    callback(null, testMetadata)
  }

  ;(consumer as any).listOffsets = (_: object, callback: CallbackWithPromise<any>) => {
    callback(null, new Map([[topic, [0n]]]))
  }

  ;(consumer as any).fetch = (options: any, callback: CallbackWithPromise<FetchResponse>) => {
    const topicIds = new Map([[topicId, topic]])
    const partition = options.topics[0].partitions[0].partition
    const node = consumer[kGetFetchNode](testMetadata as any, topic, partition, Date.now())

    fetch({ ...options, node }, (error, response) => {
      if (error) {
        const cleared = consumer[kClearPreferredReadReplicas](options.topics, topicIds)

        if (cleared) {
          const retryNode = consumer[kGetFetchNode](testMetadata as any, topic, partition, Date.now())
          fetch({ ...options, node: retryNode }, callback)
          return
        }
      } else {
        consumer[kUpdatePreferredReadReplicas](testMetadata as any, topicIds, response!)
      }

      callback(error, response)
    })
  }

  return consumer
}

function createStream (consumer: Consumer): MessagesStream<Buffer, Buffer, Buffer, Buffer> {
  return new MessagesStream(consumer, {
    topics: [topic],
    mode: MessagesStreamModes.EARLIEST,
    fallbackMode: MessagesStreamFallbackModes.EARLIEST,
    maxWaitTime: 1000,
    maxBytes: 1024,
    autocommit: false
  })
}

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
  consumer[kGetFetchNode] = (metadata: any, topic: string, partition: number) => {
    return metadata.topics.get(topic).partitions[partition].leader
  }

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
      topics: new Map([['test-topic', { id: 'test-topic-id', partitions: [{ leader: 1, leaderEpoch: 0 }] }]])
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

test('should route follow-up fetches to preferred read replicas', async t => {
  const nodes: number[] = []
  let resolveSecondFetch!: () => void
  const secondFetch = new Promise<void>(resolve => {
    resolveSecondFetch = resolve
  })

  const consumer = createConsumerMock(t, (options, callback) => {
    nodes.push(options.node)

    if (nodes.length === 1) {
      callback(null, createFetchResponse(2))
      return
    }

    resolveSecondFetch()
  })
  const stream = createStream(consumer)

  stream.resume()
  await secondFetch

  strictEqual(nodes[0], 1)
  strictEqual(nodes[1], 2)

  stream.destroy()
})

test('should keep preferred read replicas when Kafka returns no preference before the lease expires', async t => {
  const nodes: number[] = []
  let resolveThirdFetch!: () => void
  const thirdFetch = new Promise<void>(resolve => {
    resolveThirdFetch = resolve
  })

  const consumer = createConsumerMock(t, (options, callback) => {
    nodes.push(options.node)

    if (nodes.length === 1) {
      callback(null, createFetchResponse(2))
      return
    }

    if (nodes.length === 2) {
      callback(null, createFetchResponse(-1))
      return
    }

    resolveThirdFetch()
  })
  const stream = createStream(consumer)

  stream.resume()
  await thirdFetch

  strictEqual(nodes[0], 1)
  strictEqual(nodes[1], 2)
  strictEqual(nodes[2], 2)

  stream.destroy()
})

test('should fall back to leader when the preferred read replica lease expires', async t => {
  let now = 1000
  t.mock.method(Date, 'now', () => now)

  const nodes: number[] = []
  let resolveSecondFetch!: () => void
  const secondFetch = new Promise<void>(resolve => {
    resolveSecondFetch = resolve
  })

  const consumer = createConsumerMock(t, (options, callback) => {
    nodes.push(options.node)

    if (nodes.length === 1) {
      callback(null, createFetchResponse(2))
      now += consumer[kOptions].metadataMaxAge! + 1
      return
    }

    resolveSecondFetch()
  })
  const stream = createStream(consumer)

  stream.resume()
  await secondFetch

  strictEqual(nodes[0], 1)
  strictEqual(nodes[1], 1)

  stream.destroy()
})

test('should ignore unknown preferred read replicas', async t => {
  const nodes: number[] = []
  let resolveSecondFetch!: () => void
  const secondFetch = new Promise<void>(resolve => {
    resolveSecondFetch = resolve
  })

  const consumer = createConsumerMock(t, (options, callback) => {
    nodes.push(options.node)

    if (nodes.length === 1) {
      callback(null, createFetchResponse(3))
      return
    }

    resolveSecondFetch()
  })
  const stream = createStream(consumer)

  stream.resume()
  await secondFetch

  strictEqual(nodes[0], 1)
  strictEqual(nodes[1], 1)

  stream.destroy()
})

test('should ignore non-replica preferred read replicas', async t => {
  const nodes: number[] = []
  let resolveSecondFetch!: () => void
  const secondFetch = new Promise<void>(resolve => {
    resolveSecondFetch = resolve
  })
  const metadata = createMetadata([1])

  const consumer = createConsumerMock(t, (options, callback) => {
    nodes.push(options.node)

    if (nodes.length === 1) {
      callback(null, createFetchResponse(2))
      return
    }

    resolveSecondFetch()
  }, metadata)
  const stream = createStream(consumer)

  stream.resume()
  await secondFetch

  strictEqual(nodes[0], 1)
  strictEqual(nodes[1], 1)

  stream.destroy()
})

test('should fall back to leader when cached preferred read replica stops matching metadata', async t => {
  const nodes: number[] = []
  const validMetadata = createMetadata([1, 2])
  const leaderOnlyMetadata = createMetadata([1])
  let resolveThirdFetch!: () => void
  const thirdFetch = new Promise<void>(resolve => {
    resolveThirdFetch = resolve
  })

  const consumer = createConsumerMock(t, (options, callback) => {
    nodes.push(options.node)

    if (nodes.length === 1) {
      callback(null, createFetchResponse(2))
      ;(consumer as any).testMetadata = leaderOnlyMetadata
      return
    }

    if (nodes.length === 2) {
      callback(null, createFetchResponse(2))
      return
    }

    resolveThirdFetch()
  }, validMetadata)

  const stream = createStream(consumer)

  stream.resume()
  await thirdFetch

  strictEqual(nodes[0], 1)
  strictEqual(nodes[1], 1)
  strictEqual(nodes[2], 1)

  stream.destroy()
})

test('should fall back to leader when preferred read replica fetch fails', async t => {
  const nodes: number[] = []
  let resolveThirdFetch!: () => void
  const thirdFetch = new Promise<void>(resolve => {
    resolveThirdFetch = resolve
  })

  const consumer = createConsumerMock(t, (options, callback) => {
    nodes.push(options.node)

    if (nodes.length === 1) {
      callback(null, createFetchResponse(2))
      return
    }

    if (nodes.length === 2) {
      callback(new Error('preferred replica unavailable'))
      return
    }

    resolveThirdFetch()
  })
  const stream = createStream(consumer)

  stream.resume()
  await thirdFetch

  strictEqual(nodes[0], 1)
  strictEqual(nodes[1], 2)
  strictEqual(nodes[2], 1)

  stream.destroy()
})
