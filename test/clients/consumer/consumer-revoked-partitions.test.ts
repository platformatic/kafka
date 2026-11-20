import { deepStrictEqual, strictEqual } from 'node:assert'
import { once } from 'node:events'
import { test, type TestContext } from 'node:test'
import type { CallbackWithPromise } from '../../../src/apis/callbacks.ts'
import type { FetchRequestTopic, FetchResponse } from '../../../src/apis/consumer/fetch-v17.ts'
import { kConnections, kGetApi, kGetBootstrapConnection, kGetConnection } from '../../../src/clients/base/base.ts'
import { consumerGroupHeartbeatV0, ResponseError, type Consumer, type MessagesStream } from '../../../src/index.ts'
import type { RecordsBatch } from '../../../src/protocol/records.ts'
import { createConsumer, mockConnectionPoolGet, mockMetadata, mockMethod, waitFor } from '../../helpers.ts'

// These tests drive a real Consumer (KIP-848 consumer group protocol) and a real MessagesStream.
// Only the network is mocked: the coordinator answers heartbeats with the assignment chosen by the test,
// and the partition leader serves records from a fixed log.

const topic = 'test-topic'
const topicId = 'test-topic-id'
const heartbeatIntervalMs = 20

interface FetchRequest {
  partitions: Map<number, bigint>
  respond: (records: Map<number, RecordsBatch>) => void
}

function createRecordsBatch (firstOffset: bigint, count: number): RecordsBatch {
  return {
    firstOffset,
    length: 0,
    partitionLeaderEpoch: 0,
    magic: 2,
    crc: 0,
    attributes: 0,
    lastOffsetDelta: count - 1,
    firstTimestamp: 0n,
    maxTimestamp: 0n,
    producerId: -1n,
    producerEpoch: -1,
    firstSequence: 0,
    records: Array.from({ length: count }, (_, i) => ({
      length: 0,
      attributes: 0,
      timestampDelta: 0n,
      offsetDelta: i,
      key: Buffer.from('key'),
      value: Buffer.from(`value-${firstOffset + BigInt(i)}`),
      headers: []
    }))
  }
}

function createFetchResponse (partitions: Map<number, bigint>, records: Map<number, RecordsBatch>): FetchResponse {
  return {
    throttleTimeMs: 0,
    errorCode: 0,
    sessionId: 0,
    nodeEndpoints: [],
    responses: [
      {
        topicId,
        partitions: [...partitions.keys()].map(partition => ({
          partitionIndex: partition,
          errorCode: 0,
          highWatermark: 100n,
          lastStableOffset: 100n,
          logStartOffset: 0n,
          divergingEpoch: { epoch: -1, endOffset: -1n },
          currentLeader: { leaderId: -1, leaderEpoch: -1 },
          snapshotId: { endOffset: -1n, epoch: -1 },
          abortedTransactions: [],
          preferredReadReplica: -1,
          records: records.has(partition) ? [records.get(partition)!] : []
        }))
      }
    ]
  }
}

function createGroup (t: TestContext) {
  const consumer = createConsumer(t, { groupProtocol: 'consumer', retryDelay: 0, maxWaitTime: 10 })

  // What the coordinator assigns to the member on the next heartbeat, and errors to return instead
  const group = {
    consumer,
    assignment: [0, 1] as number[] | null,
    heartbeatErrors: [] as Error[],
    fetches: [] as FetchRequest[],
    onFetch: (_request: FetchRequest): boolean => false
  }

  mockMetadata(consumer, () => true, null, null, (_original, _options, callback: CallbackWithPromise<any>) => {
    const partition = { leader: 1, leaderEpoch: 0, replicas: [1], isr: [1], offlineReplicas: [] }

    callback(null, {
      brokers: new Map([[1, { nodeId: 1, host: 'localhost', port: 9092 }]]),
      topics: new Map([
        [topic, { id: topicId, partitions: [partition, partition], partitionsCount: 2, lastUpdate: Date.now() }]
      ])
    })
    return true
  })

  mockMethod(consumer, kGetBootstrapConnection, () => true, null, null, (_original, callback) => {
    callback(null, {})
    return true
  })

  mockMethod(consumer, kGetConnection, () => true, null, null, (_original, _broker, callback) => {
    callback(null, {})
    return true
  })

  // Nothing was ever committed for the group
  mockMethod(consumer, 'listCommittedOffsets', () => true, null, null, (_original, _options, callback) => {
    callback(null, new Map([[topic, [-1n, -1n]]]))
    return true
  })

  // fallbackMode: 'earliest' resolves to offset 0
  mockMethod(consumer, 'listOffsets', () => true, null, null, (_original, _options, callback) => {
    callback(null, new Map([[topic, [0n, 0n]]]))
    return true
  })

  mockMethod(consumer, kGetApi, () => true, null, null, (_original, name: string, callback: CallbackWithPromise<any>) => {
    switch (name) {
      case 'FindCoordinator':
        callback(
          null,
          Object.assign(
            (...args: any[]) => {
              args.at(-1)(null, { coordinators: [{ key: consumer.groupId, nodeId: 1, errorCode: 0 }] })
            },
            { version: 6 }
          )
        )
        break
      case 'ConsumerGroupHeartbeat':
        callback(
          null,
          Object.assign(
            (...args: any[]) => {
              const memberEpoch = args[3]
              const heartbeatCallback = args.at(-1)

              // Leaving the group
              if (memberEpoch === -1) {
                heartbeatCallback(null, { errorCode: 0, memberId: consumer.memberId, memberEpoch: -1, assignment: null })
                return
              }

              const error = group.heartbeatErrors.shift()
              if (error) {
                heartbeatCallback(error)
                return
              }

              heartbeatCallback(null, {
                throttleTimeMs: 0,
                errorCode: 0,
                errorMessage: null,
                memberId: consumer.memberId,
                memberEpoch: 1,
                heartbeatIntervalMs,
                // A null assignment means no change
                assignment: group.assignment ? { topicPartitions: [{ topicId, partitions: [...group.assignment] }] } : null
              })
            },
            { version: 1 }
          )
        )
        break
      case 'Fetch':
        callback(
          null,
          (
            _connection: unknown,
            _maxWaitMs: number,
            _minBytes: number,
            _maxBytes: number,
            _isolationLevel: number,
            _sessionId: number,
            _sessionEpoch: number,
            topics: FetchRequestTopic[],
            _forgottenTopicsData: unknown[],
            _rackId: string,
            fetchCallback: CallbackWithPromise<FetchResponse>
          ) => {
            const partitions = new Map<number, bigint>()
            for (const requested of topics[0]?.partitions ?? []) {
              partitions.set(requested.partition, requested.fetchOffset)
            }

            const request: FetchRequest = {
              partitions,
              respond: records => fetchCallback(null, createFetchResponse(partitions, records))
            }
            group.fetches.push(request)

            // Unless the test takes over the request, behave like an idle broker waiting for maxWaitTime
            if (!group.onFetch(request)) {
              setTimeout(() => request.respond(new Map()), 10)
            }
          }
        )
        break
      default:
        callback(new Error(`Unexpected API ${name}`))
    }

    return true
  })

  return group
}

async function startConsuming (group: ReturnType<typeof createGroup>) {
  const stream = (await group.consumer.consume({
    topics: [topic],
    mode: 'committed',
    fallbackMode: 'earliest',
    autocommit: false
  })) as MessagesStream<Buffer, Buffer, Buffer, Buffer>

  mockConnectionPoolGet(stream[kConnections], () => true, null, null, (_original, _broker, callback) => {
    callback(null, { instanceId: 1, send () {} } as any)
    return true
  })

  const received: string[] = []
  stream.on('data', message => received.push(`p${message.partition}:${message.offset}`))

  return { stream, received }
}

// Consumes offsets 0-2 of p0, and keeps the following fetch of p0 (from offset 3) inflight
async function consumeFirstBatchAndHoldNextFetch (group: ReturnType<typeof createGroup>) {
  let held: FetchRequest | undefined

  group.onFetch = request => {
    const offset = request.partitions.get(0)

    if (offset === 0n) {
      request.respond(new Map([[0, createRecordsBatch(0n, 3)]]))
      return true
    }

    if (offset === 3n && !held) {
      held = request
      return true
    }

    return false
  }

  const { stream, received } = await startConsuming(group)

  await waitFor(() => {
    deepStrictEqual(received, ['p0:0', 'p0:1', 'p0:2'])
    strictEqual(held !== undefined, true)
  })

  group.onFetch = () => false
  return { stream, received, held: held! }
}

async function waitForAssignment (consumer: Consumer, partitions: number[]) {
  await waitFor(() => {
    deepStrictEqual(consumer.assignments, partitions.length ? [{ topic, partitions }] : [])
  })
}

// Waits for the stream to fetch p0 again after it was assigned back, and returns the requested offset
async function nextFetchOffsetOfPartition0 (group: ReturnType<typeof createGroup>, from: number) {
  let offset: bigint | undefined

  await waitFor(() => {
    offset = group.fetches.slice(from).find(request => request.partitions.has(0))?.partitions.get(0)
    strictEqual(offset !== undefined, true)
  })

  return offset
}

test('should fetch a partition without committed offsets from the fallback offset when it is revoked and assigned back (consumer protocol)', async t => {
  const group = createGroup(t)
  const { stream, received, held } = await consumeFirstBatchAndHoldNextFetch(group)
  t.after(() => stream.close())

  // p0 moves to another member. Only a revocation: the consumer does not refresh the stream offsets
  group.assignment = [1]
  await waitForAssignment(group.consumer, [1])

  // The fetch issued before the revocation completes afterwards: its records must be dropped
  // and it must not move the fetch position of p0
  held.respond(new Map([[0, createRecordsBatch(3n, 3)]]))

  // p0 comes back, still without committed offsets
  const fetchesBeforeReassignment = group.fetches.length
  group.assignment = [0, 1]
  await waitForAssignment(group.consumer, [0, 1])

  // Offsets 0-2 were never committed, so p0 must restart from the fallback offset
  strictEqual(await nextFetchOffsetOfPartition0(group, fetchesBeforeReassignment), 0n)
  deepStrictEqual(received.slice(0, 3), ['p0:0', 'p0:1', 'p0:2'])
  strictEqual(received.includes('p0:3'), false)
})

test('should fetch a partition without committed offsets from the fallback offset after a fenced member gets it back (consumer protocol)', async t => {
  const group = createGroup(t)
  const { stream, received, held } = await consumeFirstBatchAndHoldNextFetch(group)
  t.after(() => stream.close())

  // The member is fenced: it loses all its partitions and joins again with epoch 0.
  // Hold the new assignment until the test is done with the fenced member.
  group.assignment = null
  const fenced = once(group.consumer, 'consumer:heartbeat:error')
  group.heartbeatErrors.push(
    new ResponseError(
      consumerGroupHeartbeatV0.api.key,
      0,
      { '/': [110, null] },
      {
        throttleTimeMs: 0,
        errorCode: 110,
        errorMessage: 'fenced',
        memberId: '',
        memberEpoch: 0,
        heartbeatIntervalMs,
        assignment: null
      }
    )
  )
  await fenced
  deepStrictEqual(group.consumer.assignments, [])

  held.respond(new Map([[0, createRecordsBatch(3n, 3)]]))

  // The next heartbeat assigns the same partitions back
  const fetchesBeforeReassignment = group.fetches.length
  group.assignment = [0, 1]
  await waitForAssignment(group.consumer, [0, 1])

  strictEqual(await nextFetchOffsetOfPartition0(group, fetchesBeforeReassignment), 0n)
  strictEqual(received.includes('p0:3'), false)
})

test('should not skip records of a partition which stays assigned when another partition is revoked (consumer protocol)', async t => {
  const group = createGroup(t)
  const { stream, received, held } = await consumeFirstBatchAndHoldNextFetch(group)
  t.after(() => stream.close())

  // p1 moves away while a fetch of p0 is inflight, p0 stays assigned
  group.assignment = [0]
  await waitForAssignment(group.consumer, [0])

  const fetchesAfterRevocation = group.fetches.length
  held.respond(new Map([[0, createRecordsBatch(3n, 3)]]))

  // Whatever happens to the inflight fetch, p0 continues right after the last record it delivered
  const offset = await nextFetchOffsetOfPartition0(group, fetchesAfterRevocation)
  deepStrictEqual(
    received,
    Array.from({ length: Number(offset) }, (_, i) => `p0:${i}`)
  )
})
