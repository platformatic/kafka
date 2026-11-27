import { deepStrictEqual, ok, rejects, strictEqual } from 'node:assert'
import { randomUUID } from 'node:crypto'
import { once } from 'node:events'
import { test, type TestContext } from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import { type ShareAcknowledgeResponse } from '../../../src/apis/consumer/share-acknowledge-v2.ts'
import {
  type ShareFetchRequestForgottenTopic,
  type ShareFetchRequestTopic,
  type ShareFetchResponse,
  type ShareFetchResponsePartition
} from '../../../src/apis/consumer/share-fetch-v2.ts'
import { type ShareGroupHeartbeatResponse } from '../../../src/apis/consumer/share-group-heartbeat-v1.ts'
import { type Callback } from '../../../src/apis/definitions.ts'
import {
  kConnections,
  kCreateConnectionPool,
  kGetApi,
  kGetBootstrapConnection,
  kGetConnection,
  kMetadata
} from '../../../src/clients/base/base.ts'
import { type ClusterMetadata, type MetadataOptions } from '../../../src/clients/base/types.ts'
import {
  type Broker,
  ConfigResourceTypes,
  type Connection,
  type ConnectionPool,
  IncrementalAlterConfigOperationTypes,
  NetworkError,
  ProtocolError,
  protocolErrors,
  ResponseError,
  ShareAcknowledgeTypes,
  ShareConsumer,
  type ShareConsumerAcknowledgementErrorPayload,
  type ShareConsumerOptions,
  type ShareMessage,
  type ShareMessagesStream,
  stringDeserializers,
  stringSerializers,
  UnsupportedApiError,
  UserError
} from '../../../src/index.ts'
import { IS_CONTROL, type RecordsBatch } from '../../../src/protocol/records.ts'
import { isUnsupportedVersion, usableVersions } from '../../helpers/api-versions.ts'
import { createAdmin, createProducer, createTopic, kafkaSingleBootstrapServers, waitFor } from '../../helpers.ts'

const { ACCEPT, GAP, RELEASE } = ShareAcknowledgeTypes

interface FetchRequest {
  broker: Broker
  groupId: string
  memberId: string
  epoch: number
  maxWaitMs: number
  minBytes: number
  maxBytes: number
  maxRecords: number
  batchSize: number
  extra: unknown[]
  topics: ShareFetchRequestTopic[]
  forgotten: ShareFetchRequestForgottenTopic[]
}

interface AcknowledgeRequest {
  broker: Broker
  groupId: string
  memberId: string
  epoch: number
  extra: unknown[]
  topics: ShareFetchRequestTopic[]
}

type FetchReply = ShareFetchResponse | Error | null
type FetchHandler = (request: FetchRequest, callback: Callback<ShareFetchResponse>) => void
type AcknowledgeHandler = (request: AcknowledgeRequest, callback: Callback<ShareAcknowledgeResponse>) => void

interface TopicLayout {
  id: string
  leaders: number[]
}

interface NetworkOptions {
  topics?: Record<string, TopicLayout>
  fetch?: FetchHandler
  acknowledge?: AcknowledgeHandler
  fetchVersion?: number
  acknowledgeVersion?: number
  metadata?: (options: MetadataOptions) => Error | undefined
  poolGet?: (broker: Broker) => Error | undefined
  heartbeatDelay?: number
}

interface MockedNetwork {
  fetches: FetchRequest[]
  acknowledgements: AcknowledgeRequest[]
  metadataRequests: MetadataOptions[]
  assignment: Array<{ topicId: string; partitions: number[] }> | null
  memberEpoch: number
  pools: number
}

function createShareConsumer (
  t: TestContext,
  overrideOptions: Partial<ShareConsumerOptions<string, string, string, string>> = {}
): ShareConsumer<string, string, string, string> {
  const consumer = new ShareConsumer<string, string, string, string>({
    clientId: `test-share-consumer-${randomUUID()}`,
    bootstrapBrokers: ['localhost:9092'],
    groupId: `test-share-group-${randomUUID()}`,
    retryDelay: 10,
    deserializers: stringDeserializers,
    maxWaitTime: 10,
    ...overrideOptions
  })

  t.after(() => consumer.close())
  return consumer
}

function createMetadata (topics: Record<string, TopicLayout>): ClusterMetadata {
  return {
    id: 'test-cluster',
    brokers: new Map([
      [0, { host: 'broker-0', port: 9092, rack: null }],
      [1, { host: 'broker-1', port: 9092, rack: null }]
    ]),
    controllerId: 0,
    topics: new Map(
      Object.entries(topics).map(([topic, { id, leaders }]) => [
        topic,
        {
          id,
          partitions: leaders.map(leader => ({
            leader,
            leaderEpoch: 0,
            replicas: [leader],
            isr: [leader],
            offlineReplicas: []
          })),
          partitionsCount: leaders.length,
          lastUpdate: Date.now()
        }
      ])
    ),
    lastUpdate: Date.now()
  }
}

function heartbeatResponse (network: MockedNetwork): ShareGroupHeartbeatResponse {
  return {
    throttleTimeMs: 0,
    errorCode: 0,
    errorMessage: null,
    memberId: null,
    memberEpoch: network.memberEpoch,
    heartbeatIntervalMs: 20,
    assignment: network.assignment ? { topicPartitions: network.assignment } : null
  }
}

function emptyFetch (): ShareFetchResponse {
  return {
    throttleTimeMs: 0,
    errorCode: 0,
    errorMessage: null,
    acquisitionLockTimeoutMs: 30000,
    responses: [],
    nodeEndpoints: []
  }
}

function fetchResponse (topicId: string, partitions: ShareFetchResponsePartition[]): ShareFetchResponse {
  return { ...emptyFetch(), responses: [{ topicId, partitions }] }
}

function partitionResponse (
  partitionIndex: number,
  records: RecordsBatch[],
  acquired: Array<[bigint, bigint, number]>,
  overrides: Partial<ShareFetchResponsePartition> = {}
): ShareFetchResponsePartition {
  return {
    partitionIndex,
    errorCode: 0,
    errorMessage: null,
    acknowledgeErrorCode: 0,
    acknowledgeErrorMessage: null,
    currentLeader: { leaderId: 0, leaderEpoch: 0 },
    records,
    acquiredRecords: acquired.map(([firstOffset, lastOffset, deliveryCount]) => ({
      firstOffset,
      lastOffset,
      deliveryCount
    })),
    ...overrides
  }
}

function recordsBatch (
  firstOffset: bigint,
  values: string[],
  options: { control?: boolean; headers?: [Buffer, Buffer][] } = {}
): RecordsBatch {
  return {
    firstOffset,
    length: 0,
    partitionLeaderEpoch: 0,
    magic: 2,
    crc: 0,
    attributes: options.control ? IS_CONTROL : 0,
    lastOffsetDelta: values.length - 1,
    firstTimestamp: 1000n,
    maxTimestamp: 1000n,
    producerId: -1n,
    producerEpoch: -1,
    firstSequence: -1,
    records: values.map((value, i) => ({
      length: 0,
      attributes: 0,
      timestampDelta: BigInt(i),
      offsetDelta: i,
      key: Buffer.from(`key-${value}`),
      value: Buffer.from(value),
      headers: options.headers ?? []
    }))
  }
}

// Answers the requests with the given replies in order, then with empty responses
function scriptedFetch (...replies: Array<FetchReply | ((request: FetchRequest) => FetchReply)>): FetchHandler {
  return (request, callback) => {
    const next = replies.shift()
    const reply = typeof next === 'function' ? next(request) : next

    // A null reply is never answered, like a fetch waiting on the broker
    if (reply === null) {
      return
    }

    setTimeout(() => {
      if (reply instanceof Error) {
        callback(reply)
        return
      }

      callback(null, reply ?? emptyFetch())
    }, 2)
  }
}

function acknowledgeResponse (): ShareAcknowledgeResponse {
  return {
    throttleTimeMs: 0,
    errorCode: 0,
    errorMessage: null,
    acquisitionLockTimeoutMs: 30000,
    responses: [],
    nodeEndpoints: []
  }
}

function mockShareNetwork (consumer: ShareConsumer<any, any, any, any>, options: NetworkOptions = {}): MockedNetwork {
  const topics = options.topics ?? { topic: { id: 'topic-id', leaders: [0] } }
  const fetchHandler = options.fetch ?? scriptedFetch()
  const acknowledgeHandler: AcknowledgeHandler =
    options.acknowledge ?? ((_, callback) => callback(null, acknowledgeResponse()))
  const coordinator = { isConnected: () => true } as unknown as Connection

  const network: MockedNetwork = {
    fetches: [],
    acknowledgements: [],
    metadataRequests: [],
    assignment: Object.values(topics).map(({ id, leaders }) => ({ topicId: id, partitions: leaders.map((_, i) => i) })),
    memberEpoch: 1,
    pools: 0
  }

  consumer[kMetadata] = (metadataOptions: MetadataOptions, callback: Callback<ClusterMetadata>) => {
    network.metadataRequests.push(metadataOptions)

    const error = options.metadata?.(metadataOptions)
    if (error) {
      callback(error)
      return
    }

    const unknown = metadataOptions.topics?.find(topic => !(topic in topics))
    if (unknown) {
      callback(new UserError(`Unknown topic ${unknown}.`))
      return
    }

    callback(null, createMetadata(topics))
  }

  consumer[kGetConnection] = (_broker: Broker, callback: Callback<Connection>) => {
    callback(null, coordinator)
  }

  consumer[kGetBootstrapConnection] = (callback: Callback<Connection>) => {
    callback(null, coordinator)
  }

  consumer[kCreateConnectionPool] = () => {
    network.pools++

    return {
      get (broker: Broker, callback: Callback<Connection>) {
        const error = options.poolGet?.(broker)
        if (error) {
          callback(error)
          return
        }

        callback(null, { broker } as unknown as Connection)
      },
      close (callback: Callback<void>) {
        callback(null)
      }
    } as unknown as ConnectionPool
  }

  ;(consumer as unknown as Record<symbol, unknown>)[kGetApi] = (name: string, callback: Callback<unknown>) => {
    if (name === 'FindCoordinator') {
      const api = (_connection: Connection, _keyType: number, keys: string[], apiCallback: Callback<unknown>) => {
        apiCallback(null, {
          throttleTimeMs: 0,
          coordinators: [{ key: keys[0], nodeId: 0, host: 'broker-0', port: 9092, errorCode: 0, errorMessage: null }]
        })
      }

      callback(null, Object.assign(api, { version: 6 }))
      return
    }

    if (name === 'ShareGroupHeartbeat') {
      const api = (_connection: Connection, ...args: unknown[]) => {
        const apiCallback = args.at(-1) as Callback<ShareGroupHeartbeatResponse>

        if (options.heartbeatDelay) {
          setTimeout(() => apiCallback(null, heartbeatResponse(network)), options.heartbeatDelay)
          return
        }

        apiCallback(null, heartbeatResponse(network))
      }

      callback(null, Object.assign(api, { version: 1 }))
      return
    }

    if (name === 'ShareFetch') {
      const version = options.fetchVersion ?? 2
      const api = (connection: Connection, ...args: unknown[]) => {
        const apiCallback = args.pop() as Callback<ShareFetchResponse>
        const forgotten = args.pop() as ShareFetchRequestForgottenTopic[]
        const requestTopics = args.pop() as ShareFetchRequestTopic[]
        const [groupId, memberId, epoch, maxWaitMs, minBytes, maxBytes, maxRecords, batchSize, ...extra] = args
        const request = {
          broker: (connection as unknown as { broker: Broker }).broker,
          groupId,
          memberId,
          epoch,
          maxWaitMs,
          minBytes,
          maxBytes,
          maxRecords,
          batchSize,
          extra,
          topics: requestTopics,
          forgotten
        } as FetchRequest

        network.fetches.push(request)
        fetchHandler(request, apiCallback)
      }

      callback(null, Object.assign(api, { version }))
      return
    }

    if (name === 'ShareAcknowledge') {
      const version = options.acknowledgeVersion ?? 2
      const api = (connection: Connection, ...args: unknown[]) => {
        const apiCallback = args.pop() as Callback<ShareAcknowledgeResponse>
        const requestTopics = args.pop() as ShareFetchRequestTopic[]
        const [groupId, memberId, epoch, ...extra] = args
        const request = {
          broker: (connection as unknown as { broker: Broker }).broker,
          groupId,
          memberId,
          epoch,
          extra,
          topics: requestTopics
        } as AcknowledgeRequest

        network.acknowledgements.push(request)
        acknowledgeHandler(request, apiCallback)
      }

      callback(null, Object.assign(api, { version }))
      return
    }

    callback(new UnsupportedApiError(`API ${name} is not mocked.`))
  }

  return network
}

// Reads the given number of messages, then pauses the stream
function read<Key, Value, HeaderKey, HeaderValue> (
  stream: ShareMessagesStream<Key, Value, HeaderKey, HeaderValue>,
  count: number
): Promise<ShareMessage<Key, Value, HeaderKey, HeaderValue>[]> {
  return new Promise(resolve => {
    const messages: ShareMessage<Key, Value, HeaderKey, HeaderValue>[] = []

    const onData = (message: ShareMessage<Key, Value, HeaderKey, HeaderValue>) => {
      messages.push(message)

      if (messages.length === count) {
        stream.pause()
        stream.removeListener('data', onData)
        resolve(messages)
      }
    }

    stream.on('data', onData)
  })
}

// The acknowledgements of a request, as [partition, firstOffset, lastOffset, type] tuples
function acknowledgementsOf (request: FetchRequest | AcknowledgeRequest): Array<[number, bigint, bigint, number]> {
  return request.topics.flatMap(topic =>
    topic.partitions.flatMap(partition =>
      partition.acknowledgementBatches.map((batch): [number, bigint, bigint, number] => [
        partition.partitionIndex,
        batch.firstOffset,
        batch.lastOffset,
        batch.acknowledgeTypes[0]
      ])))
}

// The acknowledgement types of each offset, over all the given requests
function acknowledgedOffsets (requests: Array<FetchRequest | AcknowledgeRequest>): Map<bigint, number[]> {
  const offsets = new Map<bigint, number[]>()

  for (const request of requests) {
    for (const [, firstOffset, lastOffset, type] of acknowledgementsOf(request)) {
      for (let offset = firstOffset; offset <= lastOffset; offset++) {
        offsets.set(offset, [...(offsets.get(offset) ?? []), type])
      }
    }
  }

  return new Map([...offsets].sort(([a], [b]) => (a < b ? -1 : 1)))
}

// Breaking out of a for await loop destroys the stream with an AbortError, which once() would reject with
function closed (stream: ShareMessagesStream<any, any, any, any>): Promise<void> {
  return new Promise(resolve => {
    if (stream.closed) {
      resolve()
      return
    }

    stream.once('close', () => resolve())
  })
}

function partitionsOf (request: FetchRequest): number[] {
  return request.topics.flatMap(topic => topic.partitions.map(partition => partition.partitionIndex))
}

test('consume should join the group and deliver the acquired records', async t => {
  const consumer = createShareConsumer(t)
  const network = mockShareNetwork(consumer, {
    fetch: scriptedFetch(
      fetchResponse('topic-id', [
        partitionResponse(
          0,
          [
            recordsBatch(0n, ['a', 'b', 'c']),
            recordsBatch(3n, ['marker'], { control: true }),
            recordsBatch(4n, ['d', 'e'])
          ],
          // Offset 0 and 5 were acquired by other members
          [[1n, 4n, 2]]
        )
      ])
    )
  })

  const stream = await consumer.consume({ topics: ['topic'] })
  strictEqual(consumer.isActive(), true)

  const messages = await read(stream, 3)
  deepStrictEqual(
    messages.map(message => [message.key, message.value, message.offset, message.deliveryCount]),
    [
      ['key-b', 'b', 1n, 2],
      ['key-c', 'c', 2n, 2],
      ['key-d', 'd', 4n, 2]
    ]
  )
  deepStrictEqual(
    messages.map(message => [message.topic, message.partition, message.timestamp]),
    [
      ['topic', 0, 1001n],
      ['topic', 0, 1002n],
      ['topic', 0, 1000n]
    ]
  )

  // The session is opened with the assigned partitions and no acknowledgements
  const [open] = network.fetches
  strictEqual(open.epoch, 0)
  strictEqual(open.groupId, consumer.groupId)
  strictEqual(open.memberId, consumer.memberId)
  deepStrictEqual(open.broker, { host: 'broker-0', port: 9092, rack: null })
  deepStrictEqual(
    [open.maxWaitMs, open.minBytes, open.maxBytes, open.maxRecords, open.batchSize, open.extra],
    [10, 1, 52428800, 500, 500, [0, false]]
  )
  deepStrictEqual(open.topics, [
    { topicId: 'topic-id', partitions: [{ partitionIndex: 0, acknowledgementBatches: [] }] }
  ])
  deepStrictEqual(open.forgotten, [])

  // The records read are accepted with the next request, and the transaction marker is a gap
  await waitFor(() => ok(network.fetches.length >= 2))
  strictEqual(network.fetches[1].epoch, 1)
  deepStrictEqual(acknowledgementsOf(network.fetches[1]), [
    [0, 1n, 2n, ACCEPT],
    [0, 3n, 3n, GAP],
    [0, 4n, 4n, ACCEPT]
  ])
  deepStrictEqual(network.fetches[1].forgotten, [])

  await waitFor(() => ok(network.fetches.length >= 3))
  strictEqual(network.fetches[2].epoch, 2)
  deepStrictEqual(network.fetches[2].topics, [])
})

test('records should only be accepted once the stream hands them to the application', async t => {
  const consumer = createShareConsumer(t)
  const network = mockShareNetwork(consumer, {
    fetch: scriptedFetch(
      fetchResponse('topic-id', [partitionResponse(0, [recordsBatch(10n, ['a', 'b', 'c', 'd'])], [[10n, 13n, 1]])])
    )
  })

  const stream = await consumer.consume({ topics: ['topic'], highWaterMark: 2 })
  const iterator = stream[Symbol.asyncIterator]()

  strictEqual((await iterator.next()).value!.value, 'a')
  strictEqual((await iterator.next()).value!.value, 'b')

  // The buffer is still at the high water mark, so nothing is fetched or acknowledged
  await sleep(50)
  strictEqual(network.fetches.length, 1)

  strictEqual((await iterator.next()).value!.value, 'c')
  await waitFor(() => ok(network.fetches.length >= 3))

  // The last record was never read: closing the stream releases it
  await iterator.return!()
  await closed(stream)

  deepStrictEqual(
    acknowledgedOffsets([...network.fetches, ...network.acknowledgements]),
    new Map([
      [10n, [ACCEPT]],
      [11n, [ACCEPT]],
      [12n, [ACCEPT]],
      [13n, [RELEASE]]
    ])
  )
  strictEqual(network.acknowledgements[0].epoch, -1)
  deepStrictEqual(network.acknowledgements[0].extra, [false])
})

test('offsets of the acquired ranges without a record should be acknowledged as gaps', async t => {
  const consumer = createShareConsumer(t)
  const network = mockShareNetwork(consumer, {
    fetch: scriptedFetch(
      fetchResponse('topic-id', [
        partitionResponse(
          0,
          // Offset 2 was compacted away, offsets 4 to 6 hold no record either
          [recordsBatch(0n, ['a', 'b']), recordsBatch(3n, ['d'])],
          [
            [3n, 6n, 2],
            [0n, 2n, 1]
          ]
        )
      ])
    )
  })

  const stream = await consumer.consume({ topics: ['topic'] })
  const messages = await read(stream, 3)
  deepStrictEqual(
    messages.map(message => [message.value, message.deliveryCount]),
    [
      ['a', 1],
      ['b', 1],
      ['d', 2]
    ]
  )

  await waitFor(() => ok(network.fetches.length >= 2))
  deepStrictEqual(acknowledgementsOf(network.fetches[1]), [
    [0, 0n, 1n, ACCEPT],
    [0, 2n, 2n, GAP],
    [0, 3n, 3n, ACCEPT],
    [0, 4n, 6n, GAP]
  ])
})

test('partitions without a leader should not be fetched until they have one', async t => {
  const consumer = createShareConsumer(t)
  const network = mockShareNetwork(consumer, { topics: { topic: { id: 'topic-id', leaders: [0, -1] } } })

  const stream = await consumer.consume({ topics: ['topic'] })
  stream.resume()

  await waitFor(() => ok(network.fetches.length >= 2))
  deepStrictEqual(partitionsOf(network.fetches[0]), [0])
})

test('the stream should fail when the broker does not support ShareFetch', async t => {
  const consumer = createShareConsumer(t)
  const network = mockShareNetwork(consumer)

  const stream = await consumer.consume({ topics: ['topic'] })
  const getApi = consumer[kGetApi].bind(consumer)
  const mocked = consumer as unknown as Record<symbol, unknown>
  mocked[kGetApi] = (name: string, callback: Callback<unknown>) => {
    if (name === 'ShareFetch' || name === 'ShareAcknowledge') {
      callback(new UnsupportedApiError(`Broker does not support API ${name}.`))
      return
    }

    getApi(name, callback as Callback<any>)
  }

  stream.resume()
  const [error] = await once(stream, 'error')
  strictEqual(error.message, 'Broker does not support API ShareFetch.')
  strictEqual(network.fetches.length, 0)
  await closed(stream)
})

test('close should end a for await loop and acknowledge what was read', async t => {
  const consumer = createShareConsumer(t)
  const network = mockShareNetwork(consumer, {
    fetch: scriptedFetch(
      fetchResponse('topic-id', [partitionResponse(0, [recordsBatch(0n, ['a', 'b', 'c', 'd'])], [[0n, 3n, 1]])]),
      null
    )
  })

  const stream = await consumer.consume({ topics: ['topic'], highWaterMark: 10 })
  const values: string[] = []

  for await (const message of stream) {
    values.push(message.value)

    if (values.length === 2) {
      // Closing does not throw in the loop, and the records still buffered never reach it
      stream.close()
    }
  }

  deepStrictEqual(values, ['a', 'b'])
  await waitFor(() => ok(stream.closed))

  // The session is closed, even if the request opening it is still inflight
  strictEqual(network.acknowledgements.length, 1)
  strictEqual(network.acknowledgements[0].epoch, -1)
  deepStrictEqual(acknowledgementsOf(network.acknowledgements[0]), [
    [0, 0n, 1n, ACCEPT],
    [0, 2n, 3n, RELEASE]
  ])

  // Closing again is a no-op
  await stream.close()
})

test('close should not hand the released records to a readable listener', async t => {
  const consumer = createShareConsumer(t)
  const network = mockShareNetwork(consumer, {
    fetch: scriptedFetch(
      fetchResponse('topic-id', [partitionResponse(0, [recordsBatch(0n, ['a', 'b', 'c'])], [[0n, 2n, 1]])]),
      null
    )
  })

  const stream = await consumer.consume({ topics: ['topic'] })
  const values: string[] = []

  // Reads a single record each time, so the others stay buffered
  stream.on('readable', () => {
    const message = stream.read()

    if (message) {
      values.push(message.value)
    }
  })

  await waitFor(() => ok(values.length === 1))
  await stream.close()

  deepStrictEqual(values, ['a'])
  deepStrictEqual(
    acknowledgedOffsets([...network.fetches, ...network.acknowledgements]),
    new Map([
      [0n, [ACCEPT]],
      [1n, [RELEASE]],
      [2n, [RELEASE]]
    ])
  )
})

test('close should wait for a stream which is already closing', async t => {
  const consumer = createShareConsumer(t)
  let release: () => void
  const network = mockShareNetwork(consumer, {
    acknowledge (_, callback) {
      release = () => callback(null, acknowledgeResponse())
    }
  })

  const stream = await consumer.consume({ topics: ['topic'] })
  stream.resume()
  await waitFor(() => ok(network.fetches.length >= 2))

  let closed = false
  const first = stream.close().then(() => {
    closed = true
  })
  const second = stream.close()

  await waitFor(() => ok(network.acknowledgements.length === 1))
  strictEqual(closed, false)
  release!()
  await Promise.all([first, second])
})

test('close should report errors from closing the connections', async t => {
  const consumer = createShareConsumer(t)
  mockShareNetwork(consumer)

  const stream = await consumer.consume({ topics: ['topic'] })
  const pool = stream[kConnections] as unknown as Record<string, unknown>
  pool.close = (callback: Callback<void>) => {
    callback(new Error('Cannot close.'))
  }

  await rejects(stream.close(), { message: 'Cannot close.' })
})

test('a session error should open a new session which acknowledges the records of the old one', async t => {
  const consumer = createShareConsumer(t)
  const sessionError = () =>
    new ResponseError(
      78,
      2,
      { '/': [protocolErrors.SHARE_SESSION_NOT_FOUND.code, null] },
      {
        ...emptyFetch(),
        errorCode: protocolErrors.SHARE_SESSION_NOT_FOUND.code
      }
    )
  const network = mockShareNetwork(consumer, {
    fetch: scriptedFetch(
      fetchResponse('topic-id', [partitionResponse(0, [recordsBatch(0n, ['a', 'b'])], [[0n, 1n, 1]])]),
      sessionError(),
      emptyFetch(),
      sessionError()
    )
  })

  const stream = await consumer.consume({ topics: ['topic'], highWaterMark: 2 })
  const iterator = stream[Symbol.asyncIterator]()
  strictEqual((await iterator.next()).value!.value, 'a')

  // The broker lost the session: a new one is opened right away, and the records stay locked for the member
  await waitFor(() => ok(network.fetches.length >= 5))
  deepStrictEqual(
    network.fetches.slice(0, 5).map(request => request.epoch),
    [0, 1, 0, 1, 0]
  )
  deepStrictEqual(acknowledgedOffsets(network.fetches.slice(2)), new Map([[0n, [ACCEPT, ACCEPT]]]))

  strictEqual((await iterator.next()).value!.value, 'b')
  await waitFor(() => ok(acknowledgedOffsets(network.fetches).has(1n)))
  await iterator.return!()
  await closed(stream)
})

test('repeated session errors should fail the stream once the retries are exhausted', async t => {
  const consumer = createShareConsumer(t, { retries: 2 })
  const sessionError = () =>
    new ResponseError(
      78,
      2,
      { '/': [protocolErrors.INVALID_SHARE_SESSION_EPOCH.code, null] },
      {
        ...emptyFetch(),
        errorCode: protocolErrors.INVALID_SHARE_SESSION_EPOCH.code
      }
    )
  const network = mockShareNetwork(consumer, {
    fetch: scriptedFetch(sessionError(), sessionError(), sessionError())
  })

  const stream = await consumer.consume({ topics: ['topic'] })
  stream.resume()

  const [error] = await once(stream, 'error')
  strictEqual(error.errors[0].apiId, 'INVALID_SHARE_SESSION_EPOCH')
  strictEqual(network.fetches.length, 3)
})

test('a stream failing after a request error should still close the session on the broker', async t => {
  const consumer = createShareConsumer(t, { retries: 0 })
  const network = mockShareNetwork(consumer, {
    fetch: scriptedFetch(
      fetchResponse('topic-id', [partitionResponse(0, [recordsBatch(0n, ['a', 'b'])], [[0n, 1n, 1]])]),
      new NetworkError('Connection lost.')
    )
  })

  const stream = await consumer.consume({ topics: ['topic'] })
  const values: string[] = []
  stream.on('data', message => values.push(message.value))

  const [error] = await once(stream, 'error')
  strictEqual(error.message, 'Connection lost.')
  await closed(stream)
  deepStrictEqual(values, ['a', 'b'])

  // The acknowledgements of the failed request are sent again when the session is closed
  deepStrictEqual(acknowledgementsOf(network.fetches[1]), [[0, 0n, 1n, ACCEPT]])
  strictEqual(network.acknowledgements.length, 1)
  strictEqual(network.acknowledgements[0].epoch, -1)
  deepStrictEqual(acknowledgementsOf(network.acknowledgements[0]), [[0, 0n, 1n, ACCEPT]])
})
test('close called while handling a record should not push the other records', async t => {
  const consumer = createShareConsumer(t)
  const network = mockShareNetwork(consumer, {
    fetch: scriptedFetch(
      fetchResponse('topic-id', [partitionResponse(0, [recordsBatch(0n, ['a', 'b', 'c'])], [[0n, 2n, 1]])]),
      null
    )
  })

  const stream = await consumer.consume({ topics: ['topic'] })
  const values: string[] = []
  const errors: Error[] = []
  stream.on('error', error => errors.push(error))

  const closing = new Promise<void>(resolve => {
    stream.on('data', message => {
      values.push(message.value)
      stream.close().then(resolve)
    })
  })

  await closing
  deepStrictEqual(values, ['a'])
  deepStrictEqual(errors, [])

  // Only the record which was handled is acknowledged, closing the session releases the others
  deepStrictEqual(acknowledgedOffsets(network.acknowledgements), new Map([[0n, [ACCEPT]]]))
})

test('partition errors which can be retried should delay the session and fail the stream once the retries are exhausted', async t => {
  const consumer = createShareConsumer(t, { retries: 2, retryDelay: 40 })
  const failing = () =>
    fetchResponse('topic-id', [
      partitionResponse(0, [], [], { errorCode: protocolErrors.UNKNOWN_TOPIC_OR_PARTITION.code })
    ])
  const network = mockShareNetwork(consumer, { fetch: scriptedFetch(failing(), failing(), failing()) })

  const stream = await consumer.consume({ topics: ['topic'] })
  const started = Date.now()
  stream.resume()

  const [error] = await once(stream, 'error')
  ok(error instanceof ProtocolError)
  strictEqual(error.apiId, 'UNKNOWN_TOPIC_OR_PARTITION')
  strictEqual(network.fetches.length, 3)
  ok(Date.now() - started >= 80)
})

test('partition errors which cannot be retried should fail the stream', async t => {
  const consumer = createShareConsumer(t)
  mockShareNetwork(consumer, {
    fetch: scriptedFetch(
      fetchResponse('topic-id', [
        partitionResponse(0, [], [], { errorCode: protocolErrors.TOPIC_AUTHORIZATION_FAILED.code })
      ])
    )
  })

  const stream = await consumer.consume({ topics: ['topic'] })
  stream.resume()

  const [error] = await once(stream, 'error')
  strictEqual(error.apiId, 'TOPIC_AUTHORIZATION_FAILED')
})

test('retriable errors should be retried after the retry delay', async t => {
  const consumer = createShareConsumer(t, { retries: 2, retryDelay: () => 30 })
  let failures = 1
  const network = mockShareNetwork(consumer, {
    poolGet () {
      return failures-- > 0 ? new NetworkError('Connection failed.') : undefined
    },
    fetch: scriptedFetch(new NetworkError('Connection lost.'), emptyFetch(), null)
  })

  const stream = await consumer.consume({ topics: ['topic'] })
  const started = Date.now()
  stream.resume()

  await waitFor(() => ok(network.fetches.length >= 3))
  ok(Date.now() - started >= 60)
  strictEqual(network.fetches[1].epoch, 0)
  strictEqual(network.fetches[2].epoch, 1)
  strictEqual(stream.destroyed, false)
})

test('the stream should fail when the retries are exhausted', async t => {
  const consumer = createShareConsumer(t, { retries: 1 })
  mockShareNetwork(consumer, {
    fetch: scriptedFetch(new NetworkError('Connection lost.'), new NetworkError('Connection lost again.'))
  })

  const stream = await consumer.consume({ topics: ['topic'] })
  stream.resume()

  const [error] = await once(stream, 'error')
  strictEqual(error.message, 'Connection lost again.')
})

test('the stream should fail on errors which cannot be retried', async t => {
  const consumer = createShareConsumer(t)
  const error = new ResponseError(
    78,
    2,
    { '/': [protocolErrors.GROUP_AUTHORIZATION_FAILED.code, null] },
    {
      ...emptyFetch(),
      errorCode: protocolErrors.GROUP_AUTHORIZATION_FAILED.code
    }
  )
  mockShareNetwork(consumer, { fetch: scriptedFetch(error) })

  const stream = await consumer.consume({ topics: ['topic'] })
  stream.resume()

  const [streamError] = await once(stream, 'error')
  strictEqual(streamError, error)
})

test('partition errors should refresh the metadata and keep the records of other partitions', async t => {
  const consumer = createShareConsumer(t)
  const response = fetchResponse('topic-id', [
    partitionResponse(0, [], [], { errorCode: protocolErrors.NOT_LEADER_OR_FOLLOWER.code }),
    partitionResponse(1, [recordsBatch(0n, ['a'])], [[0n, 0n, 1]])
  ])
  const network = mockShareNetwork(consumer, {
    topics: { topic: { id: 'topic-id', leaders: [0, 0] } },
    fetch: scriptedFetch(
      new ResponseError(
        78,
        2,
        { '/responses/0/partitions/0': [protocolErrors.NOT_LEADER_OR_FOLLOWER.code, null] },
        response
      )
    )
  })

  let cleared = 0
  const clearMetadata = consumer.clearMetadata.bind(consumer)
  consumer.clearMetadata = () => {
    cleared++
    clearMetadata()
  }

  const stream = await consumer.consume({ topics: ['topic'] })
  const [message] = await read(stream, 1)

  deepStrictEqual([message.partition, message.value], [1, 'a'])
  strictEqual(cleared, 1)
  await waitFor(() => ok(network.fetches.length >= 2))
  strictEqual(network.fetches[1].epoch, 1)
})

test('stale metadata errors of a request should refresh the metadata', async t => {
  const consumer = createShareConsumer(t)
  const error = new ResponseError(
    78,
    2,
    { '/': [protocolErrors.NOT_LEADER_OR_FOLLOWER.code, null] },
    {
      ...emptyFetch(),
      errorCode: protocolErrors.NOT_LEADER_OR_FOLLOWER.code
    }
  )
  const network = mockShareNetwork(consumer, { fetch: scriptedFetch(error, null) })

  let cleared = 0
  consumer.clearMetadata = () => {
    cleared++
  }

  const stream = await consumer.consume({ topics: ['topic'] })
  stream.resume()

  await waitFor(() => ok(network.fetches.length >= 2))
  strictEqual(cleared, 1)
  strictEqual(network.fetches[1].epoch, 0)
})

test('acknowledgement errors should be emitted', async t => {
  const consumer = createShareConsumer(t)
  const response = fetchResponse('topic-id', [
    partitionResponse(0, [], [], {
      acknowledgeErrorCode: protocolErrors.INVALID_RECORD_STATE.code,
      acknowledgeErrorMessage: 'The lock expired.'
    })
  ])
  const network = mockShareNetwork(consumer, {
    fetch: scriptedFetch(
      fetchResponse('topic-id', [partitionResponse(0, [recordsBatch(0n, ['a'])], [[0n, 0n, 1]])]),
      new ResponseError(
        78,
        2,
        { '/responses/0/partitions/0/acknowledge': [protocolErrors.INVALID_RECORD_STATE.code, 'The lock expired.'] },
        response
      ),
      null
    )
  })

  const errors: ShareConsumerAcknowledgementErrorPayload[] = []
  consumer.on('share-consumer:acknowledgement:error', payload => errors.push(payload))

  const stream = await consumer.consume({ topics: ['topic'] })
  await read(stream, 1)

  await waitFor(() => ok(errors.length === 1))
  const [{ error, ...payload }] = errors
  deepStrictEqual(payload, {
    groupId: consumer.groupId,
    memberId: consumer.memberId,
    memberEpoch: 1,
    topic: 'topic',
    partition: 0
  })
  ok(error instanceof ProtocolError)
  strictEqual(error.message, protocolErrors.INVALID_RECORD_STATE.message)
  strictEqual((error as ProtocolError).serverErrorMessage, 'The lock expired.')

  // The session goes on
  await waitFor(() => ok(network.fetches.length >= 3))
  strictEqual(network.fetches[2].epoch, 2)
})

test('the acknowledgement errors of closing a session should be emitted', async t => {
  const consumer = createShareConsumer(t)
  const response: ShareAcknowledgeResponse = {
    ...acknowledgeResponse(),
    responses: [
      {
        topicId: 'topic-id',
        partitions: [
          {
            partitionIndex: 0,
            errorCode: protocolErrors.INVALID_RECORD_STATE.code,
            errorMessage: null,
            currentLeader: { leaderId: 0, leaderEpoch: 0 }
          }
        ]
      }
    ]
  }
  mockShareNetwork(consumer, {
    fetch: scriptedFetch(
      fetchResponse('topic-id', [partitionResponse(0, [recordsBatch(0n, ['a'])], [[0n, 0n, 1]])]),
      null
    ),
    acknowledge (_, callback) {
      callback(
        new ResponseError(
          79,
          2,
          { '/responses/0/partitions/0': [protocolErrors.INVALID_RECORD_STATE.code, null] },
          response
        )
      )
    }
  })

  const errors: ShareConsumerAcknowledgementErrorPayload[] = []
  consumer.on('share-consumer:acknowledgement:error', payload => errors.push(payload))

  const stream = await consumer.consume({ topics: ['topic'] })
  await read(stream, 1)
  await stream.close()

  strictEqual(errors.length, 1)
  deepStrictEqual([errors[0].topic, errors[0].partition], ['topic', 0])
})

test('failing to close a session should not fail closing the stream', async t => {
  const consumer = createShareConsumer(t)
  let failPool = false
  const network = mockShareNetwork(consumer, {
    topics: { topic: { id: 'topic-id', leaders: [0, 1] } },
    poolGet (broker) {
      return failPool && broker.host === 'broker-0' ? new NetworkError('Connection failed.') : undefined
    },
    fetch: scriptedFetch(emptyFetch(), emptyFetch(), null, null),
    acknowledge (_, callback) {
      callback(new NetworkError('Connection lost.'))
    }
  })

  const stream = await consumer.consume({ topics: ['topic'] })
  stream.resume()
  await waitFor(() => ok(network.fetches.length === 4))

  failPool = true
  await stream.close()

  // Only the broker which could be reached got the request
  deepStrictEqual(
    network.acknowledgements.map(request => request.broker.host),
    ['broker-1']
  )
})

test('each leader should have its own session', async t => {
  const consumer = createShareConsumer(t)
  const network = mockShareNetwork(consumer, {
    topics: { topic: { id: 'topic-id', leaders: [0, 1, 0] } },
    fetch (request, callback) {
      // Only the first request to each broker is answered
      if (network.fetches.filter(fetch => fetch.broker.host === request.broker.host).length === 1) {
        setTimeout(() => callback(null, emptyFetch()), 2)
      }
    }
  })

  const stream = await consumer.consume({ topics: ['topic'] })
  stream.resume()
  await waitFor(() => ok(network.fetches.length === 4))

  const byBroker = (host: string) => network.fetches.filter(request => request.broker.host === host)
  deepStrictEqual(
    byBroker('broker-0').map(request => [request.epoch, partitionsOf(request)]),
    [
      [0, [0, 2]],
      [1, []]
    ]
  )
  deepStrictEqual(
    byBroker('broker-1').map(request => [request.epoch, partitionsOf(request)]),
    [
      [0, [1]],
      [1, []]
    ]
  )

  await stream.close()
  deepStrictEqual(network.acknowledgements.map(request => [request.broker.host, request.epoch]).sort(), [
    ['broker-0', -1],
    ['broker-1', -1]
  ])
})

test('partitions should be added to and removed from the sessions when the assignment changes', async t => {
  const consumer = createShareConsumer(t)
  const network = mockShareNetwork(consumer, {
    topics: { topic: { id: 'topic-id', leaders: [0, 0, 1] } },
    fetch: scriptedFetch(
      fetchResponse('topic-id', [partitionResponse(0, [recordsBatch(0n, ['a'])], [[0n, 0n, 1]])]),
      emptyFetch()
    )
  })
  network.assignment = [{ topicId: 'topic-id', partitions: [0] }]

  const stream = await consumer.consume({ topics: ['topic'] })
  const [message] = await read(stream, 1)
  strictEqual(message.value, 'a')
  stream.resume()

  await waitFor(() => ok(network.fetches.length >= 3))
  deepStrictEqual(partitionsOf(network.fetches[0]), [0])

  // Partition 0 is revoked, partition 1 is assigned on the same broker and partition 2 on another one
  network.assignment = [{ topicId: 'topic-id', partitions: [1, 2] }]

  await waitFor(() => {
    const request = network.fetches.find(request => partitionsOf(request).includes(1))
    ok(request)
    deepStrictEqual(request.forgotten, [{ topicId: 'topic-id', partitions: [0] }])
  })
  await waitFor(() => ok(network.fetches.some(request => request.broker.host === 'broker-1' && request.epoch === 0)))

  // Revoking everything only removes the partitions, without waiting on the broker
  network.assignment = []

  await waitFor(() => {
    const request = network.fetches.at(-1)!
    deepStrictEqual(request.forgotten, [
      { topicId: 'topic-id', partitions: [request.broker.host === 'broker-0' ? 1 : 2] }
    ])
    strictEqual(request.maxWaitMs, 0)
  })
})

test('a revoked partition should be acknowledged and removed from the session with the same request', async t => {
  const consumer = createShareConsumer(t)
  const network = mockShareNetwork(consumer, {
    topics: { topic: { id: 'topic-id', leaders: [0, 0] } },
    fetch: scriptedFetch(fetchResponse('topic-id', [partitionResponse(0, [recordsBatch(0n, ['a'])], [[0n, 0n, 1]])]))
  })

  const stream = await consumer.consume({ topics: ['topic'], highWaterMark: 1 })
  stream.pause()
  await waitFor(() => strictEqual(stream.readableLength, 1))

  // Partition 0 is revoked while its record is in the buffer
  network.assignment = [{ topicId: 'topic-id', partitions: [1] }]
  await waitFor(() => deepStrictEqual(consumer.assignments, [{ topic: 'topic', partitions: [1] }]))
  strictEqual(network.fetches.length, 1)
  stream.resume()

  await waitFor(() => {
    const request = network.fetches.find(request => acknowledgementsOf(request).length > 0)
    ok(request)
    deepStrictEqual(acknowledgementsOf(request), [[0, 0n, 0n, ACCEPT]])
    deepStrictEqual(request.forgotten, [{ topicId: 'topic-id', partitions: [0] }])
  })

  // Afterwards the partition is not fetched anymore
  const sent = network.fetches.length
  await waitFor(() => ok(network.fetches.length >= sent + 2))
  for (const request of network.fetches.slice(sent)) {
    deepStrictEqual([request.topics, request.forgotten], [[], []])
  }
})

test('the stream should not fetch while nobody is reading or the buffer is full', async t => {
  const consumer = createShareConsumer(t)
  const network = mockShareNetwork(consumer, {
    fetch: scriptedFetch(
      fetchResponse('topic-id', [partitionResponse(0, [recordsBatch(0n, ['a', 'b', 'c'])], [[0n, 2n, 1]])])
    )
  })

  const stream = await consumer.consume({ topics: ['topic'], highWaterMark: 2 })
  await sleep(100)
  strictEqual(network.fetches.length, 0)

  stream.pause()
  await waitFor(() => ok(network.fetches.length === 1))

  // The buffer is above the high water mark: the heartbeats do not restart the loop
  await sleep(100)
  strictEqual(network.fetches.length, 1)
  strictEqual(stream.readableLength, 3)

  stream.read()
  stream.read()
  await waitFor(() => ok(network.fetches.length >= 2))
})

test('the stream should wait for the membership and the assignment', async t => {
  const consumer = createShareConsumer(t)
  const network = mockShareNetwork(consumer)
  network.assignment = null

  const stream = await consumer.consume({ topics: ['topic'] })
  stream.resume()
  await sleep(100)
  strictEqual(network.fetches.length, 0)

  network.assignment = [{ topicId: 'topic-id', partitions: [0] }]
  await waitFor(() => ok(network.fetches.length >= 1))

  // While rejoining the stream stops fetching
  network.memberEpoch = 0
  await waitFor(() => strictEqual(consumer.isActive(), false), { timeout: 2000 })
  const fetches = network.fetches.length
  await sleep(100)
  ok(network.fetches.length <= fetches + 1)
})

test('a topic of the assignment which was just deleted should not stop the other topics', async t => {
  const consumer = createShareConsumer(t)
  let deleted = false
  const network = mockShareNetwork(consumer, {
    topics: { topic: { id: 'topic-id', leaders: [0] }, other: { id: 'other-id', leaders: [0] } },
    metadata: options =>
      deleted && options.topics!.includes('other') ? new UserError('Unknown topic other.') : undefined,
    fetch: scriptedFetch(fetchResponse('topic-id', [partitionResponse(0, [recordsBatch(0n, ['a'])], [[0n, 0n, 1]])]))
  })

  const stream = await consumer.consume({ topics: ['topic', 'other'] })
  await waitFor(() => strictEqual(consumer.assignments?.length, 2))

  // The coordinator has not noticed the deletion yet, so the heartbeats keep the assignment
  network.assignment = null
  deleted = true

  const [message] = await read(stream, 1)
  strictEqual(message.value, 'a')
  stream.resume()

  await waitFor(() => ok(network.fetches.length >= 2))
  deepStrictEqual(
    network.fetches[0].topics.map(topic => topic.topicId),
    ['topic-id']
  )
  deepStrictEqual(acknowledgementsOf(network.fetches[1]), [[0, 0n, 0n, ACCEPT]])
  strictEqual(stream.destroyed, false)
})

test('the stream should fail when the metadata of a single topic cannot be loaded', async t => {
  const consumer = createShareConsumer(t)
  const failure = new NetworkError('Metadata failed.')
  let fail = false
  const network = mockShareNetwork(consumer, {
    topics: { topic: { id: 'topic-id', leaders: [0] }, other: { id: 'other-id', leaders: [0] } },
    metadata (options) {
      if (!fail) {
        return undefined
      }

      return options.topics!.length > 1
        ? new UserError('Unknown topic other.')
        : options.topics![0] === 'other'
          ? failure
          : undefined
    }
  })

  const stream = await consumer.consume({ topics: ['topic', 'other'] })
  await waitFor(() => strictEqual(consumer.assignments?.length, 2))
  network.assignment = null
  fail = true
  stream.resume()

  const [error] = await once(stream, 'error')
  strictEqual(error.errors[0], failure)
  strictEqual(network.fetches.length, 0)
})

test('the stream should fail when the metadata cannot be loaded', async t => {
  const consumer = createShareConsumer(t)
  const failure = new NetworkError('Metadata failed.')
  let fail = false
  mockShareNetwork(consumer, { metadata: () => (fail ? failure : undefined) })

  const stream = await consumer.consume({ topics: ['topic'] })
  fail = true
  stream.resume()

  const [error] = await once(stream, 'error')
  strictEqual(error, failure)
})

test('the stream should use the arguments of ShareFetch v1 and ShareAcknowledge v1', async t => {
  const consumer = createShareConsumer(t)
  const network = mockShareNetwork(consumer, {
    fetchVersion: 1,
    acknowledgeVersion: 1,
    fetch: scriptedFetch(
      fetchResponse('topic-id', [partitionResponse(0, [recordsBatch(0n, ['a'])], [[0n, 0n, 1]])]),
      null
    )
  })

  const stream = await consumer.consume({ topics: ['topic'], maxRecords: 7, maxBytes: 100, minBytes: 5 })
  await read(stream, 1)
  await stream.close()

  deepStrictEqual(
    [network.fetches[0].minBytes, network.fetches[0].maxBytes, network.fetches[0].maxRecords, network.fetches[0].extra],
    [5, 100, 7, []]
  )
  deepStrictEqual(network.acknowledgements[0].extra, [])
})

test('consume should fail when the consumer is already consuming or joined with other topics', async t => {
  const consumer = createShareConsumer(t)
  mockShareNetwork(consumer, {
    topics: { topic: { id: 'topic-id', leaders: [0] }, other: { id: 'other-id', leaders: [0] } }
  })

  await consumer.joinGroup({ topics: ['topic', 'other'] })
  await rejects(consumer.consume({ topics: ['topic'] }), {
    message: 'The consumer has joined the group with different topics.'
  })
  await rejects(consumer.consume({ topics: ['topic', 'missing'] }), {
    message: 'The consumer has joined the group with different topics.'
  })

  // The same topics in a different order use the existing membership
  const stream = await consumer.consume({ topics: ['other', 'topic'] })
  await rejects(consumer.consume({ topics: ['topic', 'other'] }), { message: 'The consumer is already consuming.' })

  // Once the stream is closed, the consumer can consume again
  await stream.close()
  const another = await consumer.consume({ topics: ['topic', 'other'] })
  ok(another !== stream)
})

test('consume should validate the options', async t => {
  const consumer = createShareConsumer(t)

  await rejects(consumer.consume({ topics: [] }), (error: Error) => {
    ok(error instanceof UserError)
    ok(error.message.includes('/options/topics must NOT have fewer than 1 items'))
    return true
  })
  await rejects(consumer.consume({ topics: ['topic'], maxRecords: 0 }), (error: Error) => {
    ok(error.message.includes('/options/maxRecords must be >= 1'))
    return true
  })
  await rejects(consumer.consume({ topics: ['topic'], maxWaitTime: 1.5 }), (error: Error) => {
    ok(error.message.includes('/options/maxWaitTime must be integer'))
    return true
  })
  await rejects(consumer.consume({ topics: ['topic'], unknown: true } as any), (error: Error) => {
    ok(error.message.includes('must NOT have additional properties'))
    return true
  })
})

test('consume should report errors from joining the group', async t => {
  const consumer = createShareConsumer(t)
  mockShareNetwork(consumer)
  const mocked = consumer as unknown as Record<symbol, unknown>
  mocked[kGetApi] = (_name: string, callback: Callback<unknown>) => {
    callback(new UnsupportedApiError('Broker does not support API ShareGroupHeartbeat.'))
  }

  await rejects(consumer.consume({ topics: ['topic'] }), {
    message: 'Broker does not support API ShareGroupHeartbeat.'
  })
})

test('consume should support callbacks', (t, done) => {
  const consumer = createShareConsumer(t)
  mockShareNetwork(consumer)

  consumer.consume({ topics: ['topic'] }, (error, stream) => {
    strictEqual(error, null)
    stream!.close(error => {
      strictEqual(error, null)
      done()
    })
  })
})

test('consume should fail when the consumer is closed', async t => {
  const consumer = createShareConsumer(t)
  await consumer.close()

  await rejects(consumer.consume({ topics: ['topic'] }), { message: 'Client is closed.' })
})

test('leaveGroup with force should close the stream before leaving', async t => {
  const consumer = createShareConsumer(t)
  const network = mockShareNetwork(consumer, {
    fetch: scriptedFetch(
      fetchResponse('topic-id', [partitionResponse(0, [recordsBatch(0n, ['a', 'b'])], [[0n, 1n, 1]])]),
      null
    )
  })

  const stream = await consumer.consume({ topics: ['topic'], highWaterMark: 1 })
  await read(stream, 1)

  const order: string[] = []
  stream.once('close', () => order.push('stream'))
  consumer.once('share-consumer:group:leave', () => order.push('group'))
  await consumer.leaveGroup(true)

  deepStrictEqual(order, ['stream', 'group'])
  deepStrictEqual(acknowledgementsOf(network.acknowledgements[0]), [
    [0, 0n, 0n, ACCEPT],
    [0, 1n, 1n, RELEASE]
  ])
})

test('close should close the stream before leaving', async t => {
  const consumer = createShareConsumer(t)
  const network = mockShareNetwork(consumer)

  const stream = await consumer.consume({ topics: ['topic'] })
  stream.resume()
  await waitFor(() => ok(network.fetches.length >= 2))

  const order: string[] = []
  stream.once('close', () => order.push('stream'))
  consumer.once('share-consumer:group:leave', () => order.push('group'))
  await consumer.close()

  deepStrictEqual(order, ['stream', 'group'])
  strictEqual(network.acknowledgements.length, 1)
})

test('close should wait for a leave closing the stream', async t => {
  const consumer = createShareConsumer(t)
  let release: () => void
  const network = mockShareNetwork(consumer, {
    acknowledge (_, callback) {
      release = () => callback(null, acknowledgeResponse())
    }
  })

  const stream = await consumer.consume({ topics: ['topic'] })
  stream.resume()
  await waitFor(() => ok(network.fetches.length >= 2))

  const leaving = consumer.leaveGroup(true)
  await waitFor(() => ok(network.acknowledgements.length === 1))
  const closing = consumer.close()

  release!()
  await Promise.all([leaving, closing])
  strictEqual(stream.closed, true)
})

test('leaveGroup should fail while a stream is consuming, unless forced', async t => {
  const consumer = createShareConsumer(t)
  mockShareNetwork(consumer)

  const stream = await consumer.consume({ topics: ['topic'] })
  stream.resume()

  await rejects(consumer.leaveGroup(), { message: 'Cannot leave group while consuming messages.' })
  await new Promise<void>(resolve => {
    consumer.leaveGroup(error => {
      strictEqual(error?.message, 'Cannot leave group while consuming messages.')
      resolve()
    })
  })

  strictEqual(stream.closed, false)
  strictEqual(consumer.isActive(), true)

  await new Promise<void>((resolve, reject) => {
    consumer.leaveGroup(true, error => (error ? reject(error) : resolve()))
  })

  strictEqual(stream.closed, true)
  strictEqual(consumer.isActive(), false)
})

test('joinGroup and consume should fail while the consumer is joining', async t => {
  const consumer = createShareConsumer(t)
  mockShareNetwork(consumer, { heartbeatDelay: 20 })

  const joining = consumer.joinGroup({ topics: ['topic'] })
  await rejects(consumer.joinGroup({ topics: ['topic'] }), { message: 'The consumer is already joining the group.' })
  await rejects(consumer.consume({ topics: ['topic'] }), { message: 'The consumer is already joining the group.' })
  await joining
})

test('a replaced session should still send its acknowledgements when its broker leads no partition anymore', async t => {
  const consumer = createShareConsumer(t)
  const topics = { topic: { id: 'topic-id', leaders: [0, 1] } }
  const sessionError = new ResponseError(
    78,
    2,
    { '/': [protocolErrors.SHARE_SESSION_NOT_FOUND.code, null] },
    {
      ...emptyFetch(),
      errorCode: protocolErrors.SHARE_SESSION_NOT_FOUND.code
    }
  )
  let brokerRequests = 0
  const network = mockShareNetwork(consumer, {
    topics,
    fetch (request, callback) {
      let reply: ShareFetchResponse | Error = emptyFetch()

      if (request.broker.host === 'broker-1') {
        brokerRequests++

        if (brokerRequests === 1) {
          reply = fetchResponse('topic-id', [partitionResponse(1, [recordsBatch(0n, ['a'])], [[0n, 0n, 1]])])
        } else if (brokerRequests === 2) {
          // The session is lost while the leadership of partition 1 moves to the other broker
          topics.topic.leaders[1] = 0
          reply = sessionError
        }
      }

      setTimeout(() => (reply instanceof Error ? callback(reply) : callback(null, reply)), 2)
    }
  })

  const stream = await consumer.consume({ topics: ['topic'] })
  stream.on('data', () => {})

  // The replacement is opened without partitions, then acknowledges the record and removes the partition
  await waitFor(() => {
    const requests = network.fetches.filter(request => request.broker.host === 'broker-1')
    deepStrictEqual(
      requests
        .slice(1, 4)
        .map(request => [
          request.epoch,
          partitionsOf(request),
          acknowledgementsOf(request),
          request.forgotten,
          request.maxWaitMs
        ]),
      [
        [1, [1], [[1, 0n, 0n, ACCEPT]], [], 10],
        [0, [], [], [], 0],
        [1, [1], [[1, 0n, 0n, ACCEPT]], [{ topicId: 'topic-id', partitions: [1] }], 0]
      ]
    )
  })

  // Nothing is left to send to that broker
  await sleep(50)
  strictEqual(network.fetches.filter(request => request.broker.host === 'broker-1').length, 4)
})

test('closing the stream should open a replaced session to send its acknowledgements', async t => {
  const consumer = createShareConsumer(t, { retryDelay: 10_000 })
  const network = mockShareNetwork(consumer, {
    fetch: scriptedFetch(
      fetchResponse('topic-id', [partitionResponse(0, [recordsBatch(0n, ['a'])], [[0n, 0n, 1]])]),
      new NetworkError('Connection lost.')
    )
  })

  const stream = await consumer.consume({ topics: ['topic'] })
  stream.on('data', () => {})

  // The request with the acknowledgement fails, and the next one waits for the retry delay
  await waitFor(() => ok(network.fetches.length === 2))
  deepStrictEqual(acknowledgementsOf(network.fetches[1]), [[0, 0n, 0n, ACCEPT]])
  await sleep(20)
  await stream.close()

  deepStrictEqual(
    network.fetches.slice(2).map(request => [request.epoch, request.topics, request.maxWaitMs]),
    [[0, [], 0]]
  )
  strictEqual(network.acknowledgements.length, 1)
  strictEqual(network.acknowledgements[0].epoch, -1)
  deepStrictEqual(acknowledgementsOf(network.acknowledgements[0]), [[0, 0n, 0n, ACCEPT]])
})

test('closing the stream should not fail when a replaced session cannot be opened', async t => {
  const consumer = createShareConsumer(t, { retryDelay: 10_000 })
  let unreachable = false
  const network = mockShareNetwork(consumer, {
    poolGet: () => (unreachable ? new NetworkError('Connection failed.') : undefined),
    fetch: scriptedFetch(
      fetchResponse('topic-id', [partitionResponse(0, [recordsBatch(0n, ['a'])], [[0n, 0n, 1]])]),
      new NetworkError('Connection lost.')
    )
  })

  const stream = await consumer.consume({ topics: ['topic'] })
  stream.on('data', () => {})
  await waitFor(() => ok(network.fetches.length === 2))
  await sleep(20)

  unreachable = true
  await stream.close()
  strictEqual(network.fetches.length, 2)
  strictEqual(network.acknowledgements.length, 0)
})

test('messages should deserialize headers and convert to JSON', async t => {
  const consumer = createShareConsumer(t, {
    deserializers: {
      key: (data?: Buffer) => data?.toString().toUpperCase(),
      value: (data, headers) => `${data?.toString()}-${headers?.get('h1')}`,
      headerKey: (data?: Buffer) => data?.toString(),
      headerValue: (data?: Buffer) => data?.toString()
    }
  })
  mockShareNetwork(consumer, {
    fetch: scriptedFetch(
      fetchResponse('topic-id', [
        partitionResponse(
          0,
          [recordsBatch(5n, ['a'], { headers: [[Buffer.from('h1'), Buffer.from('v1')]] })],
          [[5n, 5n, 3]]
        )
      ]),
      null
    )
  })

  const stream = await consumer.consume({ topics: ['topic'] })
  const [message] = await read(stream, 1)

  strictEqual(message.key, 'KEY-A')
  strictEqual(message.value, 'a-v1')
  deepStrictEqual(message.headerEntries, [['h1', 'v1']])
  deepStrictEqual(message.toJSON(), {
    key: 'KEY-A',
    value: 'a-v1',
    headers: [['h1', 'v1']],
    topic: 'topic',
    partition: 0,
    timestamp: '1000',
    offset: '5',
    deliveryCount: 3
  })
})

test('messages should build their headers Map when it is read', async t => {
  const consumer = createShareConsumer(t)
  mockShareNetwork(consumer, {
    fetch: scriptedFetch(
      fetchResponse('topic-id', [
        partitionResponse(
          0,
          [recordsBatch(0n, ['a'], { headers: [[Buffer.from('h1'), Buffer.from('v1')]] })],
          [[0n, 0n, 1]]
        )
      ]),
      null
    )
  })

  const stream = await consumer.consume({
    topics: ['topic'],
    deserializers: { headerKey: (data?: Buffer) => data?.toString() }
  })
  const [message] = await read(stream, 1)

  deepStrictEqual(message.headers, new Map([['h1', 'v1']]))
  strictEqual(message.headers, message.headers)

  const headers = new Map([['other', 'value']])
  message.headers = headers
  strictEqual(message.headers, headers)
})

test('a deserialization failure should fail the stream', async t => {
  const consumer = createShareConsumer(t)
  const network = mockShareNetwork(consumer, {
    fetch: scriptedFetch(
      fetchResponse('topic-id', [partitionResponse(0, [recordsBatch(0n, ['a', 'b'])], [[0n, 1n, 1]])]),
      null
    )
  })

  const stream = await consumer.consume({
    topics: ['topic'],
    deserializers: {
      value (data?: Buffer) {
        if (data?.toString() === 'b') {
          throw new Error('Cannot deserialize.')
        }

        return data?.toString()
      }
    }
  })

  const values: string[] = []
  stream.on('data', message => values.push(message.value))
  const [error] = await once(stream, 'error')

  strictEqual(error.message, 'Failed to deserialize a message.')
  strictEqual(error.cause.message, 'Cannot deserialize.')
  deepStrictEqual(values, ['a'])
  await waitFor(() => ok(stream.closed))
  deepStrictEqual(acknowledgementsOf(network.acknowledgements[0]), [[0, 0n, 0n, ACCEPT]])
})

test('a share consumer should accept the records it reads and release the others on close', async t => {
  const admin = createAdmin(t, { bootstrapBrokers: kafkaSingleBootstrapServers })
  if (!(await usableVersions(admin, 'ShareFetch')).length) {
    t.skip('the broker does not support share groups')
    return
  }

  const topic = await createTopic(t, true, 1, kafkaSingleBootstrapServers)
  const groupId = `test-share-group-${randomUUID()}`
  const createConsumer = () =>
    createShareConsumer(t, { bootstrapBrokers: kafkaSingleBootstrapServers, groupId, maxWaitTime: 500 })

  const first = createConsumer()

  try {
    await first.joinGroup({ topics: [topic] })
  } catch (error) {
    // Some brokers advertise the API but ship with share groups disabled
    if (isUnsupportedVersion(error)) {
      t.skip('share groups are not enabled on the broker')
      return
    }

    throw error
  }

  // A new share group starts from the end of the partitions
  await admin.incrementalAlterConfigs({
    resources: [
      {
        resourceType: ConfigResourceTypes.GROUP,
        resourceName: groupId,
        configs: [
          {
            name: 'share.auto.offset.reset',
            configOperation: IncrementalAlterConfigOperationTypes.SET,
            value: 'earliest'
          }
        ]
      }
    ]
  })

  const producer = createProducer(t, { bootstrapBrokers: kafkaSingleBootstrapServers, serializers: stringSerializers })
  await producer.send({
    messages: Array.from({ length: 10 }, (_, i) => ({ topic, partition: 0, key: `key-${i}`, value: `value-${i}` }))
  })

  // The ten records arrive in one batch, so they are all acquired by the first consumer
  const stream = await first.consume({ topics: [topic], highWaterMark: 1 })
  const read: string[] = []

  for await (const message of stream) {
    strictEqual(message.deliveryCount, 1)
    read.push(message.value)

    if (read.length === 4) {
      break
    }
  }

  deepStrictEqual(read, ['value-0', 'value-1', 'value-2', 'value-3'])
  await first.close()

  // The records which were read are not delivered again, the others are, with a higher delivery count
  const second = createConsumer()
  const secondStream = await second.consume({ topics: [topic] })
  const redelivered: Array<[string, number]> = []

  for await (const message of secondStream) {
    redelivered.push([message.value, message.deliveryCount])

    if (redelivered.length === 6) {
      break
    }
  }

  deepStrictEqual(
    redelivered,
    Array.from({ length: 6 }, (_, i) => [`value-${i + 4}`, 2])
  )
})
