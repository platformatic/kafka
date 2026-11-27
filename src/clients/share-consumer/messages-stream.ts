import { Readable } from 'node:stream'
import {
  type CallbackWithPromise,
  createPromisifiedCallback,
  kCallbackPromise,
  runConcurrentCallbacks
} from '../../apis/callbacks.ts'
import { type ShareAcknowledgeResponse } from '../../apis/consumer/share-acknowledge-v2.ts'
import {
  type ShareFetchRequestAcknowledgementBatch,
  type ShareFetchRequestTopic,
  type ShareFetchResponse,
  type ShareFetchResponseAcquiredRecords,
  type ShareFetchResponsePartition
} from '../../apis/consumer/share-fetch-v2.ts'
import { ShareAcknowledgeTypes, ShareAcquireModes } from '../../apis/enumerations.ts'
import { findErrorBy, type GenericError, ProtocolError, UserError } from '../../errors.ts'
import { type ConnectionPool } from '../../network/connection-pool.ts'
import { type Broker } from '../../network/connection.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { IS_CONTROL, type KafkaRecord, type MessageToConsume } from '../../protocol/records.ts'
import { kConnections, kCreateConnectionPool, kGetApi, kMetadata, kOptions } from '../base/base.ts'
import { type ClusterMetadata, type RetryDelayGetter } from '../base/types.ts'
import { type GroupAssignment } from '../consumer/types.ts'
import { type Deserializer, type DeserializerWithHeaders, jsonDeserializer, stringDeserializer } from '../serde.ts'
import { defaultShareFetchOptions } from './options.ts'
import { type ShareConsumer } from './share-consumer.ts'
import { type ShareConsumeOptions, type ShareMessage, type ShareMessageJSON } from './types.ts'

/* c8 ignore next 3 - Fallback deserializer, nothing to really test */
function noopDeserializer (data?: Buffer): Buffer | undefined {
  return data
}

// The deserializers which do not read the headers of the message they are given
const dataOnlyDeserializers = new Set<unknown>([noopDeserializer, stringDeserializer, jsonDeserializer])

// Share session epochs wrap around to 1, since 0 opens a new session
const maxSessionEpoch = 2 ** 31 - 1

interface SharePartition {
  key: string
  topic: string
  topicId: string
  partition: number
}

interface AcknowledgementRange {
  firstOffset: bigint
  lastOffset: bigint
  type: number
}

interface PendingAcknowledgements {
  partition: SharePartition
  ranges: AcknowledgementRange[]
}

// The share session of the member on a broker. The broker keys it by group and member, so there is only one per broker.
interface ShareSession {
  nodeId: number
  broker: Broker
  // 0 opens the session, and acknowledgements cannot be sent with it
  epoch: number
  inflight: boolean
  // The partitions the broker fetches for this session
  partitions: Map<string, SharePartition>
  // Sent with the next request of the session
  acknowledgements: Map<string, PendingAcknowledgements>
  failures: number
  retryTimeout: NodeJS.Timeout | null
}

// A record which was pushed to the stream, but not handed to the application yet
// The broker locks the acquired records for the member, so any session of the member on that broker can acknowledge them
interface BufferedRecord {
  nodeId: number
  partition: SharePartition
  offset: bigint
}

type ShareFetchRequestArguments = unknown[]
type ShareAcknowledgeRequestArguments = unknown[]

class ShareConsumedMessage<Key, Value, HeaderKey, HeaderValue> implements ShareMessage<
  Key,
  Value,
  HeaderKey,
  HeaderValue
> {
  #headers: Map<HeaderKey, HeaderValue> | undefined
  key: Key
  value: Value
  headerEntries: Array<[HeaderKey, HeaderValue]>
  topic: string
  partition: number
  timestamp: bigint
  offset: bigint
  deliveryCount: number

  constructor (
    key: Key,
    value: Value,
    headers: Map<HeaderKey, HeaderValue> | undefined,
    headerEntries: Array<[HeaderKey, HeaderValue]>,
    topic: string,
    partition: number,
    timestamp: bigint,
    offset: bigint,
    deliveryCount: number
  ) {
    this.#headers = headers
    this.key = key
    this.value = value
    this.headerEntries = headerEntries
    this.topic = topic
    this.partition = partition
    this.timestamp = timestamp
    this.offset = offset
    this.deliveryCount = deliveryCount
  }

  // The Map is built when it is first read, since headerEntries already holds the headers
  get headers (): Map<HeaderKey, HeaderValue> {
    if (!this.#headers) {
      this.#headers = new Map(this.headerEntries)
    }

    return this.#headers
  }

  set headers (headers: Map<HeaderKey, HeaderValue>) {
    this.#headers = headers
  }

  toJSON (): ShareMessageJSON<Key, Value, HeaderKey, HeaderValue> {
    return {
      key: this.key,
      value: this.value,
      headers: this.headerEntries.map(([key, value]): [HeaderKey, HeaderValue] => [key, value]),
      topic: this.topic,
      partition: this.partition,
      timestamp: this.timestamp.toString(),
      offset: this.offset.toString(),
      deliveryCount: this.deliveryCount
    }
  }
}

/*
  Fetches the records of the partitions assigned to the member and acknowledges them implicitly: a record is
  accepted once the stream has handed it to the application, and the acknowledgement is sent with the next request
  to the same broker. Records still buffered when the stream is closed are released, so the group delivers them again.
*/
export class ShareMessagesStream<
  Key = Buffer,
  Value = Buffer,
  HeaderKey = Buffer,
  HeaderValue = Buffer
> extends Readable {
  #consumer: ShareConsumer<Key, Value, HeaderKey, HeaderValue>
  #minBytes: number
  #maxBytes: number
  #maxWaitTime: number
  #maxRecords: number
  #keyDeserializer: DeserializerWithHeaders<Key, HeaderKey, HeaderValue>
  #valueDeserializer: DeserializerWithHeaders<Value, HeaderKey, HeaderValue>
  #headerKeyDeserializer: Deserializer<HeaderKey>
  #headerValueDeserializer: Deserializer<HeaderValue>
  #deserializersUseHeaders: boolean
  #sessions: Map<number, ShareSession>
  #buffered: Map<ShareMessage<Key, Value, HeaderKey, HeaderValue>, BufferedRecord>
  #closing: boolean
  // The buffer is full, so the loop waits for _read
  #waitingForRead: boolean
  #onHeartbeat: () => void;

  // Fetches use their own connections, since a ShareFetch waits up to maxWaitTime and would delay the heartbeats
  [kConnections]: ConnectionPool

  constructor (
    consumer: ShareConsumer<Key, Value, HeaderKey, HeaderValue>,
    options: ShareConsumeOptions<Key, Value, HeaderKey, HeaderValue>
  ) {
    const maxRecords = options.maxRecords ?? defaultShareFetchOptions.maxRecords
    super({ objectMode: true, highWaterMark: options.highWaterMark ?? maxRecords })

    this.#consumer = consumer
    this.#minBytes = options.minBytes ?? defaultShareFetchOptions.minBytes
    this.#maxBytes = options.maxBytes ?? defaultShareFetchOptions.maxBytes
    this.#maxWaitTime = options.maxWaitTime ?? defaultShareFetchOptions.maxWaitTime
    this.#maxRecords = maxRecords
    this.#keyDeserializer =
      options.deserializers?.key ?? (noopDeserializer as DeserializerWithHeaders<Key, HeaderKey, HeaderValue>)
    this.#valueDeserializer =
      options.deserializers?.value ?? (noopDeserializer as DeserializerWithHeaders<Value, HeaderKey, HeaderValue>)
    this.#headerKeyDeserializer = options.deserializers?.headerKey ?? (noopDeserializer as Deserializer<HeaderKey>)
    this.#headerValueDeserializer =
      options.deserializers?.headerValue ?? (noopDeserializer as Deserializer<HeaderValue>)
    this.#deserializersUseHeaders =
      !dataOnlyDeserializers.has(this.#keyDeserializer) || !dataOnlyDeserializers.has(this.#valueDeserializer)
    this.#sessions = new Map()
    this.#buffered = new Map()
    this.#closing = false
    this.#waitingForRead = false
    this[kConnections] = consumer[kCreateConnectionPool]()

    // The assignment arrives with the heartbeats, so they restart a fetch loop waiting for one
    this.#onHeartbeat = () => {
      this.#fetch()
    }

    consumer.on('share-consumer:heartbeat:end', this.#onHeartbeat)
  }

  /* c8 ignore next 3 - Simple getter */
  get consumer (): ShareConsumer<Key, Value, HeaderKey, HeaderValue> {
    return this.#consumer
  }

  close (callback: CallbackWithPromise<void>): void
  close (): Promise<void>
  close (callback?: CallbackWithPromise<void>): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback<void>()
    }

    if (this.closed) {
      callback(null)
      return callback[kCallbackPromise]
    }

    let closeError: Error | null = null
    const onError = (error: Error) => {
      closeError = error
    }

    this.once('error', onError)
    this.once('close', () => {
      this.removeListener('error', onError)
      callback(closeError)
    })

    if (this.destroyed || this.#closing) {
      return callback[kCallbackPromise]
    }

    this.#closing = true
    this.#releaseBuffered()

    // Ending the stream, rather than destroying it, lets a for await loop finish without an error.
    // The buffered records are released, so they are discarded instead of reaching the application.
    this.push(null)
    this.removeAllListeners('data')
    this.removeAllListeners('readable')

    // A for await loop reads the buffer synchronously, so it is emptied before the loop can read it
    while (this.read() !== null) {
      // Nothing to do, the records are discarded
    }

    this.resume()

    return callback[kCallbackPromise]
  }

  // Every record reaches the application through a data event, whether the stream is read with an async iterator,
  // a data listener or a pipe. That is when it is acknowledged.
  emit (event: string | symbol, ...args: any[]): boolean {
    if (event === 'data') {
      this.#delivered(args[0])
    }

    return super.emit(event, ...args)
  }

  _read (): void {
    this.#waitingForRead = false
    this.#fetch()
  }

  _destroy (error: Error | null, callback: (error?: Error | null) => void): void {
    this.#closing = true
    this.#consumer.removeListener('share-consumer:heartbeat:end', this.#onHeartbeat)
    this.#releaseBuffered()

    // Closing the sessions sends their acknowledgements and releases the records which were not acknowledged
    runConcurrentCallbacks<void, ShareSession>(
      'Closing the share sessions failed.',
      this.#sessions.values(),
      (session, concurrentCallback) => {
        this.#closeSession(session, concurrentCallback)
      },
      () => {
        this.#sessions.clear()

        this[kConnections].close(closeError => {
          callback(error ?? closeError)
        })
      }
    )
  }

  #fetch (): void {
    // Nobody is reading or the buffer is full: _read restarts the loop
    if (this.#closing || this.destroyed || this.readableFlowing === null || this.#waitingForRead) {
      return
    }

    // Not a member of the group: the next heartbeat restarts the loop
    if (!this.#consumer.isActive()) {
      return
    }

    const assignments = this.#consumer.assignments ?? []

    if (!assignments.length) {
      this.#sendRequests(new Map())
      return
    }

    this.#consumer[kMetadata]({ topics: assignments.map(assignment => assignment.topic) }, (error, metadata) => {
      if (this.#closing || this.destroyed) {
        return
      }

      if (error) {
        // A topic of the assignment was deleted: the next heartbeat removes it from the assignment
        if (error instanceof UserError) {
          return
        }

        this.destroy(error)
        return
      }

      this.#sendRequests(this.#partitionsByLeader(metadata!, assignments))
    })
  }

  #partitionsByLeader (
    metadata: ClusterMetadata,
    assignments: GroupAssignment[]
  ): Map<number, Map<string, SharePartition>> {
    const partitionsByLeader = new Map<number, Map<string, SharePartition>>()

    for (const { topic, partitions } of assignments) {
      const { id: topicId, partitions: topicPartitions } = metadata.topics.get(topic)!

      for (const partition of partitions) {
        const leader = topicPartitions[partition]?.leader

        // The partition was created after the metadata was loaded, or it has no leader right now
        if (leader === undefined || !metadata.brokers.has(leader)) {
          continue
        }

        let leaderPartitions = partitionsByLeader.get(leader)
        if (!leaderPartitions) {
          leaderPartitions = new Map()
          partitionsByLeader.set(leader, leaderPartitions)
        }

        const key = `${topicId}:${partition}`
        leaderPartitions.set(key, { key, topic, topicId, partition })
      }
    }

    for (const nodeId of partitionsByLeader.keys()) {
      const broker = metadata.brokers.get(nodeId)!
      const session = this.#sessions.get(nodeId)

      if (!session) {
        this.#sessions.set(nodeId, this.#createSession(nodeId, broker))
      } else {
        session.broker = broker
      }
    }

    return partitionsByLeader
  }

  #sendRequests (partitionsByLeader: Map<number, Map<string, SharePartition>>): void {
    // Sessions on brokers which no longer lead any assigned partition still send their acknowledgements
    for (const session of this.#sessions.values()) {
      if (!session.inflight && !session.retryTimeout) {
        this.#shareFetch(session, partitionsByLeader.get(session.nodeId) ?? new Map())
      }
    }
  }

  #shareFetch (session: ShareSession, wanted: Map<string, SharePartition>): void {
    const topics = new Map<string, ShareFetchRequestTopic>()
    const forgotten = new Map<string, number[]>()
    const sent: SharePartition[] = []
    const removed: SharePartition[] = []
    let acknowledgements: Map<string, PendingAcknowledgements> = new Map()

    const addPartition = (
      partition: SharePartition,
      acknowledgementBatches: ShareFetchRequestAcknowledgementBatch[]
    ) => {
      let topic = topics.get(partition.topicId)
      if (!topic) {
        topic = { topicId: partition.topicId, partitions: [] }
        topics.set(partition.topicId, topic)
      }

      topic.partitions.push({ partitionIndex: partition.partition, acknowledgementBatches })
      sent.push(partition)
    }

    if (session.epoch === 0) {
      // A new session only has the partitions which are assigned now
      for (const partition of wanted.values()) {
        addPartition(partition, [])
      }
    } else {
      acknowledgements = session.acknowledgements
      session.acknowledgements = new Map()

      for (const { partition, ranges } of acknowledgements.values()) {
        addPartition(partition, toAcknowledgementBatches(ranges))
      }

      for (const [key, partition] of wanted) {
        if (!session.partitions.has(key) && !acknowledgements.has(key)) {
          addPartition(partition, [])
        }
      }

      // A partition can be acknowledged and removed from the session with the same request
      for (const partition of new Map([
        ...session.partitions,
        ...sent.map(sent => [sent.key, sent] as const)
      ]).values()) {
        if (!wanted.has(partition.key)) {
          let partitions = forgotten.get(partition.topicId)
          if (!partitions) {
            partitions = []
            forgotten.set(partition.topicId, partitions)
          }

          partitions.push(partition.partition)
          removed.push(partition)
        }
      }
    }

    // Nothing to fetch, acknowledge or remove from the session
    if (!wanted.size && !sent.length && !removed.length) {
      return
    }

    const epoch = session.epoch
    session.inflight = true

    const request: ShareFetchRequestArguments = [
      this.#consumer.groupId,
      this.#consumer.memberId,
      epoch,
      // Without partitions to fetch the broker would only wait
      wanted.size ? this.#maxWaitTime : 0,
      this.#minBytes,
      this.#maxBytes,
      this.#maxRecords,
      this.#maxRecords
    ]

    const forgottenTopicsData = Array.from(forgotten, ([topicId, partitions]) => ({ topicId, partitions }))

    this[kConnections].get(session.broker, (error, connection) => {
      if (error) {
        this.#handleShareFetch(session, sent, removed, acknowledgements, error)
        return
      }

      this.#consumer[kGetApi]<ShareFetchRequestArguments, ShareFetchResponse>('ShareFetch', (error, api) => {
        if (error) {
          this.#handleShareFetch(session, sent, removed, acknowledgements, error)
          return
        }

        if (api!.version >= 2) {
          request.push(ShareAcquireModes.BATCH_OPTIMIZED, false)
        }

        api!(connection!, ...request, Array.from(topics.values()), forgottenTopicsData, (
          error: Error | null,
          response?: ShareFetchResponse
        ) => {
          this.#handleShareFetch(session, sent, removed, acknowledgements, error, response)
        })
      })
    })
  }

  #handleShareFetch (
    session: ShareSession,
    sent: SharePartition[],
    removed: SharePartition[],
    acknowledgements: Map<string, PendingAcknowledgements>,
    error: Error | null,
    response?: ShareFetchResponse
  ): void {
    session.inflight = false

    // The records acquired by this request are released when the session is closed
    if (this.#closing || this.destroyed) {
      return
    }

    // The response of a failed partition or acknowledgement still holds the records acquired for the other partitions
    const errorResponse = (error as GenericError | null)?.response as ShareFetchResponse | undefined
    if (errorResponse?.errorCode === 0) {
      error = null
      response = errorResponse
    }

    if (error) {
      this.#handleShareFetchError(session, acknowledgements, error)
      return
    }

    session.epoch = session.epoch === maxSessionEpoch ? 1 : session.epoch + 1

    const partitions = new Map<string, SharePartition>()
    for (const partition of sent) {
      session.partitions.set(partition.key, partition)
      partitions.set(partition.key, partition)
    }

    for (const partition of removed) {
      session.partitions.delete(partition.key)
    }

    let canPush = true
    let partitionError: Error | null = null

    for (const { topicId, partitions: partitionResponses } of response!.responses) {
      for (const partitionResponse of partitionResponses) {
        const key = `${topicId}:${partitionResponse.partitionIndex}`
        const partition = session.partitions.get(key) ?? partitions.get(key)

        /* c8 ignore next 3 - The broker only answers for the partitions of the session */
        if (!partition) {
          continue
        }

        if (partitionResponse.acknowledgeErrorCode !== 0) {
          this.#acknowledgementFailed(
            partition,
            partitionResponse.acknowledgeErrorCode,
            partitionResponse.acknowledgeErrorMessage
          )
        }

        if (partitionResponse.errorCode !== 0) {
          partitionError ??= new ProtocolError(partitionResponse.errorCode, partitionResponse.errorMessage)
          continue
        }

        canPush = this.#pushRecords(session, partition, partitionResponse) && canPush

        if (this.#closing || this.destroyed) {
          return
        }
      }
    }

    if (!canPush) {
      this.#waitingForRead = true
    }

    // The other partitions of the session wait too, so that a failing partition does not make the loop spin
    if (partitionError) {
      // The leader moved, or the topic was deleted or recreated
      this.#consumer.clearMetadata()
      this.#retryLater(session, partitionError)
      return
    }

    session.failures = 0

    if (canPush) {
      process.nextTick(() => {
        this.#fetch()
      })
    }
  }

  #handleShareFetchError (
    session: ShareSession,
    acknowledgements: Map<string, PendingAcknowledgements>,
    error: Error
  ): void {
    // The records stay locked for the member, so the acknowledgements of the failed request are sent again
    for (const { partition, ranges } of acknowledgements.values()) {
      for (const { firstOffset, lastOffset, type } of ranges) {
        this.#acknowledge(session, partition, firstOffset, lastOffset, type)
      }
    }

    if (findErrorBy(error, 'hasStaleMetadata', true)) {
      this.#consumer.clearMetadata()
    }

    const sessionLost =
      findErrorBy(error, 'apiId', 'SHARE_SESSION_NOT_FOUND') ||
      findErrorBy(error, 'apiId', 'INVALID_SHARE_SESSION_EPOCH')

    // The broker might not know the session anymore, or the request might have changed its epoch: a new one is opened.
    // The session is only replaced when retrying, so that closing the stream still closes it on the broker.
    if (this.#retryLater(session, error, !!sessionLost)) {
      const replacement = this.#createSession(session.nodeId, session.broker)
      replacement.failures = session.failures
      replacement.acknowledgements = session.acknowledgements
      replacement.retryTimeout = session.retryTimeout
      session.retryTimeout = null
      this.#sessions.set(session.nodeId, replacement)
    }
  }

  // Schedules the next request of the session, or destroys the stream when the error cannot be retried
  #retryLater (session: ShareSession, error: Error, retryNow: boolean = false): boolean {
    const options = this.#consumer[kOptions]
    const retries = options.retries as number
    const failures = ++session.failures

    if ((!retryNow && findErrorBy(error, 'canRetry', false)) || failures > retries) {
      this.destroy(error)
      return false
    }

    let delay = options.retryDelay
    if (typeof delay === 'function') {
      delay = (delay as RetryDelayGetter)(this.#consumer, 'shareFetch', failures, retries, error)
    }

    // A lost session is opened again right away, but only once in a row
    if (retryNow && failures === 1) {
      delay = 0
    }

    session.retryTimeout = setTimeout(() => {
      const current = this.#sessions.get(session.nodeId)!
      current.retryTimeout = null
      this.#fetch()
    }, delay)

    return true
  }

  #pushRecords (session: ShareSession, partition: SharePartition, response: ShareFetchResponsePartition): boolean {
    const acquired = response.acquiredRecords

    if (!acquired.length) {
      return true
    }

    if (acquired.length > 1) {
      acquired.sort((a, b) => (a.firstOffset < b.firstOffset ? -1 : 1))
    }

    const delivered: bigint[] = []
    let canPush = true
    let range = 0

    for (const batch of response.records) {
      // The offsets of transaction markers are acknowledged as gaps below
      if (batch.attributes & IS_CONTROL) {
        continue
      }

      for (const record of batch.records) {
        // The application closed the stream while handling a record: the others are released with the session
        if (this.#closing || this.destroyed) {
          return false
        }

        const offset = batch.firstOffset + BigInt(record.offsetDelta)

        while (range < acquired.length && acquired[range].lastOffset < offset) {
          range++
        }

        // The record was acquired by another member
        if (range === acquired.length || offset < acquired[range].firstOffset) {
          continue
        }

        const message = this.#createMessage(
          record,
          partition,
          offset,
          batch.firstTimestamp + record.timestampDelta,
          acquired[range].deliveryCount
        )

        if (!message) {
          return false
        }

        delivered.push(offset)

        // Tracked before pushing, as push can emit the data event synchronously
        this.#buffered.set(message, { nodeId: session.nodeId, partition, offset })
        canPush = this.push(message)
      }
    }

    this.#acknowledgeGaps(session, partition, acquired, delivered)
    return canPush
  }

  #createMessage (
    record: KafkaRecord,
    partition: SharePartition,
    offset: bigint,
    timestamp: bigint,
    deliveryCount: number
  ): ShareMessage<Key, Value, HeaderKey, HeaderValue> | null {
    const messageToConsume = record as MessageToConsume
    messageToConsume.topic = partition.topic
    messageToConsume.partition = partition.partition

    try {
      const headers = this.#deserializersUseHeaders ? new Map<HeaderKey, HeaderValue>() : undefined
      const headerEntries: Array<[HeaderKey, HeaderValue]> = []

      for (const [headerKey, headerValue] of record.headers) {
        /* c8 ignore next 2 - Header keys and values are never null on the wire */
        const key = this.#headerKeyDeserializer(headerKey ?? undefined, messageToConsume) as HeaderKey
        const value = this.#headerValueDeserializer(headerValue ?? undefined, messageToConsume) as HeaderValue
        headers?.set(key, value)
        headerEntries.push([key, value])
      }

      return new ShareConsumedMessage<Key, Value, HeaderKey, HeaderValue>(
        this.#keyDeserializer(record.key ?? undefined, headers, messageToConsume) as Key,
        this.#valueDeserializer(record.value ?? undefined, headers, messageToConsume) as Value,
        headers,
        headerEntries,
        partition.topic,
        partition.partition,
        timestamp,
        offset,
        deliveryCount
      )
    } catch (error) {
      this.destroy(new UserError('Failed to deserialize a message.', { cause: error }))
      return null
    }
  }

  // Offsets of the acquired records which are not delivered, like transaction markers or compacted records
  #acknowledgeGaps (
    session: ShareSession,
    partition: SharePartition,
    acquired: ShareFetchResponseAcquiredRecords[],
    delivered: bigint[]
  ): void {
    let i = 0

    for (const { firstOffset, lastOffset } of acquired) {
      let next = firstOffset

      while (i < delivered.length && delivered[i] <= lastOffset) {
        if (delivered[i] > next) {
          this.#acknowledge(session, partition, next, delivered[i] - 1n, ShareAcknowledgeTypes.GAP)
        }

        next = delivered[i] + 1n
        i++
      }

      if (next <= lastOffset) {
        this.#acknowledge(session, partition, next, lastOffset, ShareAcknowledgeTypes.GAP)
      }
    }
  }

  #delivered (message: ShareMessage<Key, Value, HeaderKey, HeaderValue>): void {
    const buffered = this.#buffered.get(message)

    if (!buffered) {
      return
    }

    this.#buffered.delete(message)
    const { nodeId, partition, offset } = buffered
    this.#acknowledge(this.#sessions.get(nodeId)!, partition, offset, offset, ShareAcknowledgeTypes.ACCEPT)
  }

  #releaseBuffered (): void {
    for (const { nodeId, partition, offset } of this.#buffered.values()) {
      this.#acknowledge(this.#sessions.get(nodeId)!, partition, offset, offset, ShareAcknowledgeTypes.RELEASE)
    }

    this.#buffered.clear()
  }

  #acknowledge (
    session: ShareSession,
    partition: SharePartition,
    firstOffset: bigint,
    lastOffset: bigint,
    type: number
  ): void {
    let pending = session.acknowledgements.get(partition.key)

    if (!pending) {
      pending = { partition, ranges: [] }
      session.acknowledgements.set(partition.key, pending)
    }

    pending.ranges.push({ firstOffset, lastOffset, type })
  }

  #closeSession (session: ShareSession, callback: CallbackWithPromise<void>): void {
    clearTimeout(session.retryTimeout!)

    // A session being opened by a request still inflight is closed too, as the broker handles the requests in order
    if (session.epoch === 0 && !session.inflight) {
      callback(null)
      return
    }

    const topics = new Map<string, ShareFetchRequestTopic>()
    for (const { partition, ranges } of session.acknowledgements.values()) {
      let topic = topics.get(partition.topicId)
      if (!topic) {
        topic = { topicId: partition.topicId, partitions: [] }
        topics.set(partition.topicId, topic)
      }

      topic.partitions.push({
        partitionIndex: partition.partition,
        acknowledgementBatches: toAcknowledgementBatches(ranges)
      })
    }

    const partitions = new Map([
      ...session.partitions,
      ...Array.from(session.acknowledgements, ([key, { partition }]) => [key, partition] as const)
    ])
    session.acknowledgements = new Map()

    // Errors are ignored: if the session is not closed, the broker releases the records when their lock expires
    const finish = (error: Error | null, response?: ShareAcknowledgeResponse) => {
      response ??= (error as GenericError | null)?.response as ShareAcknowledgeResponse | undefined

      for (const { topicId, partitions: partitionResponses } of response?.responses ?? []) {
        for (const { partitionIndex, errorCode, errorMessage } of partitionResponses) {
          const partition = partitions.get(`${topicId}:${partitionIndex}`)

          if (errorCode !== 0 && partition) {
            this.#acknowledgementFailed(partition, errorCode, errorMessage)
          }
        }
      }

      callback(null)
    }

    this[kConnections].get(session.broker, (error, connection) => {
      if (error) {
        finish(error)
        return
      }

      this.#consumer[kGetApi]<ShareAcknowledgeRequestArguments, ShareAcknowledgeResponse>('ShareAcknowledge', (
        error,
        api
      ) => {
        if (error) {
          finish(error)
          return
        }

        // A session epoch of -1 closes the session after the acknowledgements
        const request: ShareAcknowledgeRequestArguments = [this.#consumer.groupId, this.#consumer.memberId, -1]
        if (api!.version >= 2) {
          request.push(false)
        }

        api!(connection!, ...request, Array.from(topics.values()), finish)
      })
    })
  }

  #acknowledgementFailed (partition: SharePartition, errorCode: number, errorMessage: NullableString): void {
    this.#consumer.emitWithDebug('share-consumer', 'acknowledgement:error', {
      groupId: this.#consumer.groupId,
      memberId: this.#consumer.memberId,
      memberEpoch: this.#consumer.memberEpoch,
      topic: partition.topic,
      partition: partition.partition,
      error: new ProtocolError(errorCode, errorMessage)
    })
  }

  #createSession (nodeId: number, broker: Broker): ShareSession {
    return {
      nodeId,
      broker,
      epoch: 0,
      inflight: false,
      partitions: new Map(),
      acknowledgements: new Map(),
      failures: 0,
      retryTimeout: null
    }
  }
}

// Adjacent offsets with the same type are acknowledged with a single batch
function toAcknowledgementBatches (ranges: AcknowledgementRange[]): ShareFetchRequestAcknowledgementBatch[] {
  ranges.sort((a, b) => (a.firstOffset < b.firstOffset ? -1 : 1))

  const batches: ShareFetchRequestAcknowledgementBatch[] = []
  let last: ShareFetchRequestAcknowledgementBatch | undefined

  for (const { firstOffset, lastOffset, type } of ranges) {
    if (last && last.acknowledgeTypes[0] === type && last.lastOffset + 1n === firstOffset) {
      last.lastOffset = lastOffset
      continue
    }

    last = { firstOffset, lastOffset, acknowledgeTypes: [type] }
    batches.push(last)
  }

  return batches
}
