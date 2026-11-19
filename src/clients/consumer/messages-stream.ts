import { Readable } from 'node:stream'
import {
  createPromisifiedCallback,
  kCallbackPromise,
  noopCallback,
  type CallbackWithPromise
} from '../../apis/callbacks.ts'
import type { FetchRequestTopic, FetchResponse } from '../../apis/consumer/fetch-v17.ts'
import type { Callback } from '../../apis/definitions.ts'
import { ListOffsetTimestamps } from '../../apis/enumerations.ts'
import {
  consumerReceivesChannel,
  createDiagnosticContext,
  notifyCreation,
  type DiagnosticContext
} from '../../diagnostic.ts'
import type { GenericError } from '../../errors.ts'
import { findErrorBy, protocolErrors, UserError } from '../../errors.ts'
import type { ConnectionPool, ConnectionPoolEventPayload } from '../../network/connection-pool.ts'
import { IS_CONTROL, type Message, type MessageJSON, type MessageToConsume } from '../../protocol/records.ts'
import { runAsyncSeries } from '../../registries/abstract.ts'
import {
  kAutocommit,
  kDeserializationError,
  kGetFetchNode,
  kInstance,
  kRefreshOffsetsAndFetch,
  kUpdateCommittedOffset
} from '../../symbols.ts'
import { kConnections, kCreateConnectionPool, kInspect, kPrometheus } from '../base/base.ts'
import type { ClusterMetadata } from '../base/types.ts'
import { ensureMetric, type Counter } from '../metrics.ts'
import {
  jsonDeserializer,
  stringDeserializer,
  type BeforeDeserializationHook,
  type BeforeHookPayloadType,
  type Deserializer,
  type DeserializerWithHeaders
} from '../serde.ts'
import type { Consumer } from './consumer.ts'
import { defaultBatchSize, defaultConsumerOptions } from './options.ts'
import {
  DeserializationErrorActions,
  MessagesStreamFallbackModes,
  MessagesStreamModes,
  type CommitOptionsPartition,
  type ConsumeOptions,
  type CorruptedMessageHandler,
  type DeserializationErrorContext,
  type DeserializationErrorHandler,
  type GroupAssignment,
  type MessageDeserializationError,
  type MessagesBatch,
  type MessagesBatchJSON,
  type Offsets
} from './types.ts'
import { partitionKey } from './utils.ts'

// Don't move this function as being in the same file will enable V8 to remove.
// For futher info, ask Matteo.
/* c8 ignore next 3 - Fallback deserializer, nothing to really test */
export function noopDeserializer (data?: Buffer): Buffer | undefined {
  return data
}

// The deserializers of this package which do not read the headers of the message they are given
const dataOnlyDeserializers = new Set<unknown>([noopDeserializer, stringDeserializer, jsonDeserializer])

export function defaultCorruptedMessageHandler (): boolean {
  return true
}

// A message whose before-deserialization hook failed, tracked until #pushRecords can report it
type MessageToConsumeWithDeserializationError = MessageToConsume & {
  [kDeserializationError]?: MessageDeserializationError
}

/*
  The messages which are pushed to the stream.

  Most records have no headers, and a Map is by far the most expensive thing a message allocates. So the
  Map of a message without headers is only created if somebody asks for it.
*/
class ConsumedMessage<Key, Value, HeaderKey, HeaderValue> implements Message<Key, Value, HeaderKey, HeaderValue> {
  #headers: Map<HeaderKey, HeaderValue> | undefined
  key: Key
  value: Value
  headerEntries: Array<[HeaderKey, HeaderValue]>
  topic: string
  partition: number
  timestamp: bigint
  offset: bigint
  leaderEpoch: number
  commit: Message['commit']
  metadata: Record<string, unknown>

  constructor (
    key: Key,
    value: Value,
    headers: Map<HeaderKey, HeaderValue> | undefined,
    headerEntries: Array<[HeaderKey, HeaderValue]>,
    topic: string,
    partition: number,
    timestamp: bigint,
    offset: bigint,
    leaderEpoch: number,
    commit: Message['commit'],
    metadata: Record<string, unknown>
  ) {
    this.#headers = headers
    this.key = key
    this.value = value
    this.headerEntries = headerEntries
    this.topic = topic
    this.partition = partition
    this.timestamp = timestamp
    this.offset = offset
    this.leaderEpoch = leaderEpoch
    this.commit = commit
    this.metadata = metadata
  }

  get headers (): Map<HeaderKey, HeaderValue> {
    return (this.#headers ??= new Map())
  }

  set headers (headers: Map<HeaderKey, HeaderValue>) {
    this.#headers = headers
  }

  toJSON (): MessageJSON<Key, Value, HeaderKey, HeaderValue> {
    return {
      key: this.key,
      value: this.value,
      headers: this.headerEntries.map(([key, value]): [HeaderKey, HeaderValue] => [key, value]),
      topic: this.topic,
      partition: this.partition,
      timestamp: this.timestamp.toString(),
      offset: this.offset.toString(),
      leaderEpoch: this.leaderEpoch,
      metadata: this.metadata
    }
  }
}

function messagesBatchToJSON<Key, Value, HeaderKey, HeaderValue> (
  this: MessagesBatch<Key, Value, HeaderKey, HeaderValue>
): MessagesBatchJSON<Key, Value, HeaderKey, HeaderValue> {
  return {
    topic: this.topic,
    partition: this.partition,
    messages: this.messages.map(message => message.toJSON()),
    firstOffset: this.firstOffset.toString(),
    lastOffset: this.lastOffset.toString(),
    leaderEpoch: this.leaderEpoch
  }
}

let currentInstance = 0

export class MessagesStream<
  Key,
  Value,
  HeaderKey,
  HeaderValue,
  Item = Message<Key, Value, HeaderKey, HeaderValue>
> extends Readable {
  #consumer: Consumer<Key, Value, HeaderKey, HeaderValue>
  #mode: string
  #fallbackMode: string
  #paused: boolean
  #refreshOffsetsInflight: boolean
  #refreshOffsetsPending: boolean
  #refreshOffsetsDestroyOnError: boolean
  #offsetsEpoch: number
  #fetches: number
  #maxFetches: number
  #batchSize: number
  #options: ConsumeOptions<Key, Value, HeaderKey, HeaderValue>
  #topics: string[]
  #offsetsToFetch: Map<string, bigint>
  #offsetsToCommit: Map<string, CommitOptionsPartition>
  #offsetsCommitted: Map<string, bigint>
  #commitWaiters: Map<string, Array<{ offset: bigint; callback: Callback<void> }>>
  #partitionsEpochs: Map<string, number>
  #inflightNodes: Map<number, { startedAt: number }>
  #keyDeserializer: DeserializerWithHeaders<Key, HeaderKey, HeaderValue>
  #valueDeserializer: DeserializerWithHeaders<Value, HeaderKey, HeaderValue>
  #headerKeyDeserializer: Deserializer<HeaderKey>
  #headerValueDeserializer: Deserializer<HeaderValue>
  #deserializersUseHeaders: boolean
  #autocommitEnabled: boolean
  #autocommitInterval: NodeJS.Timeout | null
  #autocommitInflight: boolean
  #closed: boolean
  #closeCallbacks: Callback<void>[]
  #metricsConsumedMessages: Counter | undefined
  #corruptedMessageHandler: CorruptedMessageHandler
  #deserializationErrorHandler: DeserializationErrorHandler | undefined
  #context: unknown
  #onConsumerGroupJoin: () => void
  #onBrokerDisconnect: (payload: ConnectionPoolEventPayload & { reason?: 'idle' }) => void
  #pushRecordsOperation: (
    metadata: ClusterMetadata,
    topicIds: Map<string, string>,
    response: FetchResponse,
    requestedOffsets: Map<string, bigint>,
    offsetsEpoch: number
  ) => void;

  [kInstance]: number;

  /*
    The following requests are blocking in Kafka:

    FetchRequest (soprattutto con maxWaitMs)
    JoinGroupRequest
    SyncGroupRequest
    OffsetCommitRequest
    ProduceRequest
    ListOffsetsRequest
    ListGroupsRequest
    DescribeGroupsRequest

    In order to avoid consumer group problems, we separate FetchRequest only on a separate connection.
    Also, to avoid head-of-line blocking between streams, we use a pool of connections for each stream.
  */
  [kConnections]: ConnectionPool

  constructor (
    consumer: Consumer<Key, Value, HeaderKey, HeaderValue>,
    options: ConsumeOptions<Key, Value, HeaderKey, HeaderValue>
  ) {
    const {
      autocommit,
      mode,
      fallbackMode,
      maxFetches,
      batch,
      offsets,
      context,
      deserializers,
      onCorruptedMessage,
      onDeserializationError,
      registry,
      beforeDeserialization,
      // The options below are only destructured to avoid being part of structuredClone below
      partitionAssigner: _partitionAssigner,
      partitionAssignerTopicsSelector: _partitionAssignerTopicsSelector,
      protocolsMetadata: _protocolsMetadata,
      ...otherOptions
    } = options

    if (onCorruptedMessage && onDeserializationError) {
      throw new UserError('Cannot specify both onCorruptedMessage and onDeserializationError.')
    }

    if (offsets && mode !== MessagesStreamModes.MANUAL) {
      throw new UserError('Cannot specify offsets when the stream mode is not MANUAL.')
    }

    if (!offsets && mode === MessagesStreamModes.MANUAL) {
      throw new UserError('Must specify offsets when the stream mode is MANUAL.')
    }

    const batchSize = batch === true ? defaultBatchSize : batch || 0
    /* c8 ignore next - Unless is initialized directly, highWaterMark is always defined */
    const messagesHighWaterMark = options.highWaterMark ?? defaultConsumerOptions.highWaterMark
    // maxFetches also sets the buffer threshold, measured in batches in batch mode.
    const highWaterMark =
      maxFetches ?? (batchSize ? Math.ceil(messagesHighWaterMark / batchSize) : messagesHighWaterMark)

    super({ objectMode: true, highWaterMark })
    this[kInstance] = currentInstance++
    this[kConnections] = consumer[kCreateConnectionPool](context)
    this.#consumer = consumer
    this.#mode = mode ?? MessagesStreamModes.LATEST
    this.#fallbackMode = fallbackMode ?? MessagesStreamFallbackModes.LATEST
    this.#offsetsToCommit = new Map()
    this.#offsetsCommitted = new Map()
    this.#commitWaiters = new Map()
    this.#partitionsEpochs = new Map()
    this.#paused = false
    this.#refreshOffsetsInflight = false
    this.#refreshOffsetsPending = false
    this.#refreshOffsetsDestroyOnError = false
    this.#offsetsEpoch = 0
    this.#fetches = 0
    this.#maxFetches = maxFetches ?? 0
    this.#batchSize = batchSize
    this.#topics = structuredClone(options.topics)
    this.#inflightNodes = new Map()
    this.#keyDeserializer =
      deserializers?.key ?? (noopDeserializer as DeserializerWithHeaders<Key, HeaderKey, HeaderValue>)
    this.#valueDeserializer =
      deserializers?.value ?? (noopDeserializer as DeserializerWithHeaders<Value, HeaderKey, HeaderValue>)
    this.#headerKeyDeserializer = deserializers?.headerKey ?? (noopDeserializer as Deserializer<HeaderKey>)
    this.#headerValueDeserializer = deserializers?.headerValue ?? (noopDeserializer as Deserializer<HeaderValue>)
    this.#deserializersUseHeaders =
      !dataOnlyDeserializers.has(this.#keyDeserializer) || !dataOnlyDeserializers.has(this.#valueDeserializer)
    this.#autocommitEnabled = !!options.autocommit
    this.#autocommitInflight = false
    this.#closed = false
    this.#closeCallbacks = []
    this.#corruptedMessageHandler = onCorruptedMessage ?? defaultCorruptedMessageHandler
    this.#deserializationErrorHandler = onDeserializationError
    this.#context = context

    this.#onConsumerGroupJoin = () => {
      this.#offsetsCommitted.clear()
      this.#partitionsEpochs.clear()
      this.#scheduleRefreshOffsetsAndFetch()

      // [kAutocommit] skips flushing while mid-rejoin to avoid triggering a rejoin storm (see
      // below), leaving any queued offset waiting for the next tick of the autocommit timer.
      // With autocommit disabled there is no such timer, so a manual commit requested during a
      // rebalance would otherwise never resolve. Flush now that the rejoin has completed.
      if (this.#offsetsToCommit.size > 0) {
        this[kAutocommit]()
      }
    }

    this.#onBrokerDisconnect = payload => {
      // Closing an unused connection says nothing about partition leadership
      if (payload.reason === 'idle') {
        return
      }

      this.#partitionsEpochs.clear()
    }

    if (registry) {
      this.#pushRecordsOperation = this.#beforeDeserialization.bind(this, registry.getBeforeDeserializationHook())
    } else if (beforeDeserialization) {
      this.#pushRecordsOperation = this.#beforeDeserialization.bind(this, beforeDeserialization)
    } else {
      this.#pushRecordsOperation = this.#pushRecords.bind(this)
    }

    // Restore offsets
    this.#offsetsToFetch = new Map()
    if (offsets) {
      for (const { topic, partition, offset } of offsets) {
        this.#offsetsToFetch.set(partitionKey(topic, partition), offset)
      }
    }

    // Clone the rest of the options so the user can never mutate them
    this.#options = structuredClone(otherOptions)

    // Start the autocommit interval
    if (typeof autocommit === 'number' && autocommit > 0) {
      // Invoke without a callback so the timer path keeps the mid-rejoin skip in [kAutocommit].
      this.#autocommitInterval = setInterval(() => this[kAutocommit](), autocommit as number)
    } else {
      this.#autocommitInterval = null
    }

    // When the consumer joins a group, we need to fetch again as the assignments
    // will have changed so we may have gone from last  with no assignments to
    // having some.
    this.#consumer.on('consumer:group:join', this.#onConsumerGroupJoin)

    if (consumer[kPrometheus]) {
      this.#metricsConsumedMessages = ensureMetric<Counter>(
        consumer[kPrometheus],
        'Counter',
        'kafka_consumed_messages',
        'Number of consumed Kafka messages'
      )
    }

    // Whenever the consumer loses a connection, reset all the partitions epochs
    consumer.on('client:broker:disconnect', this.#onBrokerDisconnect)

    notifyCreation('messages-stream', this)
  }

  /* c8 ignore next 3 - Simple getter */
  get consumer (): Consumer<Key, Value, HeaderKey, HeaderValue> {
    return this.#consumer
  }

  /* c8 ignore next 3 - Simple getter */
  get offsetsToFetch (): Map<string, bigint> {
    return this.#offsetsToFetch
  }

  get context (): unknown {
    return this.#context
  }

  /* c8 ignore next 3 - Simple getter */
  get offsetsToCommit (): Map<string, CommitOptionsPartition> {
    return this.#offsetsToCommit
  }

  /* c8 ignore next 3 - Simple getter */
  get offsetsCommitted (): Map<string, bigint> {
    return this.#offsetsCommitted
  }

  /* c8 ignore next 3 - Simple getter */
  get connections (): ConnectionPool {
    return this[kConnections]
  }

  close (callback: CallbackWithPromise<void>): void
  close (): Promise<void>
  close (callback?: CallbackWithPromise<void>): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback<void>()
    }

    if (this.closed || this.destroyed) {
      callback(null)
      return callback[kCallbackPromise]
    }

    this.#closeCallbacks.push(callback)

    if (this.#closed) {
      this.#afterClose(null)
      return callback[kCallbackPromise]
    }

    this.#closed = true
    this.push(null)

    if (this.#autocommitInterval) {
      clearInterval(this.#autocommitInterval)
    }

    if (this.readableFlowing === null || this.isPaused()) {
      this.removeAllListeners('data')
      this.removeAllListeners('readable')
      this.resume()
    }

    /* c8 ignore next 3 - Hard to test */
    this.once('error', error => {
      this.#afterClose(error)
    })

    this.once('close', () => {
      // We have offsets that were enqueued to be committed. Perform the operation
      /* c8 ignore next 3 - Hard to test */
      if (this.#offsetsToCommit.size > 0) {
        this[kAutocommit]()
      }

      // We have offsets that are being committed. These are awaited despite of the force parameters
      if (this.#autocommitInflight) {
        this.once('autocommit', error => {
          this.#afterClose(error)
        })

        return
      }

      this.#afterClose(null)
    })

    return callback[kCallbackPromise]
  }

  isActive (): boolean {
    if (this.#closed || this.closed || this.destroyed) {
      return false
    }

    return this.#consumer.isActive()
  }

  isConnected (): boolean {
    if (this.#closed || this.closed || this.destroyed) {
      return false
    }

    return this.#consumer.isConnected()
  }

  resume () {
    const wasPaused = this.#paused
    this.#paused = false
    const result = super.resume()

    // Restart the fetch loop when transitioning from paused → unpaused.
    //
    // When a downstream Duplex (e.g. a batching stream) signals backpressure,
    // pipeline()/pipe() calls pause() on this stream, setting #paused = true.
    // If a previously scheduled process.nextTick(#fetch) fires while #paused is
    // true, #fetch() returns early without scheduling another iteration — the
    // loop is now dead. Later, pipe() calls resume() when the downstream drains,
    // but super.resume() does not reliably trigger _read() when the readable
    // buffer is already empty and the stream is in flowing mode (Node.js
    // considers it "already flowing" and skips the _read() → #fetch() path).
    //
    // The wasPaused guard prevents a premature fetch during initial pipeline()
    // setup, where resume() is called before _construct() completes.
    if (wasPaused) {
      process.nextTick(() => {
        this.#fetch()
      })
    }

    return result
  }

  // We want to track if the stream is paused explicitly by the user, while isPaused from Node.js can also
  // be true if the stream is paused because there is no consumer.
  pause () {
    this.#paused = true
    return super.pause()
  }

  /*
    TypeScript support - Extracted from node @types/node/stream.d.ts

    * Event emitter
    * The defined events on documents including:
    *
    * Readable:
    *   1. close
    *   2. data
    *   3. end
    *   4. error
    *   5. pause
    *   6. readable
    *   7. resume
    * Custom:
    *   8. autocommit
    *   9. fetch
  */
  addListener (event: 'autocommit', listener: (err: Error, offsets: CommitOptionsPartition[]) => void): this
  addListener (event: 'fetch', listener: () => void): this
  addListener (event: 'offsets', listener: () => void): this
  addListener (event: 'data', listener: (item: Item) => void): this
  addListener (event: 'close', listener: () => void): this
  addListener (event: 'end', listener: () => void): this
  addListener (event: 'error', listener: (err: Error) => void): this
  addListener (event: 'pause', listener: () => void): this
  addListener (event: 'readable', listener: () => void): this
  addListener (event: 'resume', listener: () => void): this
  /* c8 ignore next 3 - Only forwards to Node.js implementation - Inserted here to please Typescript */
  addListener (event: string | symbol, listener: (...args: any[]) => void): this {
    return super.addListener(event, listener)
  }

  on (event: 'autocommit', listener: (err: Error, offsets: CommitOptionsPartition[]) => void): this
  on (event: 'fetch', listener: () => void): this
  on (event: 'offsets', listener: () => void): this
  on (event: 'data', listener: (item: Item) => void): this
  on (event: 'close', listener: () => void): this
  on (event: 'end', listener: () => void): this
  on (event: 'error', listener: (err: Error) => void): this
  on (event: 'pause', listener: () => void): this
  on (event: 'readable', listener: () => void): this
  on (event: 'resume', listener: () => void): this
  /* c8 ignore next 3 - Only forwards to Node.js implementation - Inserted here to please Typescript */
  on (event: string | symbol, listener: (...args: any[]) => void): this {
    return super.on(event, listener)
  }

  once (event: 'autocommit', listener: (err: Error, offsets: CommitOptionsPartition[]) => void): this
  once (event: 'fetch', listener: () => void): this
  once (event: 'offsets', listener: () => void): this
  once (event: 'data', listener: (item: Item) => void): this
  once (event: 'close', listener: () => void): this
  once (event: 'end', listener: () => void): this
  once (event: 'error', listener: (err: Error) => void): this
  once (event: 'pause', listener: () => void): this
  once (event: 'readable', listener: () => void): this
  once (event: 'resume', listener: () => void): this
  /* c8 ignore next 3 - Only forwards to Node.js implementation - Inserted here to please Typescript */
  once (event: string | symbol, listener: (...args: any[]) => void): this {
    return super.once(event, listener)
  }

  prependListener (event: 'autocommit', listener: (err: Error, offsets: CommitOptionsPartition[]) => void): this
  prependListener (event: 'fetch', listener: () => void): this
  prependListener (event: 'offsets', listener: () => void): this
  prependListener (event: 'data', listener: (item: Item) => void): this
  prependListener (event: 'close', listener: () => void): this
  prependListener (event: 'end', listener: () => void): this
  prependListener (event: 'error', listener: (err: Error) => void): this
  prependListener (event: 'pause', listener: () => void): this
  prependListener (event: 'readable', listener: () => void): this
  prependListener (event: 'resume', listener: () => void): this
  /* c8 ignore next 3 - Only forwards to Node.js implementation - Inserted here to please Typescript */
  prependListener (event: string | symbol, listener: (...args: any[]) => void): this {
    return super.prependListener(event, listener)
  }

  prependOnceListener (event: 'autocommit', listener: (err: Error, offsets: CommitOptionsPartition[]) => void): this
  prependOnceListener (event: 'fetch', listener: () => void): this
  prependOnceListener (event: 'offsets', listener: () => void): this
  prependOnceListener (event: 'data', listener: (item: Item) => void): this
  prependOnceListener (event: 'close', listener: () => void): this
  prependOnceListener (event: 'end', listener: () => void): this
  prependOnceListener (event: 'error', listener: (err: Error) => void): this
  prependOnceListener (event: 'pause', listener: () => void): this
  prependOnceListener (event: 'readable', listener: () => void): this
  prependOnceListener (event: 'resume', listener: () => void): this
  /* c8 ignore next 3 - Only forwards to Node.js implementation - Inserted here to please Typescript */
  prependOnceListener (event: string | symbol, listener: (...args: any[]) => void): this {
    return super.prependOnceListener(event, listener)
  }

  [Symbol.asyncIterator] (): NodeJS.AsyncIterator<Item> {
    return super[Symbol.asyncIterator]()
  }

  _construct (callback: (error?: Error) => void) {
    this.#refreshOffsetsInflight = true
    this.#refreshOffsets(error => {
      const wasPending = this.#refreshOffsetsPending
      this.#refreshOffsetsInflight = false
      this.#refreshOffsetsPending = false

      if (!error && wasPending) {
        this.#scheduleRefreshOffsetsAndFetch()
      }

      callback(error ?? undefined)
    })
  }

  _destroy (error: Error | null, callback: (error?: Error | null) => void): void {
    if (this.#autocommitInterval) {
      clearInterval(this.#autocommitInterval)
    }

    this.#consumer.removeListener('consumer:group:join', this.#onConsumerGroupJoin)
    this.#consumer.removeListener('client:broker:disconnect', this.#onBrokerDisconnect)

    this[kConnections].close(closeError => {
      callback(closeError ?? error)
    })
  }

  _read () {
    this.#fetch()
  }

  #maxFetchesReached (): boolean {
    return this.#maxFetches > 0 && this.#fetches >= this.#maxFetches
  }

  #fetch () {
    if (this.#maxFetchesReached()) {
      return
    }

    /* c8 ignore next 4 - Hard to test */
    if (this.#closed || this.closed || this.destroyed) {
      this.push(null)
      return
    }

    // No need to fetch if nobody is consuming the data
    if (this.readableFlowing === null || this.#paused || this.#refreshOffsetsInflight || this.#refreshOffsetsPending) {
      return
    }

    const offsetsEpoch = this.#offsetsEpoch
    this.#consumer.metadata({ topics: this.#consumer.topics.current }, (error, metadata) => {
      if (offsetsEpoch !== this.#offsetsEpoch || this.#maxFetchesReached()) {
        return
      }

      if (error) {
        this.emit('fetch')

        // The stream has been closed, ignore any error
        /* c8 ignore next 4 - Hard to test */
        if (this.#closed || this.closed || this.destroyed) {
          this.push(null)
          return
        }

        this.destroy(error)
        return
      }

      /* c8 ignore next 5 - Hard to test */
      if (this.#closed || this.closed || this.destroyed) {
        this.emit('fetch')
        this.push(null)
        return
      }

      // Remove stale inflight entries that have been pending for too long.
      // This prevents permanent partition starvation if a fetch callback is never invoked.
      const now = Date.now()
      for (const [node, inflight] of this.#inflightNodes) {
        if (now - inflight.startedAt > 120_000) {
          this.#inflightNodes.delete(node)
        }
      }

      const requests = new Map<number, FetchRequestTopic[]>()
      const topicIds = new Map<string, string>()

      // Group topic-partitions by the destination broker
      const requestedOffsets = new Map<string, bigint>()
      for (const topic of this.#topics) {
        const assignment = this.#assignmentsForTopic(topic)

        // This consumer has no assignment for the topic, continue
        if (!assignment) {
          continue
        }

        const partitions = assignment.partitions

        for (const partition of partitions) {
          const targetNode = this.#consumer[kGetFetchNode](metadata!, topic, partition, now)
          const key = partitionKey(topic, partition)

          if (this.#inflightNodes.has(targetNode)) {
            continue
          }

          let nodeRequests = requests.get(targetNode)
          if (!nodeRequests) {
            nodeRequests = []
            requests.set(targetNode, nodeRequests)
          }

          const topicId = metadata!.topics.get(topic)!.id
          topicIds.set(topicId, topic)

          const fetchOffset = this.#offsetsToFetch.get(key)!
          requestedOffsets.set(key, fetchOffset)

          const leaderEpoch = this.#partitionsEpochs.get(key) ?? -1
          nodeRequests.push({
            topicId,
            partitions: [
              {
                partition,
                fetchOffset,
                partitionMaxBytes: this.#options.maxBytesPerPartition!,
                currentLeaderEpoch: leaderEpoch,
                lastFetchedEpoch: leaderEpoch
              }
            ]
          })
        }
      }

      if (requests.size === 0) {
        return
      }

      for (const [node, nodeRequests] of requests) {
        if (this.#maxFetchesReached()) {
          break
        }

        const inflight = { startedAt: Date.now() }
        this.#inflightNodes.set(node, inflight)
        this.#consumer.fetch({ ...this.#options, node, topics: nodeRequests, connectionPool: this[kConnections] }, (
          error,
          response
        ) => {
          const ownsNode = this.#inflightNodes.get(node) === inflight
          if (ownsNode) {
            this.#inflightNodes.delete(node)
          }
          this.emit('fetch')

          if (this.#maxFetchesReached()) {
            return
          }

          // A refresh (including a rejoin) invalidates requests issued before it.
          // Never let an old response or error undo the restored offsets.
          if (offsetsEpoch !== this.#offsetsEpoch || !ownsNode) {
            if (this.#closed || this.closed || this.destroyed) {
              if (this.#inflightNodes.size === 0) {
                this.push(null)
              }
            } else if (ownsNode) {
              // The refresh may have finished while this node was still inflight.
              // Now that it is free, resume fetching from the restored position.
              process.nextTick(() => this.#fetch())
            }
            return
          }

          if (error) {
            // The stream has been closed, ignore the error
            /* c8 ignore next 4 - Hard to test */
            if (this.#closed || this.closed || this.destroyed) {
              this.push(null)
              return
            }

            if (this.#fallbackMode !== MessagesStreamFallbackModes.FAIL) {
              this.#handleOffsetOutOfRange(error as GenericError, topicIds, (recoveryError, recovered) => {
                if (this.#closed || this.closed || this.destroyed || this.#maxFetchesReached()) {
                  return
                }

                if (recoveryError) {
                  this.destroy(recoveryError)
                  return
                }

                if (recovered) {
                  process.nextTick(() => {
                    this.#fetch()
                  })
                  return
                }

                this.destroy(error)
              })
              return
            }

            this.destroy(error)
            return
          }

          if (this.#closed || this.closed || this.destroyed) {
            // When it's the last inflight, we finally close the stream.
            // This is done to avoid the user exiting from consmuming metrics like for-await and still see the process up.
            if (this.#inflightNodes.size === 0) {
              this.push(null)
            }

            return
          }

          // Consumer.fetch resets request epochs during recovery. Invalidate the stream cache
          // too, even when the successful retry returns no records to update it.
          for (const topic of nodeRequests) {
            for (const partition of topic.partitions) {
              if (partition.currentLeaderEpoch === -1) {
                this.#partitionsEpochs.delete(partitionKey(topicIds.get(topic.topicId)!, partition.partition))
              }
            }
          }

          this.#pushRecordsOperation(metadata!, topicIds, response!, requestedOffsets, offsetsEpoch)
        })
      }
    })
  }

  #handleOffsetOutOfRange (error: GenericError, topicIds: Map<string, string>, callback: Callback<boolean>): void {
    if (!findErrorBy(error, 'apiId', 'OFFSET_OUT_OF_RANGE')) {
      callback(null, false)
      return
    }

    const response = error.response as FetchResponse | undefined
    if (!response || response.errorCode !== 0) {
      callback(null, false)
      return
    }

    const recoveredOffsets: [string, bigint][] = []
    const partitionsToRefresh = new Map<string, number[]>()

    for (const topicResponse of response.responses) {
      const topic = topicIds.get(topicResponse.topicId)
      if (!topic) {
        callback(null, false)
        return
      }

      for (const partitionResponse of topicResponse.partitions) {
        if (partitionResponse.errorCode === 0) {
          continue
        }

        if (partitionResponse.errorCode !== protocolErrors.OFFSET_OUT_OF_RANGE.code) {
          callback(null, false)
          return
        }

        const offset =
          this.#fallbackMode === MessagesStreamFallbackModes.EARLIEST
            ? partitionResponse.logStartOffset
            : partitionResponse.highWatermark

        if (offset >= 0n) {
          recoveredOffsets.push([partitionKey(topic, partitionResponse.partitionIndex), offset])
          continue
        }

        let partitions = partitionsToRefresh.get(topic)
        if (!partitions) {
          partitions = []
          partitionsToRefresh.set(topic, partitions)
        }
        partitions.push(partitionResponse.partitionIndex)
      }
    }

    if (partitionsToRefresh.size > 0) {
      this.#refreshOutOfRangeOffsets(partitionsToRefresh, recoveredOffsets, callback)
      return
    }

    callback(null, this.#applyRecoveredOffsets(recoveredOffsets))
  }

  #refreshOutOfRangeOffsets (
    partitionsToRefresh: Map<string, number[]>,
    recoveredOffsets: [string, bigint][],
    callback: Callback<boolean>
  ): void {
    const partitions = Object.fromEntries(partitionsToRefresh)

    this.#consumer.listOffsets(
      {
        topics: Array.from(partitionsToRefresh.keys()),
        partitions,
        timestamp:
          this.#fallbackMode === MessagesStreamFallbackModes.EARLIEST
            ? ListOffsetTimestamps.EARLIEST
            : ListOffsetTimestamps.LATEST
      },
      (error, offsets) => {
        if (error) {
          callback(error)
          return
        }

        for (const [topic, refreshedPartitions] of partitionsToRefresh) {
          const topicOffsets = offsets!.get(topic)

          for (const partition of refreshedPartitions) {
            const offset = topicOffsets?.[partition]

            if (typeof offset !== 'bigint' || offset < 0n) {
              callback(new UserError(`Cannot recover offset out of range for topic ${topic} partition ${partition}.`))
              return
            }

            recoveredOffsets.push([partitionKey(topic, partition), offset])
          }
        }

        callback(null, this.#applyRecoveredOffsets(recoveredOffsets))
      }
    )
  }

  #applyRecoveredOffsets (recoveredOffsets: [string, bigint][]): boolean {
    for (const [key, offset] of recoveredOffsets) {
      this.#offsetsToFetch.set(key, offset)
      this.#offsetsCommitted.set(key, offset)
    }

    if (recoveredOffsets.length > 0) {
      this.emit('offsets')
    }

    return recoveredOffsets.length > 0
  }

  #pushRecords (
    metadata: ClusterMetadata,
    topicIds: Map<string, string>,
    response: FetchResponse,
    requestedOffsets: Map<string, bigint>,
    offsetsEpoch: number
  ) {
    if (offsetsEpoch !== this.#offsetsEpoch || this.#maxFetchesReached()) {
      return
    }

    const autocommit = this.#autocommitEnabled
    const batchSize = this.#batchSize
    const keyDeserializer = this.#keyDeserializer
    const valueDeserializer = this.#valueDeserializer
    const headerKeyDeserializer = this.#headerKeyDeserializer
    const headerValueDeserializer = this.#headerValueDeserializer
    const deserializersUseHeaders = this.#deserializersUseHeaders

    /*
      Publishing on the diagnostic channel requires a context object and five publications for each message.
      That is only worth paying for when somebody is listening.
    */
    const tracing = consumerReceivesChannel.hasSubscribers
    let diagnosticContext: DiagnosticContext<unknown> | undefined

    let canPush = true

    const messageMetadata = {
      consumer: {
        groupId: this.#consumer.groupId,
        memberId: this.#consumer.memberId,
        generationId: this.#consumer.generationId,
        coordinatorId: this.#consumer.coordinatorId
      }
    }

    // Parse results
    for (const topicResponse of response.responses) {
      const topic = topicIds.get(topicResponse.topicId)!

      for (const { records: recordsBatches, partitionIndex: partition } of topicResponse.partitions) {
        const key = partitionKey(topic, partition)

        // The broker can return partitions which were not requested when they linger
        // in an incremental fetch session - their data is stale and must not rewind offsets
        if (!requestedOffsets.has(key)) {
          continue
        }

        if (!recordsBatches) {
          continue
        }

        let pending: Message<Key, Value, HeaderKey, HeaderValue>[] | undefined
        let pendingContexts: DiagnosticContext<unknown>[] | undefined = batchSize && tracing ? [] : undefined

        for (const batch of recordsBatches) {
          const firstTimestamp = batch.firstTimestamp
          const firstOffset = batch.firstOffset
          const leaderEpoch = metadata.topics.get(topic)!.partitions[partition].leaderEpoch

          this.#partitionsEpochs.set(key, leaderEpoch)

          // Track offsets
          if (batch === recordsBatches[recordsBatches.length - 1]) {
            // Track the last read offset
            const lastOffset = batch.firstOffset + BigInt(batch.lastOffsetDelta)
            this.#offsetsToFetch.set(key, lastOffset + 1n)

            // Autocommit if needed
            if (autocommit && !batchSize) {
              this.#offsetsToCommit.set(key, {
                topic,
                partition,
                offset: lastOffset + 1n,
                leaderEpoch
              })
            }
          }

          // Filter control markers
          if (batch.attributes & IS_CONTROL) {
            continue
          }

          // Process messages
          for (const record of batch.records) {
            /*
              The record was just decoded and is not shared with anything else, so it is extended in place rather
              than copied. The before-deserialization hook path already does the same.
            */
            const messageToConsume = record as MessageToConsumeWithDeserializationError
            messageToConsume.topic = topic
            messageToConsume.partition = partition

            const offset = batch.firstOffset + BigInt(record.offsetDelta)

            if (offset < requestedOffsets.get(key)!) {
              // Thi is a duplicate message, ignore it
              continue
            }

            if (tracing) {
              diagnosticContext = createDiagnosticContext({
                client: this.#consumer,
                stream: this,
                operation: 'receive',
                raw: record
              })

              consumerReceivesChannel.start.publish(diagnosticContext)
            }

            const commit = autocommit || batchSize
              ? noopCallback
              : this.#commit.bind(this, topic, partition, offset + 1n, leaderEpoch)

            try {
              // Without headers, the Map is only needed by the deserializers which read it
              let headers: Map<HeaderKey, HeaderValue> | undefined =
                deserializersUseHeaders || record.headers.length > 0 ? new Map<HeaderKey, HeaderValue>() : undefined
              // Allocated at its final length, as a push on an empty array reserves room for 17 elements
              const recordHeaders = record.headers
              const headersCount = recordHeaders.length
              const headerEntries: Array<[HeaderKey, HeaderValue]> = headersCount > 0 ? new Array(headersCount) : []
              let payloadType: BeforeHookPayloadType = 'headerKey'
              let deserializedKey: Key | undefined
              let deserializedValue: Value | undefined
              let deserializationError: MessageDeserializationError | undefined
              let hookFailed = false

              try {
                // A before-deserialization hook already failed for this message, report it as is
                const hookError = messageToConsume[kDeserializationError]
                if (hookError) {
                  hookFailed = true
                  payloadType = hookError.payloadType
                  throw hookError.error
                }

                for (let i = 0; i < headersCount; i++) {
                  const header = recordHeaders[i]
                  payloadType = 'headerKey'
                  /* c8 ignore next - Hard to test */
                  const deserializedHeaderKey = headerKeyDeserializer(header[0] ?? undefined, messageToConsume)
                  payloadType = 'headerValue'
                  /* c8 ignore next - Hard to test */
                  const deserializedHeaderValue = headerValueDeserializer(header[1] ?? undefined, messageToConsume)
                  headers!.set(deserializedHeaderKey as HeaderKey, deserializedHeaderValue as HeaderValue)
                  headerEntries[i] = [deserializedHeaderKey as HeaderKey, deserializedHeaderValue as HeaderValue]
                }

                payloadType = 'key'
                /* c8 ignore next - Hard to test */
                deserializedKey = keyDeserializer(record.key ?? undefined, headers!, messageToConsume)
                payloadType = 'value'
                deserializedValue = valueDeserializer(record.value ?? undefined, headers!, messageToConsume)
              } catch (error) {
                // The error handler can explicitly commit a failed record even in batch mode.
                const commitFailedMessage = (batchSize && !autocommit
                  ? this.#commit.bind(this, topic, partition, offset + 1n, leaderEpoch)
                  : commit) as Message['commit']
                const context: DeserializationErrorContext = {
                  error,
                  payloadType,
                  record,
                  topic,
                  partition,
                  offset,
                  timestamp: firstTimestamp + record.timestampDelta,
                  commit: commitFailedMessage
                }
                const action = this.#deserializationErrorHandler
                  ? this.#deserializationErrorHandler(context)
                  : this.#corruptedMessageHandler(
                    record,
                    topic,
                    partition,
                    firstTimestamp,
                    firstOffset,
                    commitFailedMessage,
                    error
                  )
                    ? DeserializationErrorActions.FAIL
                    : DeserializationErrorActions.SKIP

                if (action === DeserializationErrorActions.CONTINUE) {
                  if (diagnosticContext) {
                    diagnosticContext.error = error
                    consumerReceivesChannel.error.publish(diagnosticContext)
                  }

                  if (batchSize) {
                    continue
                  }

                  /*
                    In per-message mode, keep the fields that were already deserialized and return
                    the failed or unattempted fields as bytes. Record the error in message metadata.
                  */
                  // A hook failure happens before anything is deserialized, so nothing can be kept
                  const headersFailed = hookFailed || payloadType === 'headerKey' || payloadType === 'headerValue'

                  if (headersFailed) {
                    headers = record.headers.length > 0 ? new Map<HeaderKey, HeaderValue>() : undefined

                    // Every entry is overwritten, including the ones which were never deserialized
                    for (let i = 0; i < headersCount; i++) {
                      const header = recordHeaders[i]
                      headers!.set(header[0] as HeaderKey, header[1] as HeaderValue)
                      headerEntries[i] = [header[0] as HeaderKey, header[1] as HeaderValue]
                    }
                  }

                  if (headersFailed || payloadType === 'key') {
                    deserializedKey = (record.key ?? undefined) as Key | undefined
                  }

                  deserializedValue = (record.value ?? undefined) as Value | undefined
                  deserializationError = { error, payloadType }
                } else if (action === DeserializationErrorActions.SKIP) {
                  continue
                } else {
                  if (diagnosticContext) {
                    diagnosticContext.error = error
                    consumerReceivesChannel.error.publish(diagnosticContext)
                  }

                  this.destroy(
                    hookFailed ? (error as Error) : new UserError('Failed to deserialize a message.', { cause: error })
                  )
                  return
                }
              }

              const message = new ConsumedMessage<Key, Value, HeaderKey, HeaderValue>(
                deserializedKey as Key,
                deserializedValue as Value,
                headers,
                headerEntries,
                topic,
                partition,
                firstTimestamp + record.timestampDelta,
                offset,
                // The batch header epoch is the epoch of the record itself, which is what
                // OffsetCommit expects as committedLeaderEpoch (KIP-320).
                batch.partitionLeaderEpoch,
                commit as Message['commit'],
                // Deserializers and hooks can enrich the metadata of the message they processed
                deserializationError
                  ? { ...messageMetadata, ...messageToConsume.metadata, deserializationError }
                  : messageToConsume.metadata
                    ? { ...messageMetadata, ...messageToConsume.metadata }
                    : messageMetadata
              )

              if (batchSize) {
                if (this.#closed || this.destroyed) {
                  return
                }

                // The next deliverable record is the commit boundary, including any skipped offsets.
                if (pending?.length === batchSize) {
                  canPush = this.#pushBatch(topic, partition, leaderEpoch, pending, pendingContexts, offset)
                  if (this.#closed || this.destroyed) {
                    return
                  }
                  pending = []
                  pendingContexts = tracing ? [] : undefined
                }

                pending ??= []
                pending.push(message)
                if (diagnosticContext) {
                  diagnosticContext.result = message
                  pendingContexts!.push(diagnosticContext)
                }
              } else if (diagnosticContext) {
                this.#metricsConsumedMessages?.inc()
                diagnosticContext.result = message
                consumerReceivesChannel.asyncStart.publish(diagnosticContext)
                canPush = this.push(message)
                consumerReceivesChannel.asyncEnd.publish(diagnosticContext)
              } else {
                this.#metricsConsumedMessages?.inc()
                canPush = this.push(message)
              }
            } catch (error) {
              if (diagnosticContext) {
                diagnosticContext.error = error
                consumerReceivesChannel.error.publish(diagnosticContext)
              }

              this.destroy(new UserError('Failed to process a message.', { cause: error }))
              return
            } finally {
              if (diagnosticContext) {
                consumerReceivesChannel.end.publish(diagnosticContext)
              }
            }
          }
        }

        if (batchSize && recordsBatches.length > 0) {
          const leaderEpoch = this.#partitionsEpochs.get(key)!
          const offset = this.#offsetsToFetch.get(key)!
          if (this.#closed || this.destroyed) {
            return
          }

          if (pending) {
            canPush = this.#pushBatch(topic, partition, leaderEpoch, pending, pendingContexts, offset)
            if (this.#closed || this.destroyed) {
              return
            }
          }

          // Do not autocommit records from a batch that was never pushed.
          if (autocommit) {
            this.#offsetsToCommit.set(key, { topic, partition, offset, leaderEpoch })
          }
        }
      }
    }

    if (this.#autocommitEnabled && !this.#autocommitInterval) {
      this[kAutocommit]()
    }

    if (this.#maxFetches > 0 && ++this.#fetches >= this.#maxFetches) {
      this.push(null)
      return
    }

    // Schedule the next fetch only when the readable buffer still has room.
    // When push() returns false, the buffer is above highWaterMark —
    // continuing to fetch would defeat Node.js backpressure and cause
    // unbounded memory growth (see #260).
    //
    // When canPush is false, the fetch loop restarts via two mechanisms:
    //  1. _read() — called by Node.js when the buffer drains below
    //     highWaterMark in both pull and flowing modes.
    //  2. resume() — when pipeline()/pipe() transitions from paused to
    //     unpaused after downstream backpressure releases, resume()
    //     explicitly schedules process.nextTick(#fetch) (see #254).
    //
    // process.nextTick yields control back to the event loop between fetch
    // cycles, ensuring heartbeats, commits, and other I/O are not starved
    // by a tight fetch loop. It also avoids growing the call stack if
    // metadata/fetch callbacks fire synchronously.
    if (canPush) {
      process.nextTick(() => {
        this.#fetch()
      })
    }
  }

  #pushBatch (
    topic: string,
    partition: number,
    leaderEpoch: number,
    messages: Message<Key, Value, HeaderKey, HeaderValue>[],
    diagnosticContexts: DiagnosticContext<unknown>[] | undefined,
    commitOffset: bigint
  ): boolean {
    const last = messages[messages.length - 1]
    const batch: MessagesBatch<Key, Value, HeaderKey, HeaderValue> = {
      topic,
      partition,
      messages,
      firstOffset: messages[0].offset,
      lastOffset: last.offset,
      leaderEpoch: last.leaderEpoch,
      commit: (this.#autocommitEnabled
        ? noopCallback
        : this.#commit.bind(this, topic, partition, commitOffset, leaderEpoch)) as Message['commit'],
      toJSON: messagesBatchToJSON
    }

    try {
      this.#metricsConsumedMessages?.inc(messages.length)
      if (diagnosticContexts) {
        for (const context of diagnosticContexts) {
          consumerReceivesChannel.asyncStart.publish(context)
        }
      }
      const canPush = this.push(batch)
      if (diagnosticContexts) {
        for (const context of diagnosticContexts) {
          consumerReceivesChannel.asyncEnd.publish(context)
        }
      }
      return canPush
    } catch (error) {
      if (diagnosticContexts) {
        for (const context of diagnosticContexts) {
          context.error = error
          consumerReceivesChannel.error.publish(context)
        }
      }
      this.destroy(new UserError('Failed to process a message.', { cause: error }))
      return false
    }
  }

  [kUpdateCommittedOffset] (topic: string, partition: number, offset: bigint): void {
    const key = partitionKey(topic, partition)
    const previous = this.#offsetsCommitted.get(key)

    if (typeof previous === 'undefined' || previous < offset) {
      this.#offsetsCommitted.set(key, offset)
    }
  }

  // Manual commits share offsetsToCommit/autocommitInflight with kAutocommit instead of
  // calling Consumer#commit directly. A raw one-shot call here has no way to stop two
  // concurrent commits on the same partition from racing: whichever response the broker
  // processes last wins regardless of offset value, so a lower offset can silently overwrite
  // a higher one that already landed. Funnelling through the same single-flight/monotonic-merge
  // machinery as autocommit coalesces same-partition calls into one in-flight request.
  #commit (
    topic: string,
    partition: number,
    offset: bigint,
    leaderEpoch: number,
    callback?: CallbackWithPromise<void>
  ): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback<void>()
    }

    const key = partitionKey(topic, partition)

    const current = this.#offsetsToCommit.get(key)
    if (!current || current.offset < offset) {
      this.#offsetsToCommit.set(key, { topic, partition, offset, leaderEpoch })
    }

    let waiters = this.#commitWaiters.get(key)
    if (!waiters) {
      waiters = []
      this.#commitWaiters.set(key, waiters)
    }
    waiters.push({ offset, callback: callback! })

    this[kAutocommit]()

    return callback[kCallbackPromise]!
  }

  // Settles every waiter whose requested offset is covered by a commit that just landed for
  // its partition (offset is the monotonic max merged from possibly several manual commits, so
  // it covers every waiter queued before this flush's snapshot was taken). A waiter added after
  // the snapshot keeps waiting for the next flush, triggered by the caller of this method.
  #settleCommitWaiters (topic: string, partition: number, offset: bigint, error: Error | null): void {
    const key = partitionKey(topic, partition)
    const waiters = this.#commitWaiters.get(key)
    if (!waiters) {
      return
    }

    const remaining: Array<{ offset: bigint; callback: Callback<void> }> = []
    for (const waiter of waiters) {
      if (waiter.offset <= offset) {
        waiter.callback(error)
      } else {
        remaining.push(waiter)
      }
    }

    if (remaining.length > 0) {
      this.#commitWaiters.set(key, remaining)
    } else {
      this.#commitWaiters.delete(key)
    }
  }

  [kAutocommit] (callback?: Callback<void>): void {
    // A commit is already running: report its outcome to the caller rather than
    // starting a second one, so a cooperative rebalance waits for the in-flight
    // commit instead of proceeding as if nothing was pending.
    if (this.#autocommitInflight) {
      if (callback) {
        // A commit which fails with a rejoin-required error destroys the stream, and a destroyed
        // stream never emits 'autocommit' again. Settle on whichever comes first so an awaiting
        // rebalance cannot deadlock on a stream that is going away.
        const onAutocommit = (error: Error | null): void => {
          this.removeListener('close', onClose)

          // The in-flight commit only carried the offsets queued when it was sent. Flush the ones
          // queued since as well, or the cooperative rebalance revokes them uncommitted.
          if (!error) {
            this[kAutocommit](callback)
            return
          }

          callback(error)
        }
        const onClose = (): void => {
          this.removeListener('autocommit', onAutocommit)
          callback(null)
        }

        this.once('autocommit', onAutocommit)
        this.once('close', onClose)
      }

      // Any offset queued by a manual commit while this flush is in flight is picked up by
      // the follow-up flush triggered right after this one settles (see below).
      return
    }

    if (this.#offsetsToCommit.size === 0) {
      callback?.(null)
      return
    }

    // Skip this tick while the consumer is mid-(re)join: a commit attempted here would fail
    // with a rejoin-required error (ILLEGAL_GENERATION/UNKNOWN_MEMBER_ID/REBALANCE_IN_PROGRESS),
    // and — since this runs on its own timer, independently of the group's own rejoin flow —
    // each such failure calls joinGroup again. With a short autocommit interval and many
    // consumers in the group, this can fire fresh JoinGroups fast enough that the group never
    // completes a rebalance round. Leave the offsets queued; they are committed once the
    // consumer becomes active again (next tick, or the final flush on close/destroy).
    // A caller-supplied callback means an explicitly requested flush (the cooperative rebalance
    // commits before revoking partitions) rather than a timer tick: it awaits this commit
    // instead of rejoining on failure, so it must not be skipped or the revoked offsets are lost.
    if (!callback && !this.#consumer.isActive()) {
      return
    }

    this.#autocommitInflight = true
    const offsets = Array.from(this.#offsetsToCommit.values())
    this.#offsetsToCommit.clear()

    this.#consumer.commit({ offsets }, error => {
      this.#autocommitInflight = false

      if (error) {
        this.emit('autocommit', error)
        // Only destroy the stream when the broker requires a group rejoin
        // (ILLEGAL_GENERATION, UNKNOWN_MEMBER_ID, REBALANCE_IN_PROGRESS).
        // Transient coordinator errors must not tear down the consumption loop.
        if (findErrorBy(error, 'needsRejoin', true)) {
          // The stream is going away: every pending manual commit, for every partition, must be
          // rejected now, since a destroyed stream never gets another chance to flush them.
          for (const waiters of this.#commitWaiters.values()) {
            for (const waiter of waiters) {
              waiter.callback(error)
            }
          }
          this.#commitWaiters.clear()
          this.destroy(error)
        } else {
          // Keep offsets produced while the commit was in flight, and retry the failed snapshot
          // without replacing a newer offset for the same partition.
          for (const offset of offsets) {
            const key = partitionKey(offset.topic, offset.partition)
            const current = this.#offsetsToCommit.get(key)
            if (!current || current.offset < offset.offset) {
              this.#offsetsToCommit.set(key, offset)
            }
          }

          // Report the failure to whichever manual commits were part of this snapshot. The
          // offset stays queued above and is retried by the next flush regardless.
          for (const offset of offsets) {
            this.#settleCommitWaiters(offset.topic, offset.partition, offset.offset, error)
          }

          // A manual commit queued for a different, newer offset while this flush was in
          // flight was not part of the failed snapshot and must not wait on this stream's
          // autocommit timer (or never, if autocommit is disabled) to get its own attempt.
          if (this.#commitWaiters.size > 0) {
            this[kAutocommit]()
          }
        }
        callback?.(error)
        return
      }

      for (const { topic, partition, offset } of offsets) {
        this[kUpdateCommittedOffset](topic, partition, offset)
        this.#settleCommitWaiters(topic, partition, offset, null)
      }

      this.emit('autocommit', null, offsets)
      callback?.(null)

      // A manual commit queued while this flush was in flight is still waiting: its offset
      // was not part of the snapshot above, so drive another flush now instead of leaving it
      // stranded until the next autocommit tick.
      if (this.#commitWaiters.size > 0) {
        this[kAutocommit]()
      }
    })
  }

  #refreshOffsets (callback: Callback<void>) {
    /* c8 ignore next 4 - Hard to test */
    if (this.#topics.length === 0) {
      callback(null)
      return
    }

    // List topic offsets
    this.#consumer.listOffsets(
      {
        topics: this.#topics,
        timestamp:
          this.#mode === MessagesStreamModes.EARLIEST ||
          (this.#mode !== MessagesStreamModes.LATEST && this.#fallbackMode === MessagesStreamFallbackModes.EARLIEST)
            ? ListOffsetTimestamps.EARLIEST
            : ListOffsetTimestamps.LATEST
      },
      (error, offsets) => {
        if (error) {
          /* c8 ignore next 4 - Hard to test */
          if (this.#closed || this.closed || this.destroyed) {
            callback(null)
            return
          }

          callback(error)
          return
        }

        if (this.#mode !== MessagesStreamModes.COMMITTED) {
          this.#assignOffsets(offsets!, new Map(), callback)
          return
        }

        // Now restore group offsets
        const topics: GroupAssignment[] = []
        for (const topic of this.#topics) {
          const assignment = this.#assignmentsForTopic(topic)

          if (!assignment) {
            continue
          }

          topics.push(assignment)
        }

        if (!topics.length) {
          // No assignment yet: do not seed fallback offsets for the whole topic.
          // consumer:group:join will refresh once partitions are assigned.
          this.emit('offsets')
          callback(null)
          return
        }

        this.#consumer.listCommittedOffsets({ topics }, (error, commits) => {
          if (error) {
            /* c8 ignore next 4 - Hard to test */
            if (this.#closed || this.closed || this.destroyed) {
              callback(null)
              return
            }

            callback(error)
            return
          }

          this.#assignOffsets(offsets!, commits!, callback)
        })
      }
    )
  }

  #scheduleRefreshOffsetsAndFetch (destroyOnError = true) {
    // Invalidate outstanding fetches immediately, not only after restoration completes.
    this.#offsetsEpoch++
    this.#refreshOffsetsDestroyOnError ||= destroyOnError

    /* c8 ignore next 4 - Hard to test */
    if (this.#refreshOffsetsInflight) {
      this.#refreshOffsetsPending = true
      return
    }

    this.#refreshOffsetsInflight = true

    this.#refreshOffsets(error => {
      const shouldDestroyOnError = this.#refreshOffsetsDestroyOnError
      const wasPending = this.#refreshOffsetsPending
      this.#refreshOffsetsInflight = false
      this.#refreshOffsetsDestroyOnError = false
      this.#refreshOffsetsPending = false

      /* c8 ignore next 9 - Hard to test */
      if (error) {
        if (shouldDestroyOnError) {
          this.destroy(error)
        }

        return
      }

      // A new one was scheduled while the previous one was inflight, we need to run it immediately
      if (wasPending) {
        this.#scheduleRefreshOffsetsAndFetch(shouldDestroyOnError)
        return
      }

      this.#fetch()
    })
  }

  [kRefreshOffsetsAndFetch] () {
    this.#scheduleRefreshOffsetsAndFetch(false)
  }

  #assignOffsets (offsets: Offsets, commits: Offsets, callback: Callback<void>) {
    for (const [topic, partitions] of offsets) {
      for (let i = 0; i < partitions.length; i++) {
        if (!this.#offsetsToFetch.has(`${topic}:${i}`)) {
          this.#offsetsToFetch.set(`${topic}:${i}`, partitions[i])
        }
      }
    }

    for (const [topic, partitions] of commits) {
      for (let i = 0; i < partitions.length; i++) {
        const offset = partitions[i]

        if (offset >= 0n) {
          this.#offsetsToFetch.set(`${topic}:${i}`, offset)
        } else if (this.#fallbackMode === MessagesStreamFallbackModes.FAIL) {
          callback(
            new UserError(
              `Topic ${topic} has no committed offset on partition ${i} for group ${this.#consumer.groupId}.`,
              { topic, partition: i, groupId: this.#consumer.groupId }
            )
          )
          return
        }
      }
    }

    // Rebuild the list of offsetsCommitted (which is used for consumer lag) out of the offsets to fetch
    for (const topic of this.#topics) {
      const assignment = this.#assignmentsForTopic(topic)

      // This consumer has no assignment for the topic, continue
      if (!assignment) {
        continue
      }

      const partitions = assignment.partitions

      for (const partition of partitions) {
        const key = partitionKey(topic, partition)
        const committed = this.#offsetsToFetch.get(key)!
        this.#offsetsCommitted.set(key, committed)
      }
    }

    this.emit('offsets')
    callback(null)
  }

  #assignmentsForTopic (topic: string): GroupAssignment | undefined {
    return this.#consumer.assignments?.find(assignment => assignment.topic === topic)
  }

  #afterClose (error: Error | null) {
    this[kConnections].close(closeError => {
      for (const callback of this.#closeCallbacks) {
        callback(error ?? closeError)
      }

      this.#closeCallbacks = []
    })
  }

  /* c8 ignore next 3 - This is a private API used to debug during development */
  [kInspect] (...args: unknown[]): void {
    this.#consumer[kInspect](...args)
  }

  #beforeDeserialization (
    hook: BeforeDeserializationHook,
    metadata: ClusterMetadata,
    topicIds: Map<string, string>,
    response: FetchResponse,
    requestedOffsets: Map<string, bigint>,
    offsetsEpoch: number
  ) {
    const requests: [Buffer | null, BeforeHookPayloadType, MessageToConsumeWithDeserializationError][] = []

    // Create the pre-deserialization requests
    for (const topicResponse of response.responses) {
      const topic = topicIds.get(topicResponse.topicId)!

      for (const { records: recordsBatches, partitionIndex: partition } of topicResponse.partitions) {
        // The broker can return partitions which were not requested when they linger
        // in an incremental fetch session - do not run hooks on their stale records
        if (!requestedOffsets.has(partitionKey(topic, partition))) {
          continue
        }

        /* c8 ignore next 3 - Hard to test */
        if (!recordsBatches) {
          continue
        }

        for (const batch of recordsBatches) {
          // Filter control markers
          /* c8 ignore next 3 - Hard to test */
          if (batch.attributes & IS_CONTROL) {
            continue
          }

          for (const message of batch.records as MessageToConsumeWithDeserializationError[]) {
            message.topic = topic
            message.partition = partition

            requests.push([message.key, 'key', message])
            requests.push([message.value, 'value', message])

            for (const [headerKey, headerValue] of message.headers) {
              requests.push([headerKey!, 'headerKey', message])
              requests.push([headerValue!, 'headerValue', message])
            }
          }
        }
      }
    }

    if (requests.length === 0) {
      this.#pushRecords(metadata, topicIds, response, requestedOffsets, offsetsEpoch)
      return
    }

    runAsyncSeries(
      (request, cb) => {
        const [data, type, message] = request

        /*
          Without an error handler a failing hook destroys the stream, as it always did. With one,
          the failure is recorded on the message it belongs to and reported to the handler in
          #pushRecords, together with everything else which can fail while deserializing, so that
          the caller can skip or degrade that single message instead of losing the stream.
        */
        const onHookSettled: Callback<void> = error => {
          if (error && this.#deserializationErrorHandler) {
            message[kDeserializationError] ??= { error, payloadType: type }
            cb(null)
            return
          }

          cb(error)
        }

        const result = hook(data, type, message, onHookSettled) as Promise<void>

        if (typeof result?.then === 'function') {
          result.then(() => onHookSettled(null), onHookSettled)
        }
      },
      requests,
      0,
      error => {
        if (offsetsEpoch !== this.#offsetsEpoch || this.#maxFetchesReached()) {
          return
        }

        if (error) {
          this.destroy(error)
          return
        }

        this.#pushRecords(metadata, topicIds, response, requestedOffsets, offsetsEpoch)
      }
    )
  }
}
