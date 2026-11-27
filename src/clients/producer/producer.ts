import { randomUUID } from 'crypto'
import {
  type CallbackWithPromise,
  createPromisifiedCallback,
  kCallbackPromise,
  runConcurrentCallbacks
} from '../../apis/callbacks.ts'
import { FindCoordinatorKeyTypes, ProduceAcks } from '../../apis/enumerations.ts'
import { type FindCoordinatorRequest, type FindCoordinatorResponse } from '../../apis/metadata/find-coordinator-v6.ts'
import { type AddOffsetsToTxnRequest, type AddOffsetsToTxnResponse } from '../../apis/producer/add-offsets-to-txn-v4.ts'
import {
  type AddPartitionsToTxnRequest,
  type AddPartitionsToTxnRequestTopic
} from '../../apis/producer/add-partitions-to-txn-v5.ts'
import { type EndTxnRequest, type EndTxnResponse } from '../../apis/producer/end-txn-v4.ts'
import { type InitProducerIdRequest, type InitProducerIdResponse } from '../../apis/producer/init-producer-id-v5.ts'
import {
  type ProduceRequest,
  type ProduceResponse,
  type ProduceResponseTopic
} from '../../apis/producer/produce-v11.ts'
import { type TxnOffsetCommitRequest, type TxnOffsetCommitResponse } from '../../apis/producer/txn-offset-commit-v4.ts'
import {
  createDiagnosticContext,
  producerInitIdempotentChannel,
  producerSendsChannel,
  producerTransactionsChannel
} from '../../diagnostic.ts'
import {
  findErrorBy,
  type GenericError,
  NetworkError,
  type ProtocolError,
  TimeoutError,
  UserError
} from '../../errors.ts'
import { type Broker, type Connection } from '../../network/connection.ts'
import {
  type CreateRecordsBatchOptions,
  type Message,
  type MessageConsumerMetadata,
  type MessageRecord,
  type MessageToProduce
} from '../../protocol/records.ts'
import { runAsyncSeries } from '../../registries/abstract.ts'
import {
  kInstance,
  kTransaction,
  kTransactionAddOffsets,
  kTransactionAddPartitions,
  kTransactionCancel,
  kTransactionCommitOffset,
  kTransactionEnd,
  kTransactionFindCoordinator,
  kTransactionPrepare
} from '../../symbols.ts'
import { emitExperimentalApiWarning, NumericMap } from '../../utils.ts'
import {
  Base,
  kAfterCreate,
  kCheckNotClosed,
  kClosed,
  kGetApi,
  kGetBootstrapConnection,
  kGetConnection,
  kMetadata,
  kOptions,
  kPerformDeduplicated,
  kPerformWithRetry,
  kPrometheus,
  kValidateOptions
} from '../base/base.ts'
import { type ClusterMetadata } from '../base/types.ts'
import { type Counter, ensureMetric, type Gauge } from '../metrics.ts'
import {
  type BeforeHookPayloadType,
  type BeforeSerializationHook,
  type Serializer,
  jsonSerializer,
  type SerializerWithHeaders,
  stringSerializer
} from '../serde.ts'
import { defaultBatchMaxBytes } from './options.ts'
import {
  produceOptionsValidator,
  producerOptionsValidator,
  producerStreamOptionsValidator,
  sendOptionsValidator
} from './validators.ts'
import { defaultPartitioner } from './partitioners.ts'
import { ProducerStream } from './producer-stream.ts'
import { Transaction } from './transaction.ts'
import {
  type Partitioner,
  type ProduceOptions,
  type ProduceResult,
  type ProducerInfo,
  type ProducerOptions,
  type ProducerStreamOptions,
  type SendOptions
} from './types.ts'

// Used with Map.forEach(), which unlike for...of does not allocate an array for each entry
let headersBytes = 0

function addHeaderBytes (value: Buffer | null | undefined, key: Buffer | null | undefined): void {
  headersBytes += (key?.length ?? 0) + (value?.length ?? 0)
}

/*
  How many bytes can be waiting while a lingered request is in flight, see #lingerSend.
  It is kept well below the default limit of the brokers for a batch of records (message.max.bytes, 1 MiB),
  as all the messages might be for the same partition.
*/
const lingerInflightMaxBytes = 262144

// How many serialized header keys a producer keeps, see #serializeHeaderKey
const maxSerializedHeaderKeys = 256

// Don't move this function as being in the same file will enable V8 to remove.
// For futher info, ask Matteo.
export function noopSerializer (data?: Buffer): Buffer | undefined {
  return data
}

// The serializers which ignore the headers and the message they are given
const dataOnlySerializers = new Set<unknown>([noopSerializer, stringSerializer, jsonSerializer])

/*
  The send() calls which are waiting to be produced together when lingering is enabled.
  All of them were made with the same settings, see #lingerKey.
*/
// How many of the messages of a request belong to a send() call, and who to notify
interface SendCall {
  count: number
  callback: CallbackWithPromise<ProduceResult>
}

/*
  The callback of a send() which has a sendTimeout tells whether the send has failed because of it, see
  #sendWithTimeout. A request is not written, nor retried, once this is true for all the calls it is made for.
*/
const kTimedOut = Symbol('plt.kafka.producer.timedOut')

type TimedOutCheck = () => boolean

const abandonedMessage = 'The messages were not produced as all their sends timed out.'
type TimedCallback = CallbackWithPromise<ProduceResult> & { [kTimedOut]?: TimedOutCheck }

function timedOutCheckOf (callback: CallbackWithPromise<ProduceResult>): TimedOutCheck | undefined {
  return (callback as TimedCallback)[kTimedOut]
}

// When some of the calls have a sendTimeout, what tells whether all of them have failed because of it
function sendsTimedOutCheck (sends: SendCall[]): TimedOutCheck | undefined {
  for (const { callback } of sends) {
    if (timedOutCheckOf(callback)) {
      return () => sends.every(send => timedOutCheckOf(send.callback)?.() === true)
    }
  }

  return undefined
}

interface PendingSends {
  key: string
  topics: Set<string>
  messages: MessageRecord[]
  sends: SendCall[]
  bytes: number
  sendOptions: Required<SendOptions<any, any, any, any>>
  produceOptions: Partial<CreateRecordsBatchOptions>
  timer: NodeJS.Timeout
}

/*
  A produce operation, which sends the messages of one or more send() calls in a single request per broker.

  The operations which write to the same partition are sent one at a time, see #performOrderedSend. pending is the
  number of partitions where an earlier operation has not completed yet: the operation starts when it is zero.
*/
interface OrderedSend {
  pending: number
  started: boolean
  // Only set for the operations which had to wait, as those are the only ones other calls can be added to
  key: string
  bytes: number
  topics: string[]
  messages: MessageRecord[]
  sends: SendCall[]
  // The partitions where the operation has been the last one, which is what #partitionTails might still hold
  tailTopics: string[]
  tailPartitions: number[]
  // The operations which wait for this one, once for each partition they wait on
  waiters: OrderedSend[] | undefined
  sendOptions: Required<SendOptions<any, any, any, any>>
  produceOptions: Partial<CreateRecordsBatchOptions>
}

/*
  How many bytes of keys, values and headers can be added to an operation which is waiting for its turn.
  This keeps the batch of each partition well within the default limit of the brokers (message.max.bytes).
  A producer whose batchMaxBytes is larger uses that instead, as it does when lingering.
*/
const maxWaitingBytes = lingerInflightMaxBytes

/*
  The messages of a send which go to the same broker. When only some of its partitions fail, the broker has
  appended the others: their response is moved to produced and their messages are removed, see #acknowledge.
*/
interface DestinationSend {
  node: number
  messages: MessageRecord[]
  produced: ProduceResponse[]
}

function messagesBytes (messages: MessageRecord[]): number {
  let bytes = 0

  for (const message of messages) {
    bytes += (message.key?.length ?? 0) + (message.value?.length ?? 0)

    if (message.headers) {
      headersBytes = 0
      message.headers.forEach(addHeaderBytes)
      bytes += headersBytes
    }
  }

  return bytes
}

export class Producer<Key = Buffer, Value = Buffer, HeaderKey = Buffer, HeaderValue = Buffer> extends Base<
  ProducerOptions<Key, Value, HeaderKey, HeaderValue>
> {
  #partitionsRoundRobin: NumericMap
  // These two values should be serializable and loadable in the constructor in order to restore
  // the idempotent producer status.
  #producerInfo: ProducerInfo | undefined
  #sequences: NumericMap
  #keySerializer: SerializerWithHeaders<Key, HeaderKey, HeaderValue>
  #valueSerializer: SerializerWithHeaders<Value, HeaderKey, HeaderValue>
  #headerKeySerializer: Serializer<HeaderKey>
  #headerValueSerializer: Serializer<HeaderValue>
  #serializedHeaderKeys: Map<string, Buffer>
  #serializersUseHeaders: boolean
  #metricsProducedMessages: Counter | undefined
  #coordinatorId: number | undefined
  #transaction: Transaction<Key, Value, HeaderKey, HeaderValue> | undefined
  #streams: Set<ProducerStream<Key, Value, HeaderKey, HeaderValue>>
  #linger: number
  #batchMaxBytes: number
  #pendingSends: PendingSends | undefined
  #lingerInflight: number
  // For each partition with operations which have not completed, the last of them
  #partitionTails: Map<string, Map<number, OrderedSend>>
  #orderedSends: boolean
  #sendOperation: (
    options: SendOptions<Key, Value, HeaderKey, HeaderValue>,
    callback: CallbackWithPromise<ProduceResult>
  ) => void

  constructor (options: ProducerOptions<Key, Value, HeaderKey, HeaderValue>) {
    if (options.idempotent) {
      options.maxInflights = 1
      options.acks = ProduceAcks.ALL
      if (options.retries === undefined || (typeof options.retries === 'number' && options.retries <= 1)) {
        options.retries = Number.MAX_SAFE_INTEGER
      }
    } else {
      options.idempotent = false
    }

    options.repeatOnStaleMetadata ??= true

    super(options)

    this[kValidateOptions](options, producerOptionsValidator, '/options')

    if (options.beforeSerialization) {
      emitExperimentalApiWarning('beforeSerialization')
    }

    if (options.registry) {
      emitExperimentalApiWarning('registry (Confluent Schema Registry integration)')
    }

    let serializers = options.serializers
    if (options.registry) {
      if (options.beforeSerialization) {
        throw new UserError('/options/beforeSerialization cannot be provided when /options/registry is provided.')
      } else if (options.serializers) {
        throw new UserError('/options/serializers cannot be provided when /options/registry is provided.')
      }

      serializers = options.registry.getSerializers()
    }

    this.#partitionsRoundRobin = new NumericMap()
    this.#sequences = new NumericMap()
    this.#keySerializer = serializers?.key ?? (noopSerializer as Serializer<Key>)
    this.#valueSerializer = serializers?.value ?? (noopSerializer as Serializer<Value>)
    this.#headerKeySerializer = serializers?.headerKey ?? (noopSerializer as Serializer<HeaderKey>)
    this.#headerValueSerializer = serializers?.headerValue ?? (noopSerializer as Serializer<HeaderValue>)
    this.#serializedHeaderKeys = new Map()
    // The serializers of this package only look at the data, so the map of the headers is not built for them
    this.#serializersUseHeaders =
      !dataOnlySerializers.has(this.#keySerializer) || !dataOnlySerializers.has(this.#valueSerializer)
    this.#streams = new Set()
    this.#linger = options.linger ?? 0
    this.#partitionTails = new Map()
    this.#orderedSends = options.orderedSends ?? false
    this.#batchMaxBytes = options.batchMaxBytes ?? defaultBatchMaxBytes
    this.#lingerInflight = 0
    this[kOptions].transactionalId ??= randomUUID()

    if (this[kPrometheus]) {
      ensureMetric<Gauge>(this[kPrometheus], 'Gauge', 'kafka_producers', 'Number of active Kafka producers').inc()

      this.#metricsProducedMessages = ensureMetric<Counter>(
        this[kPrometheus],
        'Counter',
        'kafka_produced_messages',
        'Number of produced Kafka messages'
      )
    }

    if (options.registry) {
      this.#sendOperation = this.#beforeSerialization.bind(this, options.registry.getBeforeSerializationHook())
    } else if (options.beforeSerialization) {
      this.#sendOperation = this.#beforeSerialization.bind(this, options.beforeSerialization)
    } else {
      this.#sendOperation = this.#send.bind(this)
    }

    this[kAfterCreate]('producer')
  }

  get producerId (): bigint | undefined {
    return this.#producerInfo?.producerId
  }

  get producerEpoch (): number | undefined {
    return this.#producerInfo?.producerEpoch
  }

  get transaction (): Transaction<Key, Value, HeaderKey, HeaderValue> | undefined {
    return this.#transaction
  }

  get coordinatorId (): number | undefined {
    return this.#coordinatorId
  }

  get streamsCount (): number {
    return this.#streams.size
  }

  close (force?: boolean): Promise<void>
  close (force: boolean | CallbackWithPromise<void>, callback?: CallbackWithPromise<void>): void
  close (force?: boolean | CallbackWithPromise<void>, callback?: CallbackWithPromise<void>): void | Promise<void> {
    if (typeof force === 'function') {
      callback = force
      force = false
    }

    if (!callback) {
      callback = createPromisifiedCallback<void>()
    }

    if (this[kClosed]) {
      callback(null)
      return callback[kCallbackPromise]
    }

    // Messages which are still lingering must be sent before closing
    if (this.#pendingSends) {
      this.#flushPendingSends(() => {
        this.close(force as boolean, callback!)
      })

      return callback[kCallbackPromise]
    }

    this[kClosed] = true

    this.#performClose(force as boolean, callback)

    return callback[kCallbackPromise]
  }

  #performClose (force: boolean, callback: CallbackWithPromise<void>): void {
    for (const stream of this.#streams) {
      /* c8 ignore next 3 - Hard to test */
      if (stream.closed || stream.destroyed) {
        this.#streams.delete(stream)
      }
    }

    if (this.#streams.size) {
      if (!force) {
        this[kClosed] = false
        callback(new UserError('Cannot close producer while producing messages.'))
        return
      }

      runConcurrentCallbacks(
        'Closing streams failed.',
        this.#streams,
        (stream, concurrentCallback) => {
          stream.close(concurrentCallback)
        },
        error => {
          /* c8 ignore next 5 - Hard to test */
          if (error) {
            this[kClosed] = false
            callback(error)
            return
          }

          this.#streams.clear()
          this.#performClose(false, callback)
        }
      )

      return
    }

    super.close(error => {
      if (error) {
        this[kClosed] = false
        callback(error)
        return
      }

      if (this[kPrometheus]) {
        ensureMetric<Gauge>(this[kPrometheus], 'Gauge', 'kafka_producers', 'Number of active Kafka producers').dec()
      }

      callback(null)
    })
  }

  initIdempotentProducer (
    options: ProduceOptions<Key, Value, HeaderKey, HeaderValue>,
    callback: CallbackWithPromise<ProducerInfo>
  ): void
  initIdempotentProducer (options: ProduceOptions<Key, Value, HeaderKey, HeaderValue>): Promise<ProducerInfo>
  initIdempotentProducer (
    options: ProduceOptions<Key, Value, HeaderKey, HeaderValue>,
    callback?: CallbackWithPromise<ProducerInfo>
  ): void | Promise<ProducerInfo> {
    if (!callback) {
      callback = createPromisifiedCallback<ProducerInfo>()
    }

    if (this[kCheckNotClosed](callback)) {
      return callback[kCallbackPromise]
    }

    const validationError = this[kValidateOptions](options, produceOptionsValidator, '/options', false)
    if (validationError) {
      callback(validationError)
      return callback[kCallbackPromise]
    }

    producerInitIdempotentChannel.traceCallback(
      this.#initIdempotentProducer,
      1,
      createDiagnosticContext({ client: this, operation: 'initIdempotentProducer', options }),
      this,
      options,
      callback
    )

    return callback[kCallbackPromise]!
  }

  send (
    options: SendOptions<Key, Value, HeaderKey, HeaderValue> & { [kTransaction]?: number },
    callback: CallbackWithPromise<ProduceResult>
  ): void
  send (options: SendOptions<Key, Value, HeaderKey, HeaderValue> & { [kTransaction]?: number }): Promise<ProduceResult>
  send (
    options: SendOptions<Key, Value, HeaderKey, HeaderValue> & { [kTransaction]?: number },
    callback?: CallbackWithPromise<ProduceResult>
  ): void | Promise<ProduceResult> {
    if (!callback) {
      callback = createPromisifiedCallback<ProduceResult>()
    }

    if (this[kCheckNotClosed](callback)) {
      return callback[kCallbackPromise]
    }

    if (this.#transaction && options[kTransaction] !== this.#transaction[kInstance]) {
      callback(
        new UserError(
          options[kTransaction]
            ? 'The producer is in use by another transaction.'
            : 'The producer is in use by a transaction.'
        )
      )

      return callback[kCallbackPromise]
    }

    const validationError = this[kValidateOptions](options, sendOptionsValidator, '/options', false)
    if (validationError) {
      callback(validationError)
      return callback[kCallbackPromise]
    }

    const idempotent = options.idempotent ?? this[kOptions].idempotent

    if (idempotent) {
      if (typeof options.producerId !== 'undefined' || typeof options.producerEpoch !== 'undefined') {
        callback(new UserError('Cannot specify producerId or producerEpoch when using idempotent producer.'))

        return callback[kCallbackPromise]
      }

      if (typeof options.acks !== 'undefined' && options.acks !== ProduceAcks.ALL) {
        callback(new UserError('Idempotent producer requires acks to be ALL (-1).'))

        return callback[kCallbackPromise]
      }
    }

    /*
      A transaction must only be committed or aborted once its requests have completed. A send which failed while
      its request was still in flight would let them race, so the sends of a transaction are not bounded.
    */
    if (this.#transaction && typeof options.sendTimeout !== 'undefined') {
      callback(new UserError('Cannot use sendTimeout within a transaction.'))
      return callback[kCallbackPromise]
    }

    options.acks ??= idempotent ? ProduceAcks.ALL : ProduceAcks.LEADER

    const sendTimeout = this.#transaction ? undefined : (options.sendTimeout ?? this[kOptions].sendTimeout)

    producerSendsChannel.traceCallback(
      typeof sendTimeout === 'number' ? this.#sendWithTimeout.bind(this, sendTimeout) : this.#sendOperation,
      1,
      createDiagnosticContext({ client: this, operation: 'send', options }),
      this,
      options,
      callback
    )

    return callback[kCallbackPromise]
  }

  asStream (
    options?: ProducerStreamOptions<Key, Value, HeaderKey, HeaderValue>
  ): ProducerStream<Key, Value, HeaderKey, HeaderValue> {
    options ??= {}

    const validationError = this[kValidateOptions](options, producerStreamOptionsValidator, '/options', false)
    if (validationError) {
      throw validationError
    }

    const stream = new ProducerStream(this, options)
    this.#streams.add(stream)

    stream.once('close', () => {
      this.#streams.delete(stream)
    })

    return stream
  }

  beginTransaction (
    options: ProduceOptions<Key, Value, HeaderKey, HeaderValue>,
    callback: CallbackWithPromise<Transaction<Key, Value, HeaderKey, HeaderValue>>
  ): void
  beginTransaction (
    options?: ProduceOptions<Key, Value, HeaderKey, HeaderValue>
  ): Promise<Transaction<Key, Value, HeaderKey, HeaderValue>>
  beginTransaction (
    options?: ProduceOptions<Key, Value, HeaderKey, HeaderValue>,
    callback?: CallbackWithPromise<Transaction<Key, Value, HeaderKey, HeaderValue>>
  ): void | Promise<Transaction<Key, Value, HeaderKey, HeaderValue>> {
    if (!callback) {
      callback = createPromisifiedCallback<Transaction<Key, Value, HeaderKey, HeaderValue>>()
    }

    if (this[kCheckNotClosed](callback)) {
      return callback[kCallbackPromise]
    }

    if (!options) {
      options = {}
    }

    const validationError = this[kValidateOptions](options, produceOptionsValidator, '/options', false)
    if (validationError) {
      callback(validationError)
      return callback[kCallbackPromise]
    }

    if (!this[kOptions].idempotent) {
      callback(new UserError('Cannot begin a transaction on a non-idempotent producer.'))
      return callback[kCallbackPromise]
    }

    // See send()
    if (typeof options.sendTimeout !== 'undefined') {
      callback(new UserError('Cannot use sendTimeout within a transaction.'))
      return callback[kCallbackPromise]
    }

    if (this.#transaction) {
      callback(new UserError('There is already an active transaction.'))
      return callback[kCallbackPromise]
    }

    this.#transaction = new Transaction(this)

    producerTransactionsChannel.traceCallback(
      this.#transaction[kTransactionPrepare],
      2,
      createDiagnosticContext({ client: this, operation: 'begin' }),
      this.#transaction,
      this.#producerInfo?.transactionalId,
      options,
      callback
    )

    return callback[kCallbackPromise]
  }

  getSendTopicPartitions (options: SendOptions<Key, Value, HeaderKey, HeaderValue>): Map<string, Set<number>> {
    const topicsPartitions = new Map<string, Set<number>>()
    const partitioner = options.partitioner ?? this[kOptions].partitioner

    for (const message of options.messages) {
      const topic = message.topic
      let key: Buffer | undefined
      let headers = new Map<HeaderKey, HeaderValue>()

      try {
        if (message.headers) {
          headers =
            message.headers instanceof Map
              ? (message.headers as Map<HeaderKey, HeaderValue>)
              : new Map(Object.entries(message.headers) as [HeaderKey, HeaderValue][])
        }

        key = this.#keySerializer(message.key, headers, message)
      } catch (error) {
        throw new UserError('Failed to serialize a message.', { cause: error })
      }

      const partition = this.#assignPartition(message, partitioner, key, topic)

      let partitions = topicsPartitions.get(topic)
      if (!partitions) {
        partitions = new Set()
        topicsPartitions.set(topic, partitions)
      }

      partitions.add(partition)
    }

    return topicsPartitions
  }

  getSendBrokers (
    options: SendOptions<Key, Value, HeaderKey, HeaderValue>,
    callback: CallbackWithPromise<Record<string, Broker>>
  ): void
  getSendBrokers (options: SendOptions<Key, Value, HeaderKey, HeaderValue>): Promise<Record<string, Broker>>
  getSendBrokers (
    options: SendOptions<Key, Value, HeaderKey, HeaderValue>,
    callback?: CallbackWithPromise<Record<string, Broker>>
  ): void | Promise<Record<string, Broker>> {
    if (!callback) {
      callback = createPromisifiedCallback<Record<string, Broker>>()
    }

    if (this[kCheckNotClosed](callback)) {
      return callback[kCallbackPromise]
    }

    let topicsPartitions

    try {
      topicsPartitions = this.getSendTopicPartitions(options)
    } catch (error) {
      callback(error)
      return callback[kCallbackPromise]
    }

    this[kMetadata]({ topics: Array.from(topicsPartitions!.keys()), autocreateTopics: options.autocreateTopics }, (
      error,
      metadata
    ) => {
      if (error) {
        callback(error)
        return
      }

      const brokers: Record<string, Broker> = {}

      for (const [topic, partitions] of topicsPartitions!) {
        for (const rawPartition of partitions) {
          const partition = rawPartition % metadata!.topics.get(topic)!.partitionsCount
          const leader = metadata!.topics.get(topic)!.partitions[partition!].leader
          const broker = metadata!.brokers.get(leader)

          brokers[`${topic}:${partition}`] = broker!
        }
      }

      callback(null, brokers)
    })

    return callback[kCallbackPromise]
  }

  getSendConnections (
    options: SendOptions<Key, Value, HeaderKey, HeaderValue>,
    callback: CallbackWithPromise<Record<string, Connection>>
  ): void
  getSendConnections (options: SendOptions<Key, Value, HeaderKey, HeaderValue>): Promise<Record<string, Connection>>
  getSendConnections (
    options: SendOptions<Key, Value, HeaderKey, HeaderValue>,
    callback?: CallbackWithPromise<Record<string, Connection>>
  ): void | Promise<Record<string, Connection>> {
    if (!callback) {
      callback = createPromisifiedCallback<Record<string, Connection>>()
    }

    if (this[kCheckNotClosed](callback)) {
      return callback[kCallbackPromise]
    }

    this.getSendBrokers(options, (error, brokers) => {
      if (error) {
        callback(error)
        return
      }

      runConcurrentCallbacks<[string, Connection], [string, Broker]>(
        'Preparing producer connections failed.',
        Object.entries(brokers!),
        ([topicPartition, broker], concurrentCallback) => {
          this[kGetConnection](broker, (error, connection) => {
            if (error) {
              concurrentCallback(error)
              return
            }

            concurrentCallback(null, [topicPartition, connection!])
          })
        },
        (error, results) => {
          if (error) {
            callback(error)
            return
          }

          callback(null, Object.fromEntries(results!))
        }
      )
    })

    return callback[kCallbackPromise]
  }

  [kTransactionFindCoordinator] (callback: CallbackWithPromise<void>): void {
    if (this.#coordinatorId !== undefined) {
      callback(null)
      return
    }

    const transactionalId = this.#transaction!.id

    this[kPerformDeduplicated](
      'findCoordinator',
      deduplicateCallback => {
        this[kPerformWithRetry]<FindCoordinatorResponse>(
          'findCoordinator',
          (retryCallback, attempt) => {
            this[kGetBootstrapConnection]((error, connection) => {
              if (error) {
                retryCallback(error)
                return
              }

              this[kGetApi]<FindCoordinatorRequest, FindCoordinatorResponse>('FindCoordinator', (error, api) => {
                if (error) {
                  retryCallback(error)
                  return
                }

                api!(connection!, FindCoordinatorKeyTypes.TRANSACTION, [transactionalId], (error, response) => {
                  if (response && api!.version < 4) {
                    response.coordinators[0].key = transactionalId
                  }

                  retryCallback(error, response)
                })
              })
            }, attempt)
          },
          (error, response) => {
            if (error) {
              deduplicateCallback(error)
              return
            }

            const groupInfo = response!.coordinators.find(coordinator => coordinator.key === transactionalId)!
            this.#coordinatorId = groupInfo.nodeId

            deduplicateCallback(null)
          },
          0
        )
      },
      callback
    )
  }

  [kTransactionAddPartitions] (
    transactionId: number,
    topicsPartitions: Map<string, Set<number>>,
    callback: CallbackWithPromise<void>
  ): void {
    if (transactionId !== this.#transaction?.[kInstance]) {
      callback(new UserError('The producer is in use by another transaction.'))
      return
    }

    const transactions: AddPartitionsToTxnRequestTopic[] = []

    for (const [topic, partitionsSet] of topicsPartitions) {
      transactions.push({
        name: topic,
        partitions: Array.from(partitionsSet)
      })
    }

    this[kPerformDeduplicated](
      'addPartitionsToTransaction',
      deduplicateCallback => {
        this[kPerformWithRetry]<void>(
          'addPartitionsToTransaction',
          retryCallback => {
            this.#getCoordinatorConnection((error, connection) => {
              if (error) {
                retryCallback(this.#handleError(error))
                return
              }

              this[kGetApi]<AddPartitionsToTxnRequest, AddOffsetsToTxnResponse>('AddPartitionsToTxn', (error, api) => {
                if (error) {
                  retryCallback(this.#handleError(error))
                  return
                }

                api!(
                  connection!,
                  [
                    {
                      transactionalId: this.#transaction!.id,
                      producerId: this.#producerInfo!.producerId,
                      producerEpoch: this.#producerInfo!.producerEpoch,
                      topics: transactions,
                      verifyOnly: false
                    }
                  ],
                  error => {
                    if (error) {
                      retryCallback(this.#handleError(error))
                      return
                    }

                    retryCallback(null)
                  }
                )
              })
            })
          },
          deduplicateCallback
        )
      },
      callback
    )
  }

  [kTransactionAddOffsets] (transactionId: number, groupId: string, callback: CallbackWithPromise<void>): void {
    if (transactionId !== this.#transaction?.[kInstance]) {
      callback(new UserError('The producer is in use by another transaction.'))
      return
    }

    this[kPerformDeduplicated](
      'addOffsetsToTransaction',
      deduplicateCallback => {
        this[kPerformWithRetry]<void>(
          'addOffsetsToTransaction',
          retryCallback => {
            this.#getCoordinatorConnection((error, connection) => {
              if (error) {
                retryCallback(this.#handleError(error))
                return
              }

              this[kGetApi]<AddOffsetsToTxnRequest, AddOffsetsToTxnResponse>('AddOffsetsToTxn', (error, api) => {
                if (error) {
                  retryCallback(this.#handleError(error))
                  return
                }

                api!(
                  connection!,
                  this.#transaction!.id,
                  this.#producerInfo!.producerId,
                  this.#producerInfo!.producerEpoch,
                  groupId,
                  error => {
                    if (error) {
                      retryCallback(this.#handleError(error))
                      return
                    }

                    retryCallback(null)
                  }
                )
              })
            })
          },
          deduplicateCallback
        )
      },
      callback
    )
  }

  [kTransactionCommitOffset]<
    MessageKey = Buffer,
    MessageValue = Buffer,
    MessageHeaderKey = Buffer,
    MessageHeaderValue = Buffer
  > (
    transactionId: number,
    message: Message<MessageKey, MessageValue, MessageHeaderKey, MessageHeaderValue>,
    callback: CallbackWithPromise<void>
  ): void {
    if (transactionId !== this.#transaction?.[kInstance]) {
      callback(new UserError('The producer is in use by another transaction.'))
      return
    }

    this[kPerformDeduplicated](
      'commit',
      deduplicateCallback => {
        this[kPerformWithRetry]<void>(
          'commit',
          retryCallback => {
            this[kMetadata]({ topics: [message.topic] }, (error, metadata) => {
              if (error) {
                retryCallback(error)
                return
              }

              const { groupId, generationId, memberId, coordinatorId } = message.metadata
                .consumer as MessageConsumerMetadata

              this[kGetConnection](metadata!.brokers.get(coordinatorId)!, (error, connection) => {
                if (error) {
                  retryCallback(error)
                  return
                }

                this[kGetApi]<TxnOffsetCommitRequest, TxnOffsetCommitResponse>('TxnOffsetCommit', (error, api) => {
                  if (error) {
                    retryCallback(error)
                    return
                  }

                  const { topic, partition } = message

                  api!(
                    connection!,
                    this.#transaction!.id,
                    groupId,
                    this.#producerInfo!.producerId,
                    this.#producerInfo!.producerEpoch,
                    generationId,
                    memberId,
                    null,
                    [
                      {
                        name: topic,
                        partitions: [
                          {
                            partitionIndex: message.partition!,
                            committedOffset: message.offset! + 1n,
                            committedLeaderEpoch: metadata!.topics.get(topic)!.partitions[partition].leaderEpoch
                          }
                        ]
                      }
                    ],
                    error => {
                      if (error) {
                        retryCallback(error)
                        return
                      }

                      retryCallback(null)
                    }
                  )
                })
              })
            })
          },
          deduplicateCallback
        )
      },
      callback
    )
  }

  [kTransactionEnd] (transactionId: number, commit: boolean, callback: CallbackWithPromise<void>): void {
    if (transactionId !== this.#transaction?.[kInstance]) {
      callback(new UserError('The producer is in use by another transaction.'))
      return
    }

    this[kPerformDeduplicated](
      'endTransaction',
      deduplicateCallback => {
        this[kPerformWithRetry]<void>(
          'endTransaction',
          retryCallback => {
            this.#getCoordinatorConnection((error, connection) => {
              if (error) {
                retryCallback(this.#handleError(error))
                return
              }

              this[kGetApi]<EndTxnRequest, EndTxnResponse>('EndTxn', (error, api) => {
                if (error) {
                  retryCallback(this.#handleError(error))
                  return
                }

                api!(
                  connection!,
                  this.#transaction!.id,
                  this.#producerInfo!.producerId,
                  this.#producerInfo!.producerEpoch,
                  commit,
                  error => {
                    if (error) {
                      this.#handleFencingError(error)
                      retryCallback(this.#handleError(error))
                      return
                    }

                    this.#transaction = undefined
                    retryCallback(null)
                  }
                )
              })
            })
          },
          deduplicateCallback
        )
      },
      callback
    )
  }

  [kTransactionCancel] (transactionId: number, callback: CallbackWithPromise<void>): void {
    if (transactionId !== this.#transaction?.[kInstance]) {
      callback(new UserError('The producer is in use by another transaction.'))
      return
    }

    this.#transaction = undefined
    callback(null)
  }

  #initIdempotentProducer (
    options: ProduceOptions<Key, Value, HeaderKey, HeaderValue>,
    callback: CallbackWithPromise<ProducerInfo>
  ) {
    const transactionalId = this.#transaction?.id

    this[kPerformDeduplicated](
      'initProducerId',
      deduplicateCallback => {
        this[kPerformWithRetry]<InitProducerIdResponse>(
          'initProducerId',
          (retryCallback, attempt) => {
            const onConnection: CallbackWithPromise<Connection> = (error, connection) => {
              if (error) {
                retryCallback(error)
                return
              }

              this[kGetApi]<InitProducerIdRequest, InitProducerIdResponse>('InitProducerId', (error, api) => {
                if (error) {
                  retryCallback(error)
                  return
                }

                api!(
                  connection!,
                  transactionalId,
                  this[kOptions].timeout!,
                  api!.version >= 3 ? (options.producerId ?? this[kOptions].producerId ?? -1n) : undefined,
                  api!.version >= 3 ? (options.producerEpoch ?? this[kOptions].producerEpoch ?? -1) : undefined,
                  (error, response) => {
                    retryCallback(this.#handleError(error), response)
                  }
                )
              })
            }

            // The transactional path fails over inside #getCoordinatorConnection (via
            // #findCoordinator -> kGetBootstrapConnection). The idempotent path must thread
            // `attempt` so kGetBootstrapConnection rotates past a reachable-but-hung broker.
            if (transactionalId) {
              this.#getCoordinatorConnection(onConnection)
            } else {
              this[kGetBootstrapConnection](onConnection, attempt)
            }
          },
          (error, response) => {
            if (error) {
              this.#handleFencingError(error)
              deduplicateCallback(this.#handleError(error))
              return
            }

            this.#producerInfo = {
              producerId: response!.producerId,
              producerEpoch: response!.producerEpoch,
              transactionalId
            }
            this.#sequences.clear()
            deduplicateCallback(null, this.#producerInfo)
          },
          0
        )
      },
      callback
    )
  }

  /*
    Fails the send() with a TimeoutError once sendTimeout milliseconds have passed, whatever is still going on.

    This is best-effort: a request which is in flight is not cancelled, so the messages might still be produced.
    A request stops retrying, and is not written again, only once all the calls it is made for have failed this
    way, as with linger it can carry the messages of calls which have a longer sendTimeout, or none.
  */
  #sendWithTimeout (
    sendTimeout: number,
    options: SendOptions<Key, Value, HeaderKey, HeaderValue>,
    callback: CallbackWithPromise<ProduceResult>
  ): void {
    let timedOut = false

    const timer = setTimeout(() => {
      timedOut = true
      callback(new TimeoutError(`Producing messages timed out after ${sendTimeout} ms.`))
    }, sendTimeout)

    const timedCallback: TimedCallback = (error, result) => {
      if (timedOut) {
        return
      }

      clearTimeout(timer)
      callback(error, result)
    }

    timedCallback[kTimedOut] = () => timedOut

    this.#sendOperation(options, timedCallback)
  }

  #send (options: SendOptions<Key, Value, HeaderKey, HeaderValue>, callback: CallbackWithPromise<ProduceResult>): void {
    options.idempotent ??= this[kOptions].idempotent
    options.repeatOnStaleMetadata ??= this[kOptions].repeatOnStaleMetadata
    options.partitioner ??= this[kOptions].partitioner

    const { idempotent, partitioner } = options as Required<SendOptions<Key, Value, HeaderKey, HeaderValue>>
    let producerId: bigint | undefined = options.producerId
    let producerEpoch: number | undefined = options.producerEpoch

    // If the producer was transactional and is not anymore, reset
    if (!this.#transaction && this.#producerInfo?.transactionalId) {
      this.#producerInfo = undefined
    }

    // Initialize the idempotent producer if required
    if (options.idempotent) {
      if (!this.#producerInfo) {
        const { messages, ...initOptions } = options

        this.initIdempotentProducer(initOptions, error => {
          if (error) {
            callback(error)
            return
          }

          this.#send(options, callback)
        })

        return
      }

      producerId = this.#producerInfo!.producerId
      producerEpoch = this.#producerInfo!.producerEpoch
    }

    const produceOptions: Partial<CreateRecordsBatchOptions> = {
      compression: options.compression ?? this[kOptions].compression,
      producerId,
      producerEpoch,
      sequences: idempotent ? this.#sequences : undefined,
      transactionalId: this.#transaction?.id
    }

    // Build messages records out of messages
    const topics = new Set<string>()
    const messages: MessageRecord[] = []
    for (const message of options.messages) {
      const topic = message.topic
      let key: Buffer | undefined
      let value: Buffer | undefined
      let headers: Map<HeaderKey, HeaderValue> | undefined
      let serializedHeaders: Map<Buffer, Buffer> | undefined
      const metadata = message.metadata

      try {
        if (message.headers) {
          serializedHeaders = new Map()

          if (message.headers instanceof Map) {
            headers = message.headers as Map<HeaderKey, HeaderValue>

            for (const [headerKey, headerValue] of headers) {
              serializedHeaders.set(
                this.#serializeHeaderKey(headerKey, metadata),
                this.#headerValueSerializer(headerValue, metadata)!
              )
            }
          } else {
            // The serializers of the key and the value receive the headers as a map, which is filled on the way
            const source = message.headers as Record<string, HeaderValue>

            if (this.#serializersUseHeaders) {
              headers = new Map()
            }

            for (const name in source) {
              if (!Object.hasOwn(source, name)) {
                continue
              }

              const headerValue = source[name]

              headers?.set(name as HeaderKey, headerValue)
              serializedHeaders.set(
                this.#serializeHeaderKey(name as HeaderKey, metadata),
                this.#headerValueSerializer(headerValue, metadata)!
              )
            }
          }
        }

        if (this.#serializersUseHeaders) {
          headers ??= new Map()
        }

        key = this.#keySerializer(message.key, headers, message)
        value = this.#valueSerializer(message.value, headers, message)!
      } catch (error) {
        callback(new UserError('Failed to serialize a message.', { cause: error }))
        return
      }

      const partition = this.#assignPartition(message, partitioner, key, topic)

      topics.add(topic)
      messages.push({
        key,
        value,
        headers: serializedHeaders,
        topic,
        partition,
        timestamp: message.timestamp
      })
    }

    const topicsArray = Array.from(topics)

    if (this.#linger > 0) {
      /*
        Transactions and explicit producer IDs are never merged with other send() calls.
        Whatever is lingering is sent first, so that messages are still produced in the order they were sent.
      */
      if (this.#transaction || typeof options.producerId !== 'undefined' || typeof options.producerEpoch !== 'undefined') {
        this.#flushPendingSends()
      } else {
        this.#lingerSend(
          topicsArray,
          messages,
          options as Required<SendOptions<Key, Value, HeaderKey, HeaderValue>>,
          produceOptions,
          callback
        )

        return
      }
    }

    if (this.#transaction) {
      // Get the metadata for topics, we need to normalize the partitions
      this[kMetadata]({ topics: topicsArray, autocreateTopics: options.autocreateTopics }, (error, metadata) => {
        if (error) {
          callback(error)
          return
        }

        this.#transaction?.addPartitions(metadata!, messages, error => {
          if (error) {
            callback(error)
            return
          }

          this.#performSend(
            topicsArray,
            messages,
            options as Required<SendOptions<Key, Value, HeaderKey, HeaderValue>>,
            produceOptions,
            callback
          )
        })
      })
    } else {
      this.#performSend(
        topicsArray,
        messages,
        options as Required<SendOptions<Key, Value, HeaderKey, HeaderValue>>,
        produceOptions,
        callback
      )
    }
  }

  /*
    Queues the messages of a send() call so that they are produced together with the ones of the other calls
    made within the linger time, or until batchMaxBytes are waiting.

    While a request made this way is in flight, reaching batchMaxBytes does not send another one: the messages
    keep waiting, up to lingerInflightMaxBytes, and are sent when the request completes or when the linger
    time expires. A producer which is faster than the broker therefore sends requests which
    are larger, not more numerous, and the cost of making a request is paid less often.

    Only calls made with the same settings can share a produce request. When the settings change, what is
    waiting is sent first.
  */
  #lingerSend (
    topics: string[],
    messages: MessageRecord[],
    sendOptions: Required<SendOptions<Key, Value, HeaderKey, HeaderValue>>,
    produceOptions: Partial<CreateRecordsBatchOptions>,
    callback: CallbackWithPromise<ProduceResult>
  ): void {
    const key = `${sendOptions.acks}:${produceOptions.compression}:${sendOptions.idempotent}:${sendOptions.autocreateTopics}:${sendOptions.repeatOnStaleMetadata}`

    const bytes = messagesBytes(messages)

    // A producer configured to wait for more than lingerInflightMaxBytes is not affected
    const maxBytes =
      this.#lingerInflight > 0 ? Math.max(this.#batchMaxBytes, lingerInflightMaxBytes) : this.#batchMaxBytes

    if (this.#pendingSends && (this.#pendingSends.key !== key || this.#pendingSends.bytes + bytes > maxBytes)) {
      this.#flushPendingSends()
    }

    let pending = this.#pendingSends

    if (!pending) {
      pending = this.#pendingSends = {
        key,
        topics: new Set(),
        messages: [],
        sends: [],
        bytes: 0,
        sendOptions,
        produceOptions,
        timer: setTimeout(() => this.#flushPendingSends(), this.#linger)
      }
    }

    for (const topic of topics) {
      pending.topics.add(topic)
    }

    for (const message of messages) {
      pending.messages.push(message)
    }

    pending.bytes += bytes

    pending.sends.push({ count: messages.length, callback })

    if (pending.bytes >= maxBytes) {
      this.#flushPendingSends()
    }
  }

  #flushPendingSends (onComplete?: () => void): void {
    const pending = this.#pendingSends

    if (!pending) {
      onComplete?.()
      return
    }

    this.#pendingSends = undefined
    clearTimeout(pending.timer)

    const { messages, sends } = pending

    this.#lingerInflight++

    const callback: TimedCallback = (error, result) => {
      this.#lingerInflight--

      // What has been waiting for this request is sent before the calls are notified, as they might send more
      if (this.#lingerInflight === 0 && this.#pendingSends && this.#pendingSends.bytes >= this.#batchMaxBytes) {
        this.#flushPendingSends()
      }

      this.#notifySends(messages, sends, error, result)
      onComplete?.()
    }

    callback[kTimedOut] = sendsTimedOutCheck(sends)

    this.#performSend(
      Array.from(pending.topics),
      messages,
      pending.sendOptions as Required<SendOptions<Key, Value, HeaderKey, HeaderValue>>,
      pending.produceOptions,
      callback
    )
  }

  /*
    Completes the send() calls which shared a request.

    The broker returns the offset of the first message of each partition. Messages keep their order
    within a partition, so the offset of each message is that one plus the number of messages which
    precede it in the same partition. Each send() is given the offset of its own first message.
  */
  #notifySends (
    messages: MessageRecord[],
    sends: SendCall[],
    error: Error | null,
    result: ProduceResult | undefined
  ): void {
    // A single send() gets exactly what it would have got on its own
    if (sends.length === 1) {
      sends[0].callback(error, result)
      return
    }

    if (error) {
      for (const send of sends) {
        send.callback(error)
      }

      return
    }

    if (!result!.offsets) {
      for (const send of sends) {
        send.callback(null, { unwritableNodes: result!.unwritableNodes?.slice() })
      }

      return
    }

    const firstOffsets = new Map<string, Map<number, bigint>>()

    for (const { topic, partition, offset } of result!.offsets) {
      let topicOffsets = firstOffsets.get(topic)
      if (!topicOffsets) {
        topicOffsets = new Map()
        firstOffsets.set(topic, topicOffsets)
      }

      topicOffsets.set(partition, offset)
    }

    const produced = new Map<string, Map<number, number>>()
    let index = 0

    for (const send of sends) {
      const offsets: NonNullable<ProduceResult['offsets']> = []
      const seen = new Map<string, Set<number>>()

      for (const end = index + send.count; index < end; index++) {
        const topic = messages[index].topic
        const partition = messages[index].partition!

        let topicProduced = produced.get(topic)
        if (!topicProduced) {
          topicProduced = new Map()
          produced.set(topic, topicProduced)
        }

        const position = topicProduced.get(partition) ?? 0
        topicProduced.set(partition, position + 1)

        let topicSeen = seen.get(topic)
        if (!topicSeen) {
          topicSeen = new Set()
          seen.set(topic, topicSeen)
        }

        if (!topicSeen.has(partition)) {
          topicSeen.add(partition)

          const firstOffset = firstOffsets.get(topic)?.get(partition)

          /* c8 ignore next 3 - The broker always returns an offset for the partitions it was sent */
          if (typeof firstOffset === 'undefined') {
            continue
          }

          // When the broker does not assign offsets (-1), there is nothing to add to
          offsets.push({ topic, partition, offset: firstOffset < 0n ? firstOffset : firstOffset + BigInt(position) })
        }
      }

      send.callback(null, { offsets })
    }
  }

  #performSend (
    topics: string[],
    messages: MessageRecord[],
    sendOptions: Required<SendOptions<Key, Value, HeaderKey, HeaderValue>>,
    produceOptions: Partial<CreateRecordsBatchOptions>,
    callback: CallbackWithPromise<ProduceResult>
  ): void {
    if (this.#orderedSends || sendOptions.idempotent) {
      this.#performOrderedSend(topics, messages, sendOptions, produceOptions, callback)
      return
    }

    // Get the metadata with the topic/partitions informations
    this[kMetadata]({ topics, autocreateTopics: sendOptions.autocreateTopics }, (error, metadata) => {
      if (error) {
        callback(error)
        return
      }

      const messagesByDestination = new Map<number, MessageRecord[]>()

      /*
        Track the number of messages per partition so we can update the sequence number.
        Sequences only exist for idempotent producers, so nothing is tracked otherwise. The count is
        kept by topic and then by partition so that no key has to be built for each message.
      */
      const messagesPerPartition = produceOptions.sequences ? new Map<string, Map<number, number>>() : undefined

      // Normalize the partition of all messages, then enqueue them to their destination
      for (const message of messages) {
        message.partition! %= metadata!.topics.get(message.topic)!.partitionsCount

        const { topic, partition } = message
        const leader = metadata!.topics.get(topic)!.partitions[partition!].leader

        let destination = messagesByDestination.get(leader)
        if (!destination) {
          destination = []
          messagesByDestination.set(leader, destination)
        }

        if (messagesPerPartition) {
          let topicMessages = messagesPerPartition.get(topic)
          if (!topicMessages) {
            topicMessages = new Map()
            messagesPerPartition.set(topic, topicMessages)
          }

          topicMessages.set(partition!, (topicMessages.get(partition!) ?? 0) + 1)
        }

        destination.push(message)
      }

      if (messagesPerPartition) {
        const sequenceSnapshot = new NumericMap()

        for (const [topic, topicMessages] of messagesPerPartition) {
          for (const [partition, messagesCount] of topicMessages) {
            const partitionKey = `${topic}:${partition}`
            sequenceSnapshot.set(partitionKey, this.#sequences.postIncrement(partitionKey, messagesCount, 0))
          }
        }

        produceOptions = { ...produceOptions, sequences: sequenceSnapshot }
      }

      this.#produceToDestinations(
        topics,
        messagesByDestination,
        sendOptions,
        produceOptions,
        sendOptions.repeatOnStaleMetadata,
        timedOutCheckOf(callback),
        {},
        (error, results) => {
          this.#metricsProducedMessages?.inc(messages.length)

          if (error) {
            if (sendOptions.idempotent && !this.#transaction) {
              this.#producerInfo = undefined
              this.#sequences.clear()
            }

            ;(error as GenericError).produced = results
            callback(error)
          } else {
            callback(null, results)
          }
        }
      )
    })
  }

  /*
    Idempotent sends always use this queue, even when orderedSends is disabled.

    A request which fails can be sent again, and by then a later request for the same partition might
    have been written already: it was on the wire before the failure was known. To keep the messages of a
    partition in the order they were sent, an operation only starts once the earlier ones which write to
    any of its partitions have completed, retries included. Different partitions are not affected.

    Calls which have to wait are added to the operation which is already waiting, when there is one and
    this does not change the order of any partition. They are then produced in the same request, so a
    partition which receives many concurrent calls needs few requests rather than one for each call.
  */
  #performOrderedSend (
    topics: string[],
    messages: MessageRecord[],
    sendOptions: Required<SendOptions<Key, Value, HeaderKey, HeaderValue>>,
    produceOptions: Partial<CreateRecordsBatchOptions>,
    callback: CallbackWithPromise<ProduceResult>
  ): void {
    // Get the metadata with the topic/partitions informations
    this[kMetadata]({ topics, autocreateTopics: sendOptions.autocreateTopics }, (error, metadata) => {
      if (error) {
        callback(error)
        return
      }

      /*
        Normalize the partition of all messages and look for an operation which is waiting on one of them.

        The call can join it when, on each of its partitions, that operation is the last one or it can
        become the last one: nothing is there, or what is there has started already and so does not wait
        for anything. Otherwise the messages would be written before the ones of an earlier call, or two
        operations could end up waiting for each other.
      */
      let waiting: OrderedSend | undefined
      let joinable = true
      let lastTopic: string | undefined
      let lastPartition = -1
      let topicTails: Map<number, OrderedSend> | undefined

      for (const message of messages) {
        message.partition! %= metadata!.topics.get(message.topic)!.partitionsCount

        const { topic, partition } = message

        if (topic !== lastTopic) {
          lastTopic = topic
          lastPartition = -1
          topicTails = this.#partitionTails.get(topic)
        }

        if (partition !== lastPartition) {
          lastPartition = partition!

          const tail = topicTails?.get(partition!)

          if (tail && !tail.started) {
            if (!waiting) {
              waiting = tail
            } else if (tail !== waiting) {
              joinable = false
            }
          }
        }
      }

      let key: string | undefined
      let bytes = 0

      if (waiting) {
        if (joinable) {
          key = this.#sendKey(sendOptions, produceOptions)
          joinable = waiting.key === key
        }

        if (joinable) {
          bytes = messagesBytes(messages)
          joinable = waiting.bytes + bytes <= Math.max(this.#batchMaxBytes, maxWaitingBytes)
        }
      }

      let operation: OrderedSend
      const joined = joinable && typeof waiting !== 'undefined'

      if (joined) {
        operation = waiting!

        // The messages of the first call are not copied until another call joins
        if (operation.sends.length === 1) {
          operation.messages = operation.messages.slice()
          operation.topics = operation.topics.slice()
        }

        for (const topic of topics) {
          if (!operation.topics.includes(topic)) {
            operation.topics.push(topic)
          }
        }

        operation.bytes += bytes
        operation.sends.push({ count: messages.length, callback })
      } else {
        operation = {
          pending: 0,
          started: false,
          key: '',
          bytes: 0,
          topics,
          messages,
          sends: [{ count: messages.length, callback }],
          tailTopics: [],
          tailPartitions: [],
          waiters: undefined,
          sendOptions,
          produceOptions
        }
      }

      // Make the operation the last one of each partition, after whatever is there
      lastTopic = undefined
      lastPartition = -1

      for (const message of messages) {
        const { topic, partition } = message

        if (joined) {
          operation.messages.push(message)
        }

        if (topic !== lastTopic) {
          lastTopic = topic
          lastPartition = -1
          topicTails = this.#partitionTails.get(topic)

          if (!topicTails) {
            topicTails = new Map()
            this.#partitionTails.set(topic, topicTails)
          }
        }

        if (partition !== lastPartition) {
          lastPartition = partition!

          const tail = topicTails!.get(partition!)

          if (tail !== operation) {
            if (tail) {
              operation.pending++
              tail.waiters ??= []
              tail.waiters.push(operation)
            }

            topicTails!.set(partition!, operation)
            operation.tailTopics.push(topic)
            operation.tailPartitions.push(partition!)
          }
        }
      }

      if (joined) {
        return
      }

      if (operation.pending === 0) {
        this.#startOrderedSend(operation, metadata!)
      } else {
        operation.key = key ?? this.#sendKey(sendOptions, produceOptions)
        operation.bytes = messagesBytes(messages)
      }
    })
  }

  /*
    Only calls made with the same settings can share a produce request.
    The producer ID is not part of them when the producer manages it, as it is read when the request is sent.
  */
  #sendKey (
    sendOptions: Required<SendOptions<Key, Value, HeaderKey, HeaderValue>>,
    produceOptions: Partial<CreateRecordsBatchOptions>
  ): string {
    const { producerId, producerEpoch, transactionalId } = produceOptions
    const producer =
      sendOptions.idempotent && !transactionalId ? 'managed' : `${producerId}:${producerEpoch}:${transactionalId}`

    return `${sendOptions.acks}:${produceOptions.compression}:${sendOptions.idempotent}:${sendOptions.autocreateTopics}:${sendOptions.repeatOnStaleMetadata}:${producer}`
  }

  /*
    Sends an operation whose turn has come.

    What might have changed while the operation was waiting is read now: the leaders of the partitions and,
    for an idempotent producer, its ID, epoch and sequences, which are reset when an earlier request fails.
  */
  #startOrderedSend (operation: OrderedSend, metadata?: ClusterMetadata): void {
    // No other call can join from now on
    operation.started = true

    /*
      Nothing is reserved, such as sequences, for calls which have all failed with their sendTimeout.
      The operation completes on the next tick, as a long queue of such operations would otherwise exhaust the stack.
    */
    if (sendsTimedOutCheck(operation.sends)?.()) {
      process.nextTick(() => this.#completeOrderedSend(operation, new TimeoutError(abandonedMessage)))
      return
    }

    const { sendOptions, produceOptions } = operation

    if (!metadata) {
      this[kMetadata](
        { topics: operation.topics, autocreateTopics: sendOptions.autocreateTopics },
        (error, metadata) => {
          if (error) {
            this.#completeOrderedSend(operation, error)
            return
          }

          this.#startOrderedSend(operation, metadata!)
        }
      )

      return
    }

    if (sendOptions.idempotent && !produceOptions.transactionalId) {
      const producerInfo = this.#producerInfo

      if (!producerInfo) {
        this.initIdempotentProducer({}, error => {
          if (error) {
            this.#completeOrderedSend(operation, error)
            return
          }

          this.#startOrderedSend(operation, metadata)
        })

        return
      }

      if (
        produceOptions.producerId !== producerInfo.producerId ||
        produceOptions.producerEpoch !== producerInfo.producerEpoch
      ) {
        operation.produceOptions = {
          ...produceOptions,
          producerId: producerInfo.producerId,
          producerEpoch: producerInfo.producerEpoch
        }
      }
    }

    this.#produceOrderedSend(operation, metadata)
  }

  #produceOrderedSend (operation: OrderedSend, metadata: ClusterMetadata): void {
    const { topics, messages, sendOptions } = operation
    let produceOptions = operation.produceOptions

    const messagesByDestination = new Map<number, MessageRecord[]>()

    /*
      Track the number of messages per partition so we can update the sequence number.
      Sequences only exist for idempotent producers, so nothing is tracked otherwise. The count is
      kept by topic and then by partition so that no key has to be built for each message.
    */
    const messagesPerPartition = produceOptions.sequences ? new Map<string, Map<number, number>>() : undefined

    // Enqueue the messages to their destination
    for (const message of messages) {
      const { topic, partition } = message
      const leader = metadata.topics.get(topic)?.partitions[partition!]?.leader

      if (typeof leader === 'undefined') {
        this.#completeOrderedSend(
          operation,
          new UserError(`Partition ${partition} of topic ${topic} is not available anymore.`)
        )

        return
      }

      let destination = messagesByDestination.get(leader)
      if (!destination) {
        destination = []
        messagesByDestination.set(leader, destination)
      }

      if (messagesPerPartition) {
        let topicMessages = messagesPerPartition.get(topic)
        if (!topicMessages) {
          topicMessages = new Map()
          messagesPerPartition.set(topic, topicMessages)
        }

        topicMessages.set(partition!, (topicMessages.get(partition!) ?? 0) + 1)
      }

      destination.push(message)
    }

    if (messagesPerPartition) {
      const sequenceSnapshot = new NumericMap()

      for (const [topic, topicMessages] of messagesPerPartition) {
        for (const [partition, messagesCount] of topicMessages) {
          const partitionKey = `${topic}:${partition}`
          sequenceSnapshot.set(partitionKey, this.#sequences.postIncrement(partitionKey, messagesCount, 0))
        }
      }

      produceOptions = { ...produceOptions, sequences: sequenceSnapshot }
    }

    this.#produceToDestinations(
      topics,
      messagesByDestination,
      sendOptions,
      produceOptions,
      sendOptions.repeatOnStaleMetadata,
      sendsTimedOutCheck(operation.sends),
      {},
      (error, results) => {
        this.#metricsProducedMessages?.inc(messages.length)

        if (error) {
          if (sendOptions.idempotent) {
            this.#invalidateProducer(produceOptions)
          }

          ;(error as GenericError).produced = results
          this.#completeOrderedSend(operation, error)
        } else {
          this.#completeOrderedSend(operation, null, results)
        }
      }
    )
  }

  #completeOrderedSend (operation: OrderedSend, error: Error | null, result?: ProduceResult): void {
    const { tailTopics, tailPartitions, waiters } = operation

    // Nothing is kept for the partitions which have no other operation
    for (let i = 0; i < tailTopics.length; i++) {
      const topicTails = this.#partitionTails.get(tailTopics[i])

      if (topicTails?.get(tailPartitions[i]) === operation) {
        topicTails.delete(tailPartitions[i])

        if (topicTails.size === 0) {
          this.#partitionTails.delete(tailTopics[i])
        }
      }
    }

    // Let the next operations go first, they were made before whatever the callbacks do
    if (waiters) {
      for (const waiter of waiters) {
        if (--waiter.pending === 0) {
          this.#startOrderedSend(waiter)
        }
      }
    }

    this.#notifySends(operation.messages, operation.sends, error, result)
  }

  /*
    Produces the messages of each broker at the same time.

    When a partition failed because its leader has moved, the messages which were not appended are grouped again by
    their current leader once every broker has answered, and produced again. This is done once, when
    repeatOnStaleMetadata is enabled.
  */
  #produceToDestinations (
    topics: string[],
    messagesByDestination: Map<number, MessageRecord[]>,
    sendOptions: Required<SendOptions<Key, Value, HeaderKey, HeaderValue>>,
    produceOptions: Partial<CreateRecordsBatchOptions>,
    repeatOnStaleMetadata: boolean,
    timedOut: TimedOutCheck | undefined,
    results: ProduceResult,
    callback: (error: Error | null, results: ProduceResult) => void
  ): void {
    const destinations: DestinationSend[] = []

    runConcurrentCallbacks(
      'Producing messages failed.',
      messagesByDestination,
      ([node, messages], concurrentCallback) => {
        const destination: DestinationSend = { node, messages, produced: [] }
        destinations.push(destination)

        this.#performSingleDestinationSend(
          topics,
          destination,
          this[kOptions].timeout!,
          sendOptions.acks,
          sendOptions.autocreateTopics,
          repeatOnStaleMetadata,
          produceOptions,
          true,
          timedOut,
          concurrentCallback
        )
      },
      (error, apiResults) => {
        const repeat = repeatOnStaleMetadata && !!findErrorBy(error, 'hasStaleMetadata', true)
        const remaining: MessageRecord[] = []

        for (let i = 0; i < destinations.length; i++) {
          const { node, messages, produced } = destinations[i]
          const failed = typeof apiResults![i] === 'undefined'

          if (sendOptions.acks === ProduceAcks.NO_RESPONSE) {
            results.unwritableNodes ??= []

            if ((failed && !repeat) || apiResults![i] === false) {
              results.unwritableNodes.push(node)
            }
          } else {
            results.offsets ??= []

            for (const { responses } of produced) {
              for (const { name, partitionResponses } of responses) {
                for (const partitionResponse of partitionResponses) {
                  results.offsets.push({
                    topic: name,
                    partition: partitionResponse.index,
                    offset: partitionResponse.baseOffset
                  })
                }
              }
            }
          }

          if (failed && repeat) {
            for (const message of messages) {
              remaining.push(message)
            }
          }
        }

        if (!repeat) {
          callback(error, results)
          return
        }

        this.clearMetadata()
        this[kMetadata]({ topics, autocreateTopics: sendOptions.autocreateTopics }, (error, metadata) => {
          if (error) {
            callback(error, results)
            return
          }

          const messagesByLeader = new Map<number, MessageRecord[]>()

          for (const message of remaining) {
            const leader = metadata!.topics.get(message.topic)!.partitions[message.partition!].leader

            let leaderMessages = messagesByLeader.get(leader)
            if (!leaderMessages) {
              leaderMessages = []
              messagesByLeader.set(leader, leaderMessages)
            }

            leaderMessages.push(message)
          }

          this.#produceToDestinations(
            topics,
            messagesByLeader,
            sendOptions,
            produceOptions,
            false,
            timedOut,
            results,
            (error, results) => {
              // Report the partitions in the order they were first sent in, as when no leader moved
              const positions = new Map<string, number>()

              for (const messages of messagesByDestination.values()) {
                for (const { topic, partition } of messages) {
                  const key = `${topic}:${partition}`

                  if (!positions.has(key)) {
                    positions.set(key, positions.size)
                  }
                }
              }

              results.offsets?.sort(
                (a, b) => positions.get(`${a.topic}:${a.partition}`)! - positions.get(`${b.topic}:${b.partition}`)!
              )

              callback(error, results)
            }
          )
        })
      }
    )
  }

  #performSingleDestinationSend (
    topics: string[],
    destination: DestinationSend,
    timeout: number,
    acks: number,
    autocreateTopics: boolean,
    repeatOnStaleMetadata: boolean,
    produceOptions: Partial<CreateRecordsBatchOptions>,
    recoverOnProducerStateLoss: boolean,
    timedOut: TimedOutCheck | undefined,
    callback: CallbackWithPromise<boolean | ProduceResponse>
  ): void {
    let refreshMetadata = false

    this[kPerformWithRetry]<boolean | ProduceResponse>(
      'produce',
      retryCallback => {
        // No attempt starts for calls which have all failed with their sendTimeout
        if (timedOut?.()) {
          retryCallback(new TimeoutError(abandonedMessage))
          return
        }

        this[kMetadata]({ topics, autocreateTopics, forceUpdate: refreshMetadata, retries: 0 }, (error, metadata) => {
          if (error) {
            refreshMetadata = NetworkError.isRetryable(error)
            retryCallback(error)
            return
          }

          refreshMetadata = false
          const { topic, partition } = destination.messages[0]
          const leader = metadata!.topics.get(topic)!.partitions[partition!].leader

          this[kGetConnection](metadata!.brokers.get(leader)!, (error, connection) => {
            if (error) {
              refreshMetadata = NetworkError.isRetryable(error)
              retryCallback(error)
              return
            }

            this[kGetApi]<ProduceRequest, ProduceResponse>('Produce', (error, api) => {
              if (error) {
                refreshMetadata = NetworkError.isRetryable(error)
                retryCallback(error)
                return
              }

              // Nor is anything written when they have while the attempt was getting there
              if (timedOut?.()) {
                retryCallback(new TimeoutError(abandonedMessage))
                return
              }

              api!(connection!, acks, timeout, destination.messages, produceOptions, (error, response) => {
                if (acks !== ProduceAcks.NO_RESPONSE) {
                  this.#acknowledge(destination, error, response as ProduceResponse)
                }

                refreshMetadata = NetworkError.isRetryable(error)
                retryCallback(error, response)
              })
            })
          })
        })
      },
      (error, results) => {
        if (error) {
          // Only the head of a partition's queue can reach the broker, so a sequence refusal is a real gap.
          if (
            recoverOnProducerStateLoss &&
            !timedOut?.() &&
            produceOptions.sequences &&
            !this.#transaction &&
            this.#isProducerStateLost(error)
          ) {
            this.#recoverIdempotentProducer(destination.messages, produceOptions, (error, recoveredProduceOptions) => {
              if (error) {
                callback(error)
                return
              }

              this.#performSingleDestinationSend(
                topics,
                destination,
                timeout,
                acks,
                autocreateTopics,
                repeatOnStaleMetadata,
                recoveredProduceOptions!,
                false,
                timedOut,
                (error, results) => {
                  if (error) {
                    this.#invalidateProducer(recoveredProduceOptions!)
                  }

                  callback(error, results)
                }
              )
            })
            return
          }

          callback(error, results)
          return
        }

        callback(error, results)
      },
      0,
      [],
      error => {
        // There is nobody left to retry for
        if (timedOut?.()) {
          return true
        }

        /* c8 ignore next 3 - Hard to test */
        if (!repeatOnStaleMetadata) {
          return false
        }

        return !!findErrorBy(error, 'hasStaleMetadata', true)
      }
    )
  }

  /*
    When only some partitions of a request fail, the broker has appended the others, and the ResponseError carries the
    response which says so. Those partitions are recorded and removed from the messages, so a retry does not write
    them again.
  */
  #acknowledge (destination: DestinationSend, error: Error | null, response: ProduceResponse | undefined): void {
    if (!error) {
      destination.produced.push(response!)
      return
    }

    const failedResponse: ProduceResponse | undefined = findErrorBy(error, 'code', 'PLT_KFK_RESPONSE')?.response
    const responses: ProduceResponseTopic[] = []
    const appended = new Set<string>()

    for (const { name, partitionResponses } of failedResponse?.responses ?? []) {
      const succeeded = partitionResponses.filter(partitionResponse => partitionResponse.errorCode === 0)

      if (succeeded.length > 0) {
        responses.push({ name, partitionResponses: succeeded })

        for (const { index } of succeeded) {
          appended.add(`${name}:${index}`)
        }
      }
    }

    if (responses.length === 0) {
      return
    }

    destination.produced.push({ ...failedResponse!, responses })
    destination.messages = destination.messages.filter(
      message => !appended.has(`${message.topic}:${message.partition}`)
    )
  }

  #isProducerStateLost (error: Error): boolean {
    return !!(
      findErrorBy(error, 'apiId', 'UNKNOWN_PRODUCER_ID') || findErrorBy(error, 'apiId', 'OUT_OF_ORDER_SEQUENCE_NUMBER')
    )
  }

  #invalidateProducer (options: Partial<CreateRecordsBatchOptions>): void {
    // A late failure from another partition must not invalidate the recovered session.
    const current = this.#producerInfo
    if (
      !this.#transaction &&
      current &&
      current.producerId === options.producerId &&
      current.producerEpoch === options.producerEpoch
    ) {
      this.#producerInfo = undefined
      this.#sequences.clear()
    }
  }

  #recoverIdempotentProducer (
    messages: MessageRecord[],
    produceOptions: Partial<CreateRecordsBatchOptions>,
    callback: CallbackWithPromise<Partial<CreateRecordsBatchOptions>>
  ): void {
    const resend: CallbackWithPromise<ProducerInfo> = (error, producerInfo) => {
      if (error) {
        callback(error)
        return
      }

      const sequences = new NumericMap()
      const messagesPerPartition = new NumericMap()

      for (const { topic, partition } of messages) {
        messagesPerPartition.postIncrement(`${topic}:${partition}`, 1, 0)
      }

      for (const [partitionKey, messagesCount] of messagesPerPartition) {
        sequences.set(partitionKey, this.#sequences.postIncrement(partitionKey, messagesCount, 0))
      }

      callback(null, {
        ...produceOptions,
        producerId: producerInfo!.producerId,
        producerEpoch: producerInfo!.producerEpoch,
        sequences
      })
    }

    // Another partition may already have recovered from the same loss of producer state.
    const current = this.#producerInfo
    if (
      current &&
      (current.producerId !== produceOptions.producerId || current.producerEpoch !== produceOptions.producerEpoch)
    ) {
      resend(null, current)
      return
    }

    this.#producerInfo = undefined
    this.initIdempotentProducer(
      { producerId: produceOptions.producerId, producerEpoch: produceOptions.producerEpoch },
      resend
    )
  }

  #getCoordinatorConnection (callback: CallbackWithPromise<Connection>): void {
    if (this.#coordinatorId === undefined) {
      this[kTransactionFindCoordinator](error => {
        if (error) {
          callback(error)
          return
        }

        this.#getCoordinatorConnection(callback)
      })
      return
    }

    // Get a connection to the coordinator
    this[kMetadata]({}, (error, metadata) => {
      if (error) {
        callback(error)
        return
      }

      this[kGetConnection](metadata!.brokers.get(this.#coordinatorId!)!, (error, connection) => {
        if (error) {
          callback(this.#handleError(error))
          return
        }

        callback(null, connection)
      })
    })
  }

  #handleFencingError (error: Error): void {
    if (findErrorBy<ProtocolError>(error, 'producerFenced', true)) {
      this.#producerInfo = undefined
      this.#transaction = undefined
    }
  }

  #handleError (error: Error | null): Error | null {
    if (
      findErrorBy(error, 'code', 'PLT_KFK_NETWORK') ||
      findErrorBy(error, 'apiId', 'COORDINATOR_LOAD_IN_PROGRESS') ||
      findErrorBy(error, 'apiId', 'COORDINATOR_NOT_AVAILABLE') ||
      findErrorBy(error, 'apiId', 'NOT_COORDINATOR')
    ) {
      this.#coordinatorId = undefined
    }

    return error
  }

  /*
    Applications use a handful of header keys, which are the same for every message. When they are serialized
    with the string serializer of this package the result only depends on the key, so it is kept. Nothing can be
    assumed for other serializers, which also receive the metadata of the message.
  */
  #serializeHeaderKey (key: HeaderKey, metadata: unknown): Buffer {
    if (this.#headerKeySerializer !== (stringSerializer as Serializer<HeaderKey>) || typeof key !== 'string') {
      return this.#headerKeySerializer(key, metadata)!
    }

    let serialized = this.#serializedHeaderKeys.get(key)

    if (!serialized) {
      serialized = Buffer.from(key, 'utf-8')

      if (this.#serializedHeaderKeys.size < maxSerializedHeaderKeys) {
        this.#serializedHeaderKeys.set(key, serialized)
      }
    }

    return serialized
  }

  #beforeSerialization (
    hook: BeforeSerializationHook<Key, Value, HeaderKey, HeaderValue>,
    options: SendOptions<Key, Value, HeaderKey, HeaderValue>,
    callback: CallbackWithPromise<ProduceResult>
  ): void {
    // Create the pre-serialization requests
    const requests: [unknown, BeforeHookPayloadType, MessageToProduce<Key, Value, HeaderKey, HeaderValue>][] = []

    for (const message of options.messages) {
      requests.push([message.key, 'key', message])
      requests.push([message.value, 'value', message])

      if (typeof message.headers !== 'undefined') {
        const headers =
          message.headers instanceof Map
            ? (message.headers as Map<HeaderKey, HeaderValue>)
            : new Map(Object.entries(message.headers) as [HeaderKey, HeaderValue][])

        for (const [headerKey, headerValue] of headers) {
          requests.push([headerKey, 'headerKey', message])
          requests.push([headerValue, 'headerValue', message])
        }
      }
    }

    if (requests.length === 0) {
      this.#send(options, callback)
      return
    }

    runAsyncSeries(
      (request, cb) => {
        const [data, type, message] = request

        const result = hook(data, type, message, cb) as Promise<void>

        if (typeof result?.then === 'function') {
          result.then(() => cb(null), cb)
        }
      },
      requests,
      0,
      error => {
        if (error) {
          callback(error)
          return
        }

        this.#send(options, callback)
      }
    )
  }

  #assignPartition (
    message: MessageToProduce<Key, Value, HeaderKey, HeaderValue>,
    partitioner: Partitioner<Key, Value, HeaderKey, HeaderValue> | undefined,
    key: Buffer<ArrayBufferLike> | undefined,
    topic: string
  ) {
    let partition: number = 0

    if (typeof message.partition !== 'number') {
      if (partitioner) {
        partition = partitioner(message, key, { metadata: this.currentMetadata })
      } else if (key) {
        partition = defaultPartitioner(message, key)
      } else {
        // Use the roundrobin
        partition = this.#partitionsRoundRobin.postIncrement(topic, 1, 0)
      }
    } else {
      partition = message.partition
    }

    return partition
  }
}
