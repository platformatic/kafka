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
import { type ProduceRequest, type ProduceResponse } from '../../apis/producer/produce-v11.ts'
import { type TxnOffsetCommitRequest, type TxnOffsetCommitResponse } from '../../apis/producer/txn-offset-commit-v4.ts'
import {
  createDiagnosticContext,
  producerInitIdempotentChannel,
  producerSendsChannel,
  producerTransactionsChannel
} from '../../diagnostic.ts'
import { findErrorBy, type GenericError, NetworkError, type ProtocolError, TimeoutError, UserError } from '../../errors.ts'
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
interface PendingSends {
  key: string
  topics: Set<string>
  messages: MessageRecord[]
  // For each send(), how many of the messages above belong to it and who to notify
  sends: { count: number, callback: CallbackWithPromise<ProduceResult> }[]
  bytes: number
  sendOptions: Required<SendOptions<any, any, any, any>>
  produceOptions: Partial<CreateRecordsBatchOptions>
  // The earliest delivery deadline of the sends above, if any has one
  deadline: number | undefined
  timer: NodeJS.Timeout
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

    options.acks ??= idempotent ? ProduceAcks.ALL : ProduceAcks.LEADER

    producerSendsChannel.traceCallback(
      this.#sendOperation,
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
                  options.producerId ?? this[kOptions].producerId ?? 0n,
                  options.producerEpoch ?? this[kOptions].producerEpoch ?? 0,
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

  #send (
    options: SendOptions<Key, Value, HeaderKey, HeaderValue>,
    callback: CallbackWithPromise<ProduceResult>,
    deadline: number | undefined = this.#deliveryDeadline(options)
  ): void {
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

          this.#send(options, callback, deadline)
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
          callback,
          deadline
        )

        return
      }
    }

    if (this.#transaction) {
      // Get the metadata for topics, we need to normalize the partitions
      this[kMetadata]({ topics: topicsArray, autocreateTopics: options.autocreateTopics, deadline }, (error, metadata) => {
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
            callback,
            deadline
          )
        })
      })
    } else {
      this.#performSend(
        topicsArray,
        messages,
        options as Required<SendOptions<Key, Value, HeaderKey, HeaderValue>>,
        produceOptions,
        callback,
        deadline
      )
    }
  }

  // When the messages of a send() must be produced by, as a Date.now() timestamp
  #deliveryDeadline (options: SendOptions<Key, Value, HeaderKey, HeaderValue>): number | undefined {
    const deliveryTimeout = options.deliveryTimeout ?? this[kOptions].deliveryTimeout
    return typeof deliveryTimeout === 'number' ? Date.now() + deliveryTimeout : undefined
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
    callback: CallbackWithPromise<ProduceResult>,
    deadline: number | undefined
  ): void {
    const key = `${sendOptions.acks}:${produceOptions.compression}:${sendOptions.idempotent}:${sendOptions.autocreateTopics}:${sendOptions.repeatOnStaleMetadata}`

    let bytes = 0

    for (const message of messages) {
      bytes += (message.key?.length ?? 0) + (message.value?.length ?? 0)

      if (message.headers) {
        headersBytes = 0
        message.headers.forEach(addHeaderBytes)
        bytes += headersBytes
      }
    }

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
        deadline,
        timer: setTimeout(() => this.#flushPendingSends(), this.#linger)
      }
    } else if (typeof deadline === 'number') {
      pending.deadline = typeof pending.deadline === 'number' ? Math.min(pending.deadline, deadline) : deadline
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

    this.#performSend(
      Array.from(pending.topics),
      messages,
      pending.sendOptions as Required<SendOptions<Key, Value, HeaderKey, HeaderValue>>,
      pending.produceOptions,
      (error, result) => {
        this.#lingerInflight--

        // What has been waiting for this request is sent before the calls are notified, as they might send more
        if (this.#lingerInflight === 0 && this.#pendingSends && this.#pendingSends.bytes >= this.#batchMaxBytes) {
          this.#flushPendingSends()
        }

        // A single send() gets exactly what it would have got without lingering
        if (sends.length === 1) {
          sends[0].callback(error, result)
          onComplete?.()
          return
        }

        if (error) {
          for (const send of sends) {
            send.callback(error)
          }

          onComplete?.()
          return
        }

        if (!result!.offsets) {
          for (const send of sends) {
            send.callback(null, { unwritableNodes: result!.unwritableNodes?.slice() })
          }

          onComplete?.()
          return
        }

        /*
          The broker returns the offset of the first message of each partition. Messages keep their order
          within a partition, so the offset of each message is that one plus the number of messages which
          precede it in the same partition. Each send() is given the offset of its own first message.
        */
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

        onComplete?.()
      },
      pending.deadline
    )
  }

  #performSend (
    topics: string[],
    messages: MessageRecord[],
    sendOptions: Required<SendOptions<Key, Value, HeaderKey, HeaderValue>>,
    produceOptions: Partial<CreateRecordsBatchOptions>,
    callback: CallbackWithPromise<ProduceResult>,
    deadline: number | undefined
  ): void {
    // Get the metadata with the topic/partitions informations
    this[kMetadata]({ topics, autocreateTopics: sendOptions.autocreateTopics, deadline }, (error, metadata) => {
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

      // Track nodes so that we can get their ID for delayed write reporting
      const nodes: number[] = []
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

      runConcurrentCallbacks(
        'Producing messages failed.',
        messagesByDestination,
        ([destination, destinationMessages], concurrentCallback) => {
          nodes.push(destination)

          this.#performSingleDestinationSend(
            topics,
            destinationMessages,
            this[kOptions].timeout!,
            sendOptions.acks,
            sendOptions.autocreateTopics,
            sendOptions.repeatOnStaleMetadata,
            produceOptions,
            true,
            concurrentCallback,
            deadline
          )
        },
        (error, apiResults) => {
          this.#metricsProducedMessages?.inc(messages.length)
          const results: ProduceResult = {}

          if (sendOptions.acks === ProduceAcks.NO_RESPONSE) {
            const unwritableNodes = []

            for (let i = 0; i < apiResults!.length; i++) {
              if (typeof apiResults![i] === 'undefined' || apiResults![i] === false) {
                unwritableNodes.push(nodes[i])
              }
            }

            results.unwritableNodes = unwritableNodes
          } else {
            const topics: ProduceResult['offsets'] = []

            for (const result of apiResults!) {
              for (const { name, partitionResponses } of (result as ProduceResponse)?.responses ?? []) {
                for (const partitionResponse of partitionResponses) {
                  topics.push({
                    topic: name,
                    partition: partitionResponse.index,
                    offset: partitionResponse.baseOffset
                  })
                }
              }

              results.offsets = topics
            }
          }

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

  #performSingleDestinationSend (
    topics: string[],
    messages: MessageRecord[],
    timeout: number,
    acks: number,
    autocreateTopics: boolean,
    repeatOnStaleMetadata: boolean,
    produceOptions: Partial<CreateRecordsBatchOptions>,
    recoverOnProducerStateLoss: boolean,
    callback: CallbackWithPromise<boolean | ProduceResponse>,
    deadline: number | undefined
  ): void {
    const { topic, partition } = messages[0]
    let refreshMetadata = false

    this[kPerformWithRetry]<boolean | ProduceResponse>(
      'produce',
      retryCallback => {
        this[kMetadata]({ topics, autocreateTopics, forceUpdate: refreshMetadata, retries: 0, deadline }, (error, metadata) => {
          if (error) {
            refreshMetadata = NetworkError.isRetryable(error)
            retryCallback(error)
            return
          }

          refreshMetadata = false
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

              // Nothing is written once the deadline has passed, whatever the time it took to get here
              if (typeof deadline === 'number' && Date.now() >= deadline) {
                retryCallback(new TimeoutError('The delivery timeout expired before the messages could be produced.'))
                return
              }

              api!(connection!, acks, timeout, messages, produceOptions, (error, response) => {
                refreshMetadata = NetworkError.isRetryable(error)
                retryCallback(error, response)
              })
            })
          })
        })
      },
      (error, results) => {
        if (error) {
          // If the last error was due to stale metadata, we retry the operation with this set of messages
          // since the partition is already set, it should attempt on the new destination
          /* c8 ignore next - Hard to test */
          const hasStaleMetadata = findErrorBy(error, 'hasStaleMetadata', true)

          if (hasStaleMetadata && repeatOnStaleMetadata) {
            this.clearMetadata()
            this.#performSingleDestinationSend(
              topics,
              messages,
              timeout,
              acks,
              autocreateTopics,
              false,
              produceOptions,
              recoverOnProducerStateLoss,
              callback,
              deadline
            )
            return
          }

          if (recoverOnProducerStateLoss && !this.#transaction && this.#isProducerStateLost(error)) {
            this.#recoverIdempotentProducer(messages, produceOptions, (error, recoveredProduceOptions) => {
              if (error) {
                callback(error)
                return
              }

              this.#performSingleDestinationSend(
                topics,
                messages,
                timeout,
                acks,
                autocreateTopics,
                repeatOnStaleMetadata,
                recoveredProduceOptions!,
                false,
                callback,
                deadline
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
        /* c8 ignore next 3 - Hard to test */
        if (!repeatOnStaleMetadata) {
          return false
        }

        return !!findErrorBy(error, 'hasStaleMetadata', true)
      },
      undefined,
      undefined,
      deadline
    )
  }

  #isProducerStateLost (error: Error): boolean {
    return !!(
      findErrorBy(error, 'apiId', 'UNKNOWN_PRODUCER_ID') || findErrorBy(error, 'apiId', 'OUT_OF_ORDER_SEQUENCE_NUMBER')
    )
  }

  #recoverIdempotentProducer (
    messages: MessageRecord[],
    produceOptions: Partial<CreateRecordsBatchOptions>,
    callback: CallbackWithPromise<Partial<CreateRecordsBatchOptions>>
  ): void {
    this.#producerInfo = undefined
    this.#sequences.clear()

    this.initIdempotentProducer({}, (error, producerInfo) => {
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
    })
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
