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
import { type GenericError, type ProtocolError, UserError } from '../../errors.ts'
import { type Connection } from '../../network/connection.ts'
import { murmur2 } from '../../protocol/murmur2.ts'
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
  type SerializerWithHeaders
} from '../serde.ts'
import {
  produceOptionsValidator,
  producerOptionsValidator,
  producerStreamOptionsValidator,
  sendOptionsValidator
} from './options.ts'
import { ProducerStream } from './producer-stream.ts'
import { Transaction } from './transaction.ts'
import {
  type ProduceOptions,
  type ProduceResult,
  type ProducerInfo,
  type ProducerOptions,
  type ProducerStreamOptions,
  type SendOptions
} from './types.ts'

// Don't move this function as being in the same file will enable V8 to remove.
// For futher info, ask Matteo.
export function noopSerializer (data?: Buffer): Buffer | undefined {
  return data
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
  #metricsProducedMessages: Counter | undefined
  #coordinatorId!: number
  #transaction: Transaction<Key, Value, HeaderKey, HeaderValue> | undefined
  #streams: Set<ProducerStream<Key, Value, HeaderKey, HeaderValue>>
  #sendOperation: (
    options: SendOptions<Key, Value, HeaderKey, HeaderValue>,
    callback: CallbackWithPromise<ProduceResult>
  ) => void

  constructor (options: ProducerOptions<Key, Value, HeaderKey, HeaderValue>) {
    if (options.idempotent) {
      options.maxInflights = 1
      options.acks = ProduceAcks.ALL
      options.retries = Number.MAX_SAFE_INTEGER
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
    this.#streams = new Set()
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

  get coordinatorId (): number {
    return this.#coordinatorId
  }

  get streamsCount (): number {
    return this.#streams.size
  }

  close (force: boolean | CallbackWithPromise<void>, callback?: CallbackWithPromise<void>): void
  close (force?: boolean): Promise<void>
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
          retryCallback => {
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

                api!(connection!, FindCoordinatorKeyTypes.TRANSACTION, [transactionalId], retryCallback)
              })
            })
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
                retryCallback(error)
                return
              }

              this[kGetApi]<AddPartitionsToTxnRequest, AddOffsetsToTxnResponse>('AddPartitionsToTxn', (error, api) => {
                if (error) {
                  retryCallback(error)
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
                      retryCallback(error)
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
                retryCallback(error)
                return
              }

              this[kGetApi]<AddOffsetsToTxnRequest, AddOffsetsToTxnResponse>('AddOffsetsToTxn', (error, api) => {
                if (error) {
                  retryCallback(error)
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
                      retryCallback(error)
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
                retryCallback(error)
                return
              }

              this[kGetApi]<EndTxnRequest, EndTxnResponse>('EndTxn', (error, api) => {
                if (error) {
                  retryCallback(error)
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
                      retryCallback(error)
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
          retryCallback => {
            const connector = transactionalId
              ? this.#getCoordinatorConnection.bind(this)
              : this[kGetBootstrapConnection].bind(this)

            connector((error, connection) => {
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
                  retryCallback
                )
              })
            })
          },
          (error, response) => {
            if (error) {
              this.#handleFencingError(error)
              deduplicateCallback(error)
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
      let headers = new Map<HeaderKey, HeaderValue>()
      const serializedHeaders = new Map<Buffer, Buffer>()
      const metadata = message.metadata

      try {
        if (message.headers) {
          headers =
            message.headers instanceof Map
              ? (message.headers as Map<HeaderKey, HeaderValue>)
              : new Map(Object.entries(message.headers) as [HeaderKey, HeaderValue][])

          for (const [key, value] of headers) {
            serializedHeaders.set(
              this.#headerKeySerializer(key as HeaderKey, metadata)!,
              this.#headerValueSerializer(value, metadata)!
            )
          }
        }

        key = this.#keySerializer(message.key, headers, message)
        value = this.#valueSerializer(message.value, headers, message)!
      } catch (error) {
        callback(new UserError('Failed to serialize a message.', { cause: error }))
        return
      }

      let partition: number = 0

      if (typeof message.partition !== 'number') {
        if (partitioner) {
          partition = partitioner(message)
        } else if (key) {
          partition = murmur2(key) >>> 0
        } else {
          // Use the roundrobin
          partition = this.#partitionsRoundRobin.postIncrement(topic, 1, 0)
        }
      } else {
        partition = message.partition
      }

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

  #performSend (
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

      const messagesByDestination = new Map<number, MessageRecord[]>()

      // Track the number of messages per partition so we can update the sequence number
      const messagesPerPartition = new NumericMap()

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

        const messagePartitionKey = `${message.topic}:${partition}`
        messagesPerPartition.postIncrement(messagePartitionKey, 1, 0)
        destination.push(message)
      }

      // Track nodes so that we can get their ID for delayed write reporting
      const nodes: number[] = []

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
            concurrentCallback
          )
        },
        (error, apiResults) => {
          if (error) {
            callback(error)
            return
          }

          this.#metricsProducedMessages?.inc(messages.length)
          const results: ProduceResult = {}

          if (sendOptions.acks === ProduceAcks.NO_RESPONSE) {
            const unwritableNodes = []

            for (let i = 0; i < apiResults!.length; i++) {
              if (apiResults![i] === false) {
                unwritableNodes.push(nodes[i])
              }
            }

            results.unwritableNodes = unwritableNodes
          } else {
            const topics: ProduceResult['offsets'] = []

            for (const result of apiResults!) {
              for (const { name, partitionResponses } of (result as ProduceResponse).responses) {
                for (const partitionResponse of partitionResponses) {
                  topics.push({
                    topic: name,
                    partition: partitionResponse.index,
                    offset: partitionResponse.baseOffset
                  })

                  const partitionKey = `${name}:${partitionResponse.index}`
                  this.#sequences.postIncrement(partitionKey, messagesPerPartition.get(partitionKey)!, 0)
                }
              }

              results.offsets = topics
            }
          }

          callback(null, results)
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
    callback: CallbackWithPromise<boolean | ProduceResponse>
  ): void {
    // Get the metadata with the topic/partitions informations
    this[kMetadata]({ topics, autocreateTopics }, (error, metadata) => {
      if (error) {
        callback(error)
        return
      }

      const { topic, partition } = messages[0]
      const leader = metadata!.topics.get(topic)!.partitions[partition!].leader

      this[kPerformWithRetry]<boolean | ProduceResponse>(
        'produce',
        retryCallback => {
          this[kGetConnection](metadata!.brokers.get(leader)!, (error, connection) => {
            if (error) {
              retryCallback(error)
              return
            }

            this[kGetApi]<ProduceRequest, ProduceResponse>('Produce', (error, api) => {
              if (error) {
                retryCallback(error)
                return
              }

              api!(connection!, acks, timeout, messages, produceOptions, retryCallback)
            })
          })
        },
        (error, results) => {
          if (error) {
            // If the last error was due to stale metadata, we retry the operation with this set of messages
            // since the partition is already set, it should attempt on the new destination
            const hasStaleMetadata = (error as GenericError).findBy('hasStaleMetadata', true)

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
                callback
              )
              return
            }

            callback(error)
            return
          }

          callback(error, results)
        },
        0,
        [],
        error => {
          return repeatOnStaleMetadata && !!(error as GenericError).findBy('hasStaleMetadata', true)
        }
      )
    })
  }

  #getCoordinatorConnection (callback: CallbackWithPromise<Connection>): void {
    // Get a connection to the coordinator
    this[kMetadata]({}, (error, metadata) => {
      if (error) {
        callback(error)
        return
      }

      this[kPerformWithRetry](
        'getCoordinatorConnection',
        retryCallback => {
          this[kGetConnection](metadata!.brokers.get(this.#coordinatorId)!, (error, connection) => {
            if (error) {
              retryCallback(error)
              return
            }

            retryCallback(null, connection)
          })
        },
        callback
      )
    })
  }

  #handleFencingError (error: Error): void {
    if ((error as GenericError).findBy<ProtocolError>?.('producerFenced', true)) {
      this.#producerInfo = undefined
      this.#transaction = undefined
    }
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
}
