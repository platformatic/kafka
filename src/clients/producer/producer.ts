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
  type MessageRecord
} from '../../protocol/records.ts'
import { NumericMap } from '../../utils.ts'
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
import { type MessagesStream } from '../consumer/messages-stream.ts'
import { type Counter, ensureMetric, type Gauge } from '../metrics.ts'
import { type Serializer, type SerializerWithHeaders } from '../serde.ts'
import { produceOptionsValidator, producerOptionsValidator, sendOptionsValidator } from './options.ts'
import {
  type ProduceOptions,
  type ProduceResult,
  type ProducerInfo,
  type ProducerOptions,
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
  #transactionalId: string | undefined
  #transactionTopicPartitions: Set<string>
  #transactionConsumerGroups: Set<string>

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
    this.#partitionsRoundRobin = new NumericMap()
    this.#sequences = new NumericMap()
    this.#keySerializer = options.serializers?.key ?? (noopSerializer as Serializer<Key>)
    this.#valueSerializer = options.serializers?.value ?? (noopSerializer as Serializer<Value>)
    this.#headerKeySerializer = options.serializers?.headerKey ?? (noopSerializer as Serializer<HeaderKey>)
    this.#headerValueSerializer = options.serializers?.headerValue ?? (noopSerializer as Serializer<HeaderValue>)
    this.#transactionTopicPartitions = new Set()
    this.#transactionConsumerGroups = new Set()

    this[kValidateOptions](options, producerOptionsValidator, '/options')

    if (this[kPrometheus]) {
      ensureMetric<Gauge>(this[kPrometheus], 'Gauge', 'kafka_producers', 'Number of active Kafka producers').inc()

      this.#metricsProducedMessages = ensureMetric<Counter>(
        this[kPrometheus],
        'Counter',
        'kafka_produced_messages',
        'Number of produced Kafka messages'
      )
    }

    this[kAfterCreate]('producer')
  }

  get producerId (): bigint | undefined {
    return this.#producerInfo?.producerId
  }

  get producerEpoch (): number | undefined {
    return this.#producerInfo?.producerEpoch
  }

  get transactionalId (): string | undefined {
    return this.#transactionalId
  }

  get coordinatorId (): number {
    return this.#coordinatorId
  }

  close (callback: CallbackWithPromise<void>): void
  close (): Promise<void>
  close (callback?: CallbackWithPromise<void>): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback<void>()
    }

    if (this[kClosed]) {
      callback(null)
      return callback[kCallbackPromise]
    }

    this[kClosed] = true

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

    return callback[kCallbackPromise]
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
      callback(validationError, undefined as unknown as ProducerInfo)
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

  send (options: SendOptions<Key, Value, HeaderKey, HeaderValue>, callback: CallbackWithPromise<ProduceResult>): void
  send (options: SendOptions<Key, Value, HeaderKey, HeaderValue>): Promise<ProduceResult>
  send (
    options: SendOptions<Key, Value, HeaderKey, HeaderValue>,
    callback?: CallbackWithPromise<ProduceResult>
  ): void | Promise<ProduceResult> {
    if (!callback) {
      callback = createPromisifiedCallback<ProduceResult>()
    }

    if (this[kCheckNotClosed](callback)) {
      return callback[kCallbackPromise]
    }

    const validationError = this[kValidateOptions](options, sendOptionsValidator, '/options', false)
    if (validationError) {
      callback(validationError, undefined as unknown as ProduceResult)
      return callback[kCallbackPromise]
    }

    producerSendsChannel.traceCallback(
      this.#send,
      1,
      createDiagnosticContext({ client: this, operation: 'send', options }),
      this,
      options,
      callback
    )

    return callback[kCallbackPromise]
  }

  beginTransaction (callback: CallbackWithPromise<void>): void
  beginTransaction (): Promise<void>
  beginTransaction (callback?: CallbackWithPromise<void>): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback<void>()
    }

    if (this[kCheckNotClosed](callback)) {
      return callback[kCallbackPromise]
    }

    if (!this[kOptions].idempotent) {
      callback(new UserError('Cannot begin a transaction on a non-idempotent producer.'))
      return callback[kCallbackPromise]
    }

    if (!this[kOptions].transactionalId) {
      callback(new UserError('transactionalId must be set when creating a producer to begin a transaction.'))
      return callback[kCallbackPromise]
    }

    producerTransactionsChannel.traceCallback(
      this.#beginTransaction,
      0,
      createDiagnosticContext({ client: this, operation: 'begin' }),
      this,
      callback
    )

    return callback[kCallbackPromise]
  }

  commitTransaction (callback: CallbackWithPromise<void>): void
  commitTransaction (): Promise<void>
  commitTransaction (callback?: CallbackWithPromise<void>): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback<void>()
    }

    if (this[kCheckNotClosed](callback)) {
      return callback[kCallbackPromise]
    }

    if (!this[kOptions].idempotent) {
      callback(new UserError('Cannot commit a transaction of a non-idempotent producer.'))
      return callback[kCallbackPromise]
    }

    if (!this.#transactionalId) {
      callback(new UserError('No active transaction to commit.'))
      return callback[kCallbackPromise]
    }

    producerTransactionsChannel.traceCallback(
      this.#endTransaction,
      1,
      createDiagnosticContext({ client: this, operation: 'commit' }),
      this,
      true,
      callback
    )

    return callback[kCallbackPromise]
  }

  abortTransaction (callback: CallbackWithPromise<void>): void
  abortTransaction (): Promise<void>
  abortTransaction (callback?: CallbackWithPromise<void>): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback<void>()
    }

    if (this[kCheckNotClosed](callback)) {
      return callback[kCallbackPromise]
    }

    if (!this[kOptions].idempotent) {
      callback(new UserError('Cannot abort a transaction of a non-idempotent producer.'))
      return callback[kCallbackPromise]
    }

    if (!this.#transactionalId) {
      callback(new UserError('No active transaction to abort.'))
      return callback[kCallbackPromise]
    }

    producerTransactionsChannel.traceCallback(
      this.#endTransaction,
      1,
      createDiagnosticContext({ client: this, operation: 'abort' }),
      this,
      false,
      callback
    )

    return callback[kCallbackPromise]
  }

  linkMessageStreamToTransaction<
    LinkKey = Buffer,
    LinkValue = Buffer,
    LinkHeaderKey = Buffer,
    LinkHeaderValue = Buffer
  > (
    stream: MessagesStream<LinkKey, LinkValue, LinkHeaderKey, LinkHeaderValue>,
    callback: CallbackWithPromise<void>
  ): void
  linkMessageStreamToTransaction<
    LinkKey = Buffer,
    LinkValue = Buffer,
    LinkHeaderKey = Buffer,
    LinkHeaderValue = Buffer
  > (stream: MessagesStream<LinkKey, LinkValue, LinkHeaderKey, LinkHeaderValue>): Promise<void>
  linkMessageStreamToTransaction<
    LinkKey = Buffer,
    LinkValue = Buffer,
    LinkHeaderKey = Buffer,
    LinkHeaderValue = Buffer
  > (
    stream: MessagesStream<LinkKey, LinkValue, LinkHeaderKey, LinkHeaderValue>,
    callback?: CallbackWithPromise<void>
  ): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback<void>()
    }

    if (this[kCheckNotClosed](callback)) {
      return callback[kCallbackPromise]
    }

    if (!this[kOptions].idempotent) {
      callback(new UserError('Cannot link a message stream to a transaction on a non-idempotent producer.'))
      return callback[kCallbackPromise]
    }

    if (!this.#transactionalId) {
      callback(new UserError('No active transaction to link stream to.'))
      return callback[kCallbackPromise]
    }

    // TODO@PI: If the stream has autocommit enabled, we should throw an error

    const groupId = stream.consumer.groupId

    if (this.#transactionConsumerGroups.has(groupId)) {
      callback(null)
      return
    }

    this.#transactionConsumerGroups.add(groupId)

    producerTransactionsChannel.traceCallback(
      this.#addOffsetsToTransaction,
      1,
      createDiagnosticContext({ client: this, operation: 'linkConsumerGroup', groupId }),
      this,
      groupId,
      callback
    )

    return callback[kCallbackPromise]
  }

  commit<MessageKey = Buffer, MessageValue = Buffer, MessageHeaderKey = Buffer, MessageHeaderValue = Buffer> (
    message: Message<MessageKey, MessageValue, MessageHeaderKey, MessageHeaderValue>,
    callback: CallbackWithPromise<void>
  ): void
  commit<MessageKey = Buffer, MessageValue = Buffer, MessageHeaderKey = Buffer, MessageHeaderValue = Buffer> (
    message: Message<MessageKey, MessageValue, MessageHeaderKey, MessageHeaderValue>
  ): Promise<void>
  commit<MessageKey = Buffer, MessageValue = Buffer, MessageHeaderKey = Buffer, MessageHeaderValue = Buffer> (
    message: Message<MessageKey, MessageValue, MessageHeaderKey, MessageHeaderValue>,
    callback?: CallbackWithPromise<void>
  ): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback<void>()
    }

    if (this[kCheckNotClosed](callback)) {
      return callback[kCallbackPromise]
    }

    if (!this[kOptions].idempotent) {
      callback(new UserError('Cannot commit a message to a transaction on a non-idempotent producer.'))
      return callback[kCallbackPromise]
    }

    if (!this.#transactionalId) {
      callback(new UserError('No active transaction to commit message to.'))
      return callback[kCallbackPromise]
    }

    producerTransactionsChannel.traceCallback(
      this.#commit,
      1,
      createDiagnosticContext({ client: this, operation: 'commit', message }),
      this,
      message,
      callback
    )

    return callback[kCallbackPromise]
  }

  #initIdempotentProducer (
    options: ProduceOptions<Key, Value, HeaderKey, HeaderValue>,
    callback: CallbackWithPromise<ProducerInfo>
  ) {
    this[kPerformDeduplicated](
      'initProducerId',
      deduplicateCallback => {
        this[kPerformWithRetry]<InitProducerIdResponse>(
          'initProducerId',
          retryCallback => {
            const connector = this.#transactionalId
              ? this.#getCoordinatorConnection.bind(this)
              : this[kGetBootstrapConnection].bind(this)

            connector((error, connection) => {
              if (error) {
                retryCallback(error, undefined as unknown as InitProducerIdResponse)
                return
              }

              this[kGetApi]<InitProducerIdRequest, InitProducerIdResponse>('InitProducerId', (error, api) => {
                if (error) {
                  retryCallback(error, undefined as unknown as InitProducerIdResponse)
                  return
                }

                api(
                  connection,
                  this.#transactionalId,
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
              deduplicateCallback(error, undefined as unknown as ProducerInfo)
              return
            }

            this.#producerInfo = { producerId: response.producerId, producerEpoch: response.producerEpoch }
            deduplicateCallback(null, this.#producerInfo)
          },
          0
        )
      },
      callback
    )
  }

  #beginTransaction (callback: CallbackWithPromise<void>) {
    this.#findCoordinator(error => {
      if (error) {
        callback(error)
        return
      }

      this.#transactionalId = this[kOptions].transactionalId!

      if (!this.#producerInfo) {
        this.initIdempotentProducer({}, error => {
          if (error) {
            this.#transactionalId = undefined
            callback(error)
            return
          }

          callback(null)
        })

        return
      }

      callback(null)
    })
  }

  #send (options: SendOptions<Key, Value, HeaderKey, HeaderValue>, callback: CallbackWithPromise<ProduceResult>): void {
    options.idempotent ??= this[kOptions].idempotent
    options.repeatOnStaleMetadata ??= this[kOptions].repeatOnStaleMetadata
    options.partitioner ??= this[kOptions].partitioner

    let { idempotent, partitioner } = options as Required<SendOptions<Key, Value, HeaderKey, HeaderValue>>
    let producerId: bigint | undefined = options.producerId
    let producerEpoch: number | undefined = options.producerEpoch

    // Some values are overriden when using transactions
    if (this.#transactionalId) {
      idempotent = true
      options.acks = ProduceAcks.ALL
    } else {
      options.acks ??= idempotent ? ProduceAcks.ALL : ProduceAcks.LEADER
    }

    // We still need to initialize the producerId
    if (idempotent) {
      if (!this.#producerInfo) {
        const { messages, ...initOptions } = options

        this.initIdempotentProducer(initOptions, error => {
          if (error) {
            callback(error, undefined as unknown as ProduceResult)
            return
          }

          this.#send(options, callback)
        })

        return
      }

      producerId = this.#producerInfo!.producerId
      producerEpoch = this.#producerInfo!.producerEpoch

      if (typeof options.producerId !== 'undefined' || typeof options.producerEpoch !== 'undefined') {
        callback(
          new UserError('Cannot specify producerId or producerEpoch when using idempotent producer.'),
          undefined as unknown as ProduceResult
        )

        return
      }

      if (options.acks !== ProduceAcks.ALL) {
        callback(
          new UserError('Idempotent producer requires acks to be ALL (-1).'),
          undefined as unknown as ProduceResult
        )

        return
      }
    }

    const produceOptions: Partial<CreateRecordsBatchOptions> = {
      compression: options.compression ?? this[kOptions].compression,
      producerId,
      producerEpoch,
      sequences: idempotent ? this.#sequences : undefined,
      transactionalId: this.#transactionalId
    }

    // Build messages records out of messages
    const topics = new Set<string>()
    const messages: MessageRecord[] = []
    for (const message of options.messages) {
      const topic = message.topic
      let headers = new Map<HeaderKey, HeaderValue>()
      const serializedHeaders = new Map<Buffer, Buffer>()

      if (message.headers) {
        headers =
          message.headers instanceof Map
            ? (message.headers as Map<HeaderKey, HeaderValue>)
            : new Map(Object.entries(message.headers) as [HeaderKey, HeaderValue][])

        for (const [key, value] of headers) {
          serializedHeaders.set(this.#headerKeySerializer(key as HeaderKey)!, this.#headerValueSerializer(value)!)
        }
      }

      const key = this.#keySerializer(message.key, headers)
      const value = this.#valueSerializer(message.value, headers)!

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

    if (this.#transactionalId) {
      this.#addPartitionsToTransaction(
        Array.from(topics),
        messages,
        options as Required<SendOptions<Key, Value, HeaderKey, HeaderValue>>,
        produceOptions,
        callback
      )
    } else {
      this.#performSend(
        Array.from(topics),
        messages,
        options as Required<SendOptions<Key, Value, HeaderKey, HeaderValue>>,
        produceOptions,
        callback
      )
    }
  }

  #addPartitionsToTransaction (
    topics: string[],
    messages: MessageRecord[],
    sendOptions: Required<SendOptions<Key, Value, HeaderKey, HeaderValue>>,
    produceOptions: Partial<CreateRecordsBatchOptions>,
    callback: CallbackWithPromise<ProduceResult>
  ): void {
    // Get the metadata for topics, we need to normalize the partitions
    this[kMetadata]({ topics, autocreateTopics: sendOptions.autocreateTopics }, (
      error: Error | null,
      metadata: ClusterMetadata
    ) => {
      if (error) {
        callback(error, undefined as unknown as ProduceResult)
        return
      }

      // Scan all messages to find the partitions we need to add to the transaction
      const toAdd = new Map<string, Set<number>>()
      for (const message of messages) {
        message.partition! %= metadata.topics.get(message.topic)!.partitionsCount

        const { topic, partition } = message
        const key = `${topic}:${partition}`

        if (!this.#transactionTopicPartitions.has(key)) {
          this.#transactionTopicPartitions.add(key)

          let partitionsSet = toAdd.get(topic)
          if (!partitionsSet) {
            partitionsSet = new Set<number>()
            toAdd.set(topic, partitionsSet)
          }

          partitionsSet.add(partition!)
        }
      }

      // Nothing to add, continue sending
      if (toAdd.size === 0) {
        this.#performSend(topics, messages, sendOptions, produceOptions, callback)
        return
      }

      producerTransactionsChannel.traceCallback(
        this.#performAddPartitionsToTransaction,
        1,
        createDiagnosticContext({ client: this, operation: 'addPartitions' }),
        this,
        toAdd,
        (error?: Error | null) => {
          if (error) {
            callback!(error, undefined as unknown as ProduceResult)
            return
          }

          this.#performSend(topics, messages, sendOptions, produceOptions, callback)
        }
      )
    })
  }

  #performSend (
    topics: string[],
    messages: MessageRecord[],
    sendOptions: Required<SendOptions<Key, Value, HeaderKey, HeaderValue>>,
    produceOptions: Partial<CreateRecordsBatchOptions>,
    callback: CallbackWithPromise<ProduceResult>
  ): void {
    // Get the metadata with the topic/partitions informations
    this[kMetadata]({ topics, autocreateTopics: sendOptions.autocreateTopics }, (
      error: Error | null,
      metadata: ClusterMetadata
    ) => {
      if (error) {
        callback(error, undefined as unknown as ProduceResult)
        return
      }

      const messagesByDestination = new Map<number, MessageRecord[]>()

      // Track the number of messages per partition so we can update the sequence number
      const messagesPerPartition = new NumericMap()

      // Normalize the partition of all messages, then enqueue them to their destination
      for (const message of messages) {
        message.partition! %= metadata.topics.get(message.topic)!.partitionsCount

        const { topic, partition } = message
        const leader = metadata.topics.get(topic)!.partitions[partition!].leader

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

      runConcurrentCallbacks<ProduceResponse | boolean>(
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
            callback(error, undefined as unknown as ProduceResult)
            return
          }

          this.#metricsProducedMessages?.inc(messages.length)
          const results: ProduceResult = {}

          if (sendOptions.acks === ProduceAcks.NO_RESPONSE) {
            const unwritableNodes = []

            for (let i = 0; i < apiResults.length; i++) {
              if (apiResults[i] === false) {
                unwritableNodes.push(nodes[i])
              }
            }

            results.unwritableNodes = unwritableNodes
          } else {
            const topics: ProduceResult['offsets'] = []

            for (const result of apiResults) {
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
    this[kMetadata]({ topics, autocreateTopics }, (error: Error | null, metadata: ClusterMetadata) => {
      if (error) {
        callback(error, undefined as unknown as ProduceResponse)
        return
      }

      const { topic, partition } = messages[0]
      const leader = metadata.topics.get(topic)!.partitions[partition!].leader

      this[kPerformWithRetry]<boolean | ProduceResponse>(
        'produce',
        retryCallback => {
          this[kGetConnection](metadata.brokers.get(leader)!, (error, connection) => {
            if (error) {
              retryCallback(error, undefined as unknown as ProduceResponse)
              return
            }

            this[kGetApi]<ProduceRequest, ProduceResponse>('Produce', (error, api) => {
              if (error) {
                retryCallback(error, undefined as unknown as ProduceResponse)
                return
              }

              api(connection, acks, timeout, messages, produceOptions, retryCallback)
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

            callback(error, undefined as unknown as ProduceResponse)
            return
          }

          callback(error, results)
        },
        0,
        [],
        (error: Error) => {
          return repeatOnStaleMetadata && !!(error as GenericError).findBy('hasStaleMetadata', true)
        }
      )
    })
  }

  #findCoordinator (callback: CallbackWithPromise<void>): void {
    if (this.#coordinatorId !== undefined) {
      callback(null)
      return
    }

    const transactionalId = this[kOptions].transactionalId!

    this[kPerformDeduplicated](
      'findCoordinator',
      deduplicateCallback => {
        this[kPerformWithRetry]<FindCoordinatorResponse>(
          'findCoordinator',
          retryCallback => {
            this[kGetBootstrapConnection]((error, connection) => {
              if (error) {
                retryCallback(error, undefined as unknown as FindCoordinatorResponse)
                return
              }

              this[kGetApi]<FindCoordinatorRequest, FindCoordinatorResponse>('FindCoordinator', (error, api) => {
                if (error) {
                  retryCallback(error, undefined as unknown as FindCoordinatorResponse)
                  return
                }

                api(connection, FindCoordinatorKeyTypes.TRANSACTION, [transactionalId], retryCallback)
              })
            })
          },
          (error, response) => {
            if (error) {
              deduplicateCallback(error)
              return
            }

            const groupInfo = response.coordinators.find(coordinator => coordinator.key === transactionalId)!
            this.#coordinatorId = groupInfo.nodeId

            deduplicateCallback(null)
          },
          0
        )
      },
      callback
    )
  }

  #getCoordinatorConnection (callback: CallbackWithPromise<Connection>): void {
    // Get a connection to the coordinator
    this[kMetadata]({}, (error: Error | null, metadata: ClusterMetadata) => {
      if (error) {
        callback(error, undefined as unknown as Connection)
        return
      }

      this[kPerformWithRetry](
        'getCoordinatorConnection',
        retryCallback => {
          this[kGetConnection](metadata.brokers.get(this.#coordinatorId)!, (error, connection) => {
            if (error) {
              retryCallback(error, undefined as unknown as Connection)
              return
            }

            retryCallback(null, connection)
          })
        },
        callback
      )
    })
  }

  #endTransaction (commit: boolean, callback: CallbackWithPromise<void>): void {
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

                api(
                  connection,
                  this.#transactionalId!,
                  this.#producerInfo!.producerId,
                  this.#producerInfo!.producerEpoch,
                  commit,
                  (error: Error | null) => {
                    if (error) {
                      this.#handleFencingError(error)
                      retryCallback(error)
                      return
                    }

                    this.#transactionTopicPartitions.clear()
                    this.#transactionConsumerGroups.clear()
                    this.#transactionalId = undefined
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

  #performAddPartitionsToTransaction (
    topicsPartitions: Map<string, Set<number>>,
    callback: CallbackWithPromise<void>
  ): void {
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

                api(
                  connection,
                  [
                    {
                      transactionalId: this.#transactionalId!,
                      producerId: this.#producerInfo!.producerId,
                      producerEpoch: this.#producerInfo!.producerEpoch,
                      topics: transactions,
                      verifyOnly: false
                    }
                  ],
                  (error: Error | null) => {
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

  #addOffsetsToTransaction (groupId: string, callback: CallbackWithPromise<void>): void {
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

                api(
                  connection,
                  this.#transactionalId!,
                  this.#producerInfo!.producerId,
                  this.#producerInfo!.producerEpoch,
                  groupId,
                  (error: Error | null) => {
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

  #commit<MessageKey = Buffer, MessageValue = Buffer, MessageHeaderKey = Buffer, MessageHeaderValue = Buffer> (
    message: Message<MessageKey, MessageValue, MessageHeaderKey, MessageHeaderValue>,
    callback: CallbackWithPromise<void>
  ): void {
    this[kPerformDeduplicated](
      'commit',
      deduplicateCallback => {
        this[kPerformWithRetry]<void>(
          'commit',
          retryCallback => {
            this[kMetadata]({ topics: [message.topic] }, (error: Error | null, metadata: ClusterMetadata) => {
              if (error) {
                retryCallback(error)
                return
              }

              const { groupId, generationId, memberId, coordinatorId } = message.metadata
                .consumer as MessageConsumerMetadata

              this[kGetConnection](metadata.brokers.get(coordinatorId)!, (error, connection) => {
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

                  api(
                    connection,
                    this.#transactionalId!,
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
                            committedOffset: message.offset!,
                            committedLeaderEpoch: metadata.topics.get(topic)!.partitions[partition].leaderEpoch
                          }
                        ]
                      }
                    ],
                    (error: Error | null) => {
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

  #handleFencingError (error: Error): void {
    if ((error as GenericError).findBy<ProtocolError>?.('producerFenced', true)) {
      this.#producerInfo = undefined
      this.#transactionTopicPartitions.clear()
      this.#transactionalId = undefined
    }
  }
}
