import { ProduceAcks } from '../../apis/enumerations.ts'
import { type InitProducerIdResponse, api as initProducerIdV5 } from '../../apis/producer/init-producer-id.ts'
import { type ProduceResponse, api as produceV11 } from '../../apis/producer/produce.ts'
import { type GenericError, UserError } from '../../errors.ts'
import { murmur2 } from '../../protocol/murmur2.ts'
import { type CreateRecordsBatchOptions, type MessageRecord } from '../../protocol/records.ts'
import { NumericMap } from '../../utils.ts'
import {
  Base,
  kBootstrapBrokers,
  kCheckNotClosed,
  kClearMetadata,
  kConnections,
  kMetadata,
  kOptions,
  kPerformDeduplicated,
  kPerformWithRetry,
  kValidateOptions
} from '../base/base.ts'
import { type ClusterMetadata } from '../base/types.ts'
import {
  type CallbackWithPromise,
  createPromisifiedCallback,
  kCallbackPromise,
  runConcurrentCallbacks
} from '../callbacks.ts'
import { type Serializer } from '../serde.ts'
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
function noopSerializer (data?: Buffer): Buffer | undefined {
  return data
}

export class Producer<Key = Buffer, Value = Buffer, HeaderKey = Buffer, HeaderValue = Buffer> extends Base<
  ProducerOptions<Key, Value, HeaderKey, HeaderValue>
> {
  #partitionsRoundRobin: NumericMap
  // These two values should be serializable and loadable in the constructor in order to restore
  // the idempotent producer status.
  #producerInfo!: ProducerInfo
  #sequences: NumericMap
  #keySerializer: Serializer<Key>
  #valueSerializer: Serializer<Value>
  #headerKeySerializer: Serializer<HeaderKey>
  #headerValueSerializer: Serializer<HeaderValue>

  constructor (options: ProducerOptions<Key, Value, HeaderKey, HeaderValue>) {
    if (options.idempotent) {
      options.maxInflights = 1
      options.acks = ProduceAcks.ALL
      options.retries = Number.MAX_SAFE_INTEGER
    }

    options.repeatOnStaleMetadata ??= true

    super(options)
    this.#partitionsRoundRobin = new NumericMap()
    this.#sequences = new NumericMap()
    this.#keySerializer = options.serializers?.key ?? (noopSerializer as Serializer<Key>)
    this.#valueSerializer = options.serializers?.value ?? (noopSerializer as Serializer<Value>)
    this.#headerKeySerializer = options.serializers?.headerKey ?? (noopSerializer as Serializer<HeaderKey>)
    this.#headerValueSerializer = options.serializers?.headerValue ?? (noopSerializer as Serializer<HeaderValue>)

    this[kValidateOptions](options, producerOptionsValidator, '/options')
  }

  get producerId (): bigint | undefined {
    return this.#producerInfo?.producerId
  }

  get producerEpoch (): number | undefined {
    return this.#producerInfo?.producerEpoch
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

    return this[kPerformDeduplicated](
      'initProducerId',
      deduplicateCallback => {
        this[kPerformWithRetry]<InitProducerIdResponse>(
          'initProducerId',
          retryCallback => {
            this[kConnections].getFirstAvailable(this[kBootstrapBrokers], (error, connection) => {
              if (error) {
                callback(error, undefined as unknown as ProducerInfo)
                return
              }

              initProducerIdV5(
                connection,
                null,
                this[kOptions].timeout!,
                options.producerId ?? this[kOptions].producerId ?? 0n,
                options.producerEpoch ?? this[kOptions].producerEpoch ?? 0,
                retryCallback
              )
            })
          },
          (error, response) => {
            if (error) {
              callback(error, undefined as unknown as ProducerInfo)
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

    options.idempotent ??= this[kOptions].idempotent ?? false
    /* c8 ignore next */
    options.repeatOnStaleMetadata ??= this[kOptions].repeatOnStaleMetadata ?? true
    options.partitioner ??= this[kOptions].partitioner

    const { idempotent, partitioner } = options as Required<SendOptions<Key, Value, HeaderKey, HeaderValue>>

    if (idempotent) {
      options.acks ??= ProduceAcks.ALL
    } else {
      options.acks ??= ProduceAcks.LEADER
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

          this.send(options, callback)
        })

        return callback[kCallbackPromise]
      }

      if (typeof options.producerId !== 'undefined' || typeof options.producerEpoch !== 'undefined') {
        callback(
          new UserError('Cannot specify producerId or producerEpoch when using idempotent producer.'),
          undefined as unknown as ProduceResult
        )

        return callback[kCallbackPromise]
      }

      if (options.acks !== ProduceAcks.ALL) {
        callback(
          new UserError('Idempotent producer requires acks to be ALL (-1).'),
          undefined as unknown as ProduceResult
        )

        return callback[kCallbackPromise]
      }
    }

    const produceOptions: Partial<CreateRecordsBatchOptions> = {
      compression: options.compression ?? this[kOptions].compression,
      producerId: idempotent ? this.#producerInfo.producerId : options.producerId,
      producerEpoch: idempotent ? this.#producerInfo.producerEpoch : options.producerEpoch,
      sequences: idempotent ? this.#sequences : undefined
    }

    // Build messages records out of messages
    const topics = new Set<string>()
    const messages: MessageRecord[] = []
    for (const message of options.messages) {
      const topic = message.topic
      const key = this.#keySerializer(message.key)
      const headers = new Map<Buffer, Buffer>()
      let partition: number = 0

      if (typeof message.partition !== 'number') {
        if (partitioner) {
          partition = partitioner(message)
        } else if (key) {
          partition = murmur2(key) >>> 0
        } else {
          // Use the roundrobin
          partition = this.#partitionsRoundRobin.postIncrement(topic, 1, -1)
        }
      } else {
        partition = message.partition
      }

      if (message.headers) {
        const entries = message.headers instanceof Map ? message.headers : Object.entries(message.headers)

        for (const [key, value] of entries) {
          headers.set(this.#headerKeySerializer(key as HeaderKey)!, this.#headerValueSerializer(value)!)
        }
      }

      topics.add(topic)
      messages.push({
        key,
        value: this.#valueSerializer(message.value)!,
        headers,
        topic,
        partition,
        timestamp: message.timestamp
      })
    }

    this.#performSend(
      Array.from(topics),
      messages,
      options as Required<SendOptions<Key, Value, HeaderKey, HeaderValue>>,
      produceOptions,
      callback
    )

    return callback[kCallbackPromise]
  }

  #performSend (
    topics: string[],
    messages: MessageRecord[],
    sendOptions: Required<SendOptions<Key, Value, HeaderKey, HeaderValue>>,
    produceOptions: Partial<CreateRecordsBatchOptions>,
    callback: CallbackWithPromise<ProduceResult>
  ): void {
    // Get the metadata with the topic/partitions informations
    this[kMetadata](
      { topics, autocreateTopics: sendOptions.autocreateTopics },
      (error: Error | null, metadata: ClusterMetadata) => {
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
          ([destination, messages], concurrentCallback) => {
            nodes.push(destination)

            this.#performSingleDestinationSend(
              topics,
              messages,
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
      }
    )
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
          this[kConnections].get(metadata.brokers.get(leader)!, (error, connection) => {
            if (error) {
              retryCallback(error, undefined as unknown as ProduceResponse)
              return
            }

            produceV11(connection, acks, timeout, messages, produceOptions, retryCallback)
          })
        },
        (error, results) => {
          if (error) {
            // If the last error was due to stale metadata, we retry the operation with this set of messages
            // since the partition is already set, it should attempt on the new destination
            const hasStaleMetadata = (error as GenericError).findBy('hasStaleMetadata', true)

            if (hasStaleMetadata && repeatOnStaleMetadata) {
              this[kClearMetadata]()
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
}
