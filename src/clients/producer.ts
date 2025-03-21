import { type ProduceResponse, produceV11 } from '../apis/producer/produce.ts'
import { type Broker } from '../connection/definitions.ts'
import { murmur2 } from '../protocol/murmur2.ts'
import { type CreateRecordsBatchOptions, type Message } from '../protocol/records.ts'
import {
  type CallbackWithPromise,
  createPromisifiedCallback,
  kCallbackPromise,
  runConcurrentCallbacks
} from './callbacks.ts'
import { Client, type ClientOptions, type ClusterMetadata } from './client.ts'

import { ProduceAcks } from '../apis/enumerations.ts'
import { type InitProducerIdResponse, initProducerIdV5 } from '../apis/producer/init-producer-id.ts'
import { type MultipleErrors, UserError } from '../errors.ts'
import { type CompressionAlgorithms, compressionsAlgorithms } from '../protocol/compression.ts'
import { ajv, niceJoin } from '../utils.ts'

export type Partitioner = (message: Message) => number

export interface ProduceOptions {
  idempotent: boolean
  acks: number
  compression: CompressionAlgorithms
  timeout: number
  producerId: bigint
  producerEpoch: number
  partitioner: Partitioner
  repeatOnStaleMetadata: boolean
}

export type ProducerOptions = ClientOptions & Partial<ProduceOptions>

export interface ProducerInfo {
  producerId: bigint
  producerEpoch: number
}

export interface ProduceResult {
  // This is only defined when ack is not NO_RESPONSE
  offsets?: Record<string, Record<string, bigint>>
  // This is only defined when ack is NO_RESPONSE
  unwritableNodes?: number[]
}

export const produceOptionsSchema = {
  type: 'object',
  properties: {
    idempotent: { type: 'boolean' },
    acks: {
      type: 'number',
      enum: Object.values(ProduceAcks),
      errorMessage: `should be one of ${niceJoin(
        Object.entries(ProduceAcks).map(([k, v]) => `${v} (${k})`),
        ' or '
      )}`
    },
    compression: { type: 'string', enum: Object.keys(compressionsAlgorithms) },
    timeout: { type: 'number' },
    producerId: { bigint: true },
    producerEpoch: { type: 'number' },
    partitioner: { function: true },
    repeatOnStaleMetadata: { type: 'boolean' }
  },
  additionalProperties: true
}

export const produceOptionsValidator = ajv.compile(produceOptionsSchema)

export class Producer extends Client<ProducerOptions> {
  #partitionsRoundRobin: Record<string, number> = {}
  // These two values should be serializable and loadable in the constructor in order to restore
  // the idempotent producer status.
  #producerInfo!: ProducerInfo
  #sequences: Record<string, number>

  constructor (
    clientId: string,
    bootstrapBrokers: Broker | string | Broker[] | string[],
    options: Partial<ProducerOptions> = {}
  ) {
    if (options.idempotent) {
      options.maxInflights = 1
      options.acks = ProduceAcks.ALL
      options.retries = Number.MAX_SAFE_INTEGER
    }

    options.repeatOnStaleMetadata ??= true

    super(clientId, bootstrapBrokers, options as Partial<ClientOptions>)
    this.#partitionsRoundRobin = {}
    this.#sequences = {}

    this.validateOptions(options, produceOptionsValidator, '/options')
  }

  initIdempotentProducer (producerId: bigint, producerEpoch: number, callback: CallbackWithPromise<ProducerInfo>): void
  initIdempotentProducer (producerId?: bigint, producerEpoch?: number): Promise<ProducerInfo>
  initIdempotentProducer (
    producerId: bigint = 0n,
    producerEpoch: number = 0,
    callback?: CallbackWithPromise<ProducerInfo>
  ): void | Promise<ProducerInfo> {
    if (!callback) {
      callback = createPromisifiedCallback<ProducerInfo>()
    }

    if (typeof producerId !== 'bigint' || producerId < 0n) {
      callback(new UserError('producerId must be a non-negative number.'), undefined as unknown as ProducerInfo)
      return callback[kCallbackPromise]
    }

    if (typeof producerEpoch !== 'number' || producerEpoch < 0) {
      callback(new UserError('producerEpoch must be a non-negative number.'), undefined as unknown as ProducerInfo)
      return callback[kCallbackPromise]
    }

    return this.performDeduplicated(
      'initProducerId',
      deduplicateCallback => {
        this.performWithRetry<InitProducerIdResponse>(
          'Initializing idempotent producer failed.',
          retryCallback => {
            this.connections.getFromMultiple(this.bootstrapBrokers, (error, connection) => {
              if (error) {
                callback(error, undefined as unknown as ProducerInfo)
                return
              }

              initProducerIdV5(
                connection,
                null,
                this.options.operationTimeout,
                producerId,
                producerEpoch,
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
          0,
          this.options.retries
        )
      },
      callback
    )
  }

  produce (messages: Message[], options?: Partial<ProduceOptions>): Promise<ProduceResult>
  produce (messages: Message[], callback: CallbackWithPromise<ProduceResult>): void
  produce (messages: Message[], options: Partial<ProduceOptions>, callback: CallbackWithPromise<ProduceResult>): void
  produce (
    messages: Message[],
    options: Partial<ProduceOptions> | CallbackWithPromise<ProduceResult> = {},
    callback?: CallbackWithPromise<ProduceResult>
  ): void | Promise<ProduceResult> {
    if (typeof options === 'function') {
      callback = options
      options = {}
    }

    if (!callback) {
      callback = createPromisifiedCallback<ProduceResult>()
    }

    const validationError = this.validateOptions(options, produceOptionsValidator, '/options', false)

    if (validationError) {
      callback(validationError, undefined as unknown as ProduceResult)
      return callback[kCallbackPromise]
    }

    let { idempotent, timeout, acks, partitioner, compression, producerId, producerEpoch, repeatOnStaleMetadata } =
      options

    idempotent ??= this.options.idempotent ?? false

    // We still need to initialize the producerId
    if (idempotent && !this.#producerInfo) {
      this.initIdempotentProducer(producerId ?? 0n, producerEpoch ?? 0, error => {
        if (error) {
          callback(error, undefined as unknown as ProduceResult)
          return
        }

        this.produce(messages, options, callback)
      })

      return callback[kCallbackPromise]
    }

    timeout ??= this.options.operationTimeout
    acks ??= this.options.acks ?? (idempotent ? ProduceAcks.ALL : ProduceAcks.LEADER)
    partitioner ??= this.options.partitioner
    compression ??= this.options.compression
    repeatOnStaleMetadata ??= this.options.repeatOnStaleMetadata ?? true

    if (idempotent) {
      if (typeof producerId !== 'undefined' || typeof producerEpoch !== 'undefined') {
        callback(
          new UserError('Cannot specify producerId or producerEpoch when using idempotent producer.'),
          undefined as unknown as ProduceResult
        )

        return callback[kCallbackPromise]
      }

      if (acks !== ProduceAcks.ALL) {
        callback(
          new UserError('Idempotent producer requires acks to be ALL (-1).'),
          undefined as unknown as ProduceResult
        )

        return callback[kCallbackPromise]
      }

      producerId = this.#producerInfo!.producerId
      producerEpoch = this.#producerInfo!.producerEpoch
    }

    const produceOptions: Partial<CreateRecordsBatchOptions> = {
      compression,
      producerId,
      producerEpoch,
      sequences: idempotent ? this.#sequences : undefined
    }

    // Ensure a partition to all messages
    const topics = new Set<string>()
    for (const message of messages) {
      const topic = message.topic
      topics.add(topic)

      if (message.partition == null) {
        if (partitioner) {
          message.partition = partitioner(message)
        } else if (message.key) {
          message.partition = murmur2(message.key) >>> 0
        } else {
          // Use the roundrobin
          this.#partitionsRoundRobin[topic] ??= -1
          message.partition = ++this.#partitionsRoundRobin[topic]
        }
      }
    }

    this.#performProduce(Array.from(topics), messages, timeout, acks, repeatOnStaleMetadata, produceOptions, callback)
    return callback[kCallbackPromise]
  }

  #performProduce (
    topics: string[],
    messages: Message[],
    timeout: number,
    acks: number,
    repeatOnStaleMetadata: boolean,
    produceOptions: Partial<CreateRecordsBatchOptions>,
    callback: CallbackWithPromise<ProduceResult>
  ): void {
    // Get the metadata with the topic/partitions informations
    this.metadata(topics, (error: Error | null, metadata: ClusterMetadata) => {
      if (error) {
        callback(error, undefined as unknown as ProduceResult)
        return
      }

      const messagesByDestination = new Map<number, Message[]>()

      // Track the number of messages per partition so we can update the sequence number
      const messagesPerPartition: Record<string, number> = {}

      // Normalize the partition of all messages, then enqueue them to their destination
      for (const message of messages) {
        message.partition! %= metadata.topics[message.topic].partitionsCount

        const { topic, partition } = message
        const leader = metadata.topics[topic].partitions[partition!].leader

        let destination = messagesByDestination.get(leader)
        if (!destination) {
          destination = []
          messagesByDestination.set(leader, destination)
        }

        const messagePartitionKey = `${message.topic}:${partition}`
        messagesPerPartition[messagePartitionKey] ??= 0
        messagesPerPartition[messagePartitionKey]++
        destination.push(message)
      }

      // Track nodes so that we can get their ID for delayed write reporting
      const nodes: number[] = []

      runConcurrentCallbacks<ProduceResponse | boolean>(
        'Producing messages failed.',
        messagesByDestination,
        ([destination, messages], concurrentCallback) => {
          nodes.push(destination)

          this.#performSingleDestinationProduce(
            topics,
            messages,
            timeout,
            acks,
            repeatOnStaleMetadata,
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

          if (acks === ProduceAcks.NO_RESPONSE) {
            const writeDelayedNodes = []

            for (let i = 0; i < apiResults.length; i++) {
              if (apiResults[i] === false) {
                writeDelayedNodes.push(nodes[i])
              }
            }

            results.unwritableNodes = writeDelayedNodes
          } else {
            const topics: ProduceResult['offsets'] = {}

            for (const result of apiResults) {
              for (const { name, partitionResponses } of (result as ProduceResponse).responses) {
                topics[name] ??= {}

                for (const partitionResponse of partitionResponses) {
                  topics[name][partitionResponse.index] = partitionResponse.baseOffset

                  const partitionKey = `${name}:${partitionResponse.index}`
                  this.#sequences[partitionKey] ??= 0
                  this.#sequences[partitionKey] += messagesPerPartition[partitionKey]
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

  #performSingleDestinationProduce (
    topics: string[],
    messages: Message[],
    timeout: number,
    acks: number,
    repeatOnStaleMetadata: boolean,
    produceOptions: Partial<CreateRecordsBatchOptions>,
    callback: CallbackWithPromise<boolean | ProduceResponse>
  ): void {
    // Get the metadata with the topic/partitions informations
    this.metadata(topics, (error: Error | null, metadata: ClusterMetadata) => {
      if (error) {
        callback(error, undefined as unknown as ProduceResponse)
        return
      }

      const { topic, partition } = messages[0]
      const leader = metadata.topics[topic].partitions[partition!].leader

      this.performWithRetry<boolean | ProduceResponse>(
        'Producing messages failed.',
        retryCallback => {
          this.connections.get(metadata.brokers[leader], (error, connection) => {
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
            const hasStaleMetadata = (error as MultipleErrors).hasAny('hasStaleMetadata', true)

            if (hasStaleMetadata && repeatOnStaleMetadata) {
              this.clearMetadata()
              this.#performSingleDestinationProduce(topics, messages, timeout, acks, false, produceOptions, callback)
              return
            }

            callback(error, undefined as unknown as ProduceResponse)
            return
          }

          callback(error, results)
        },
        0,
        this.options.retries,
        [],
        (error: Error) => {
          return repeatOnStaleMetadata && (error as MultipleErrors).hasAny('hasStaleMetadata', true)
        }
      )
    })
  }
}
