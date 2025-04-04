import { type ValidateFunction } from 'ajv'
import { EventEmitter } from 'node:events'
import { type Callback } from '../../apis/definitions.ts'
import type { MetadataResponse } from '../../apis/metadata/metadata.ts'
import { metadataV12 } from '../../apis/metadata/metadata.ts'
import { ConnectionPool, type Broker, type ConnectionOptions } from '../../connection/index.ts'
import type { GenericError } from '../../errors.ts'
import { MultipleErrors, NetworkError, UserError } from '../../errors.ts'
import { loggers } from '../../logging.ts'
import { ajv } from '../../utils.ts'
import { createPromisifiedCallback, kCallbackPromise, type CallbackWithPromise } from '../callbacks.ts'
import { baseOptionsValidator, defaultBaseOptions, defaultPort, metadataOptionsValidator } from './options.ts'
import { type BaseOptions, type ClusterMetadata, type ClusterTopicMetadata, type MetadataOptions } from './types.ts'

export class Base<OptionsType extends BaseOptions> extends EventEmitter {
  // General status
  protected clientId: string
  protected bootstrapBrokers: Broker[]
  protected options: OptionsType
  protected connections: ConnectionPool
  protected closed: boolean = false

  #inflightDeduplications: Map<string, CallbackWithPromise<any>[]>
  #metadata: ClusterMetadata | undefined

  constructor (options: OptionsType) {
    super()

    // Validate options
    this.options = Object.assign({}, defaultBaseOptions as OptionsType, options) as OptionsType
    this.validateOptions(this.options, baseOptionsValidator, '/options')
    this.clientId = options.clientId

    // Initialize bootstrap brokers
    this.bootstrapBrokers = []
    for (const broker of options.bootstrapBrokers) {
      this.bootstrapBrokers.push(this.parseBroker(broker))
    }

    // Initialize connection pool
    this.connections = new ConnectionPool(this.clientId, this.options as ConnectionOptions)
    for (const event of ['connect', 'disconnect', 'failed', 'drain']) {
      this.connections.on(event, payload => this.emitWithDebug('client', `broker:${event}`, payload))
    }
    this.closed = false

    this.#inflightDeduplications = new Map()
  }

  emitWithDebug (section: string | null, name: string, ...args: any[]): boolean {
    if (!section) {
      return this.emit(name, ...args)
    }

    loggers[section]?.debug({ event: name, payload: args })
    return this.emit(`${section}:${name}`, ...args)
  }

  close (callback: CallbackWithPromise<void>): void
  close (): Promise<void>
  close (callback?: CallbackWithPromise<void>): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback<void>()
    }

    this.closed = true
    this.connections.disconnect(callback)

    return callback[kCallbackPromise]
  }

  metadata (options: MetadataOptions, callback: CallbackWithPromise<ClusterMetadata>): void
  metadata (options: MetadataOptions): Promise<ClusterMetadata>
  metadata (options: MetadataOptions, callback?: CallbackWithPromise<ClusterMetadata>): void | Promise<ClusterMetadata> {
    if (!callback) {
      callback = createPromisifiedCallback<ClusterMetadata>()
    }

    const validationError = this.validateOptions(options, metadataOptionsValidator, '/options', false)
    if (validationError) {
      callback(validationError, undefined as unknown as ClusterMetadata)
      return callback[kCallbackPromise]
    }

    this._metadata(options, callback)
    return callback[kCallbackPromise]
  }

  protected _metadata (options: MetadataOptions, callback: CallbackWithPromise<ClusterMetadata>): void {
    const metadataMaxAge = options.metadataMaxAge ?? this.options.metadataMaxAge!

    const isStale =
      options.forceUpdate ||
      !this.#metadata ||
      Date.now() > this.#metadata.lastUpdate + metadataMaxAge ||
      options.topics.some(topic => !this.#metadata?.topics.has(topic))

    if (!isStale) {
      callback(null, this.#metadata!)
      return
    }

    const autocreateTopics = options.autocreateTopics ?? this.options.autocreateTopics

    this.performDeduplicated(
      'metadata',
      deduplicateCallback => {
        this.performWithRetry<MetadataResponse>(
          'metadata',
          retryCallback => {
            this.connections.getFromMultiple(this.bootstrapBrokers, (error, connection) => {
              if (error) {
                retryCallback(error, undefined as unknown as MetadataResponse)
                return
              }

              metadataV12(connection, options.topics, autocreateTopics, true, retryCallback)
            })
          },
          (error: Error | null, metadata: MetadataResponse) => {
            if (error) {
              deduplicateCallback(error, undefined as unknown as ClusterMetadata)
              return
            }

            const brokers: ClusterMetadata['brokers'] = new Map()
            const topics: ClusterMetadata['topics'] = new Map()

            for (const broker of metadata.brokers) {
              const { host, port } = broker
              brokers.set(broker.nodeId, { host, port })
            }

            for (const { name, topicId: id, partitions: rawPartitions, isInternal } of metadata.topics) {
              if (isInternal) {
                continue
              }

              const partitions: ClusterTopicMetadata['partitions'] = []

              for (const rawPartition of rawPartitions.sort((a, b) => a.partitionIndex - b.partitionIndex)) {
                partitions[rawPartition.partitionIndex] = {
                  leader: rawPartition.leaderId,
                  leaderEpoch: rawPartition.leaderEpoch,
                  replicas: rawPartition.replicaNodes
                }
              }

              topics.set(name, { id, partitions, partitionsCount: rawPartitions.length })
            }

            this.#metadata = {
              id: metadata.clusterId!,
              brokers,
              topics,
              lastUpdate: Date.now()
            }

            this.emitWithDebug('client', 'metadata', this.#metadata)
            deduplicateCallback(null, this.#metadata)
          },
          0
        )
      },
      callback
    )
  }

  protected checkNotClosed (callback: CallbackWithPromise<any>): boolean {
    if (this.closed) {
      const error = new UserError('Client is closed.')
      callback(error, undefined)
      return true
    }

    return false
  }

  protected clearMetadata (): void {
    this.#metadata = undefined
  }

  protected parseBroker (broker: Broker | string): Broker {
    if (typeof broker === 'string') {
      if (broker.includes(':')) {
        const [host, port] = broker.split(':')
        return { host, port: Number(port) }
      } else {
        return { host: broker, port: defaultPort }
      }
    }

    return broker
  }

  protected performWithRetry<ReturnType>(
    operationId: string,
    operation: (callback: Callback<ReturnType>) => void,
    callback: CallbackWithPromise<ReturnType>,
    attempt: number = 0,
    errors: Error[] = [],
    shouldSkipRetry?: (e: Error) => boolean
  ): void | Promise<ReturnType> {
    const retries = this.options.retries!
    this.emitWithDebug('client', 'performWithRetry', operationId, attempt, retries)

    operation((error, result) => {
      if (error) {
        const genericError = error as GenericError
        const retriable = genericError.findBy?.('code', NetworkError.code) || genericError.findBy?.('canRetry', true)
        errors.push(error)

        if (attempt < retries && retriable && !shouldSkipRetry?.(error)) {
          setTimeout(() => {
            this.performWithRetry(operationId, operation, callback, attempt + 1, errors, shouldSkipRetry)
          }, this.options.retryDelay)
        } else {
          if (attempt === 0) {
            callback(error, undefined as ReturnType)
          }

          callback(new MultipleErrors(`${operationId} failed ${attempt + 1} times.`, errors), undefined as ReturnType)
        }

        return
      }

      callback(null, result!)
    })

    return callback[kCallbackPromise]
  }

  protected performDeduplicated<ReturnType>(
    operationId: string,
    operation: (callback: CallbackWithPromise<ReturnType>) => void,
    callback: CallbackWithPromise<ReturnType>
  ): void | Promise<ReturnType> {
    let inflights = this.#inflightDeduplications.get(operationId)

    if (!inflights) {
      inflights = []
      this.#inflightDeduplications.set(operationId, inflights)
    }

    inflights.push(callback)

    if (inflights.length === 1) {
      this.emitWithDebug('client', 'performDeduplicated', operationId)
      operation((error, result) => {
        this.#inflightDeduplications.set(operationId, [])

        for (const cb of inflights!) {
          cb(error, result)
        }

        inflights = []
      })
    }

    return callback[kCallbackPromise]
  }

  protected validateOptions (
    target: unknown,
    validator: ValidateFunction<unknown>,
    targetName: string,
    throwOnErrors: boolean = true
  ): Error | null {
    if (!this.options.strict) {
      return null
    }

    const valid = validator(target)

    if (!valid) {
      const error = new UserError(ajv.errorsText(validator.errors, { dataVar: targetName }) + '.')

      if (throwOnErrors) {
        throw error
      }

      return error
    }

    return null
  }
}
