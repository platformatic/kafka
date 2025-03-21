import { type ValidateFunction } from 'ajv'
import { EventEmitter } from 'node:events'
import { type Callback } from '../apis/definitions.ts'
import type { MetadataResponse } from '../apis/metadata/metadata.ts'
import { metadataV12 } from '../apis/metadata/metadata.ts'
import { ConnectionPool, type Broker, type ConnectionOptions } from '../connection/index.ts'
import type { GenericError } from '../errors.ts'
import { MultipleErrors, NetworkError, UserError } from '../errors.ts'
import { ajv } from '../utils.ts'
import { createPromisifiedCallback, kCallbackPromise, type CallbackWithPromise } from './callbacks.ts'

export interface ClusterPartitionMetadata {
  leader: number
  replicas: number[]
}

export interface ClusterTopicMetadata {
  id: string
  partitions: Record<string, ClusterPartitionMetadata>
  partitionsCount: number
}

export interface ClusterMetadata {
  id: string
  brokers: Record<string, Broker>
  topics: Record<string, ClusterTopicMetadata>
  lastUpdate: number
}

export interface MetadataOptions {
  autocreateTopics: boolean
  forceUpdate: boolean
}

export interface ClientOptions extends ConnectionOptions {
  operationTimeout: number
  retryDelay: number
  retries: number
  autocreateTopics: boolean
  metadataMaxAge: number
  strictOptions: boolean
}

export const defaultOptions: ClientOptions = {
  connectTimeout: 5000,
  operationTimeout: 5000,
  retries: 3,
  retryDelay: 250,
  maxInflights: 2,
  metadataMaxAge: 5000, // 5 minutes
  autocreateTopics: false,
  strictOptions: false
}

export const clientOptionsSchema = {
  type: 'object',
  properties: {
    connectTimeout: { type: 'number', minimum: 0 },
    operationTimeout: { type: 'number', minimum: 0 },
    retries: { type: 'number', minimum: 0 },
    retryDelay: { type: 'number', minimum: 0 },
    maxInflights: { type: 'number', minimum: 0 },
    metadataMaxAge: { type: 'number', minimum: 0 },
    autocreateTopics: { type: 'boolean' },
    strictOptions: { type: 'boolean' }
  },
  additionalProperties: true
}

export const optionsValidator = ajv.compile(clientOptionsSchema)

export const DEFAULT_PORT = 9092

export class Client<T extends ClientOptions> extends EventEmitter {
  // General status
  protected clientId: string
  protected bootstrapBrokers: Broker[]
  protected options: T
  protected connections: ConnectionPool

  #inflightDeduplications: Map<string, CallbackWithPromise<any>[]>
  #metadata: ClusterMetadata | undefined

  constructor (clientId: string, bootstrapBrokers: Broker | string | Broker[] | string[], options: Partial<T> = {}) {
    if (typeof clientId !== 'string' || !clientId) {
      throw new UserError('/clientId must be a non-empty string.')
    }

    super()

    this.clientId = clientId

    // Validate brokers
    this.bootstrapBrokers = [bootstrapBrokers].flat().map(this.parseBroker)

    // Validate options
    this.options = Object.assign({}, defaultOptions as T, options)
    this.validateOptions(this.options, optionsValidator, '/options')

    this.connections = new ConnectionPool(this.clientId, this.options as ConnectionOptions)
    this.#inflightDeduplications = new Map()
  }

  close (): Promise<void>
  close (callback: CallbackWithPromise<void>): void
  close (callback?: CallbackWithPromise<void>): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback<void>()
    }

    this.connections.disconnect(callback)

    return callback[kCallbackPromise]
  }

  metadata (topics: string[], callback: CallbackWithPromise<ClusterMetadata>): void
  metadata (topics: string[], options?: Partial<MetadataOptions>): Promise<ClusterMetadata>
  metadata (topics: string[], options: Partial<MetadataOptions>, callback: CallbackWithPromise<ClusterMetadata>): void
  metadata (
    topics: string[],
    options: Partial<MetadataOptions> | Callback<ClusterMetadata> = {},
    callback?: CallbackWithPromise<ClusterMetadata>
  ): void | Promise<ClusterMetadata> {
    if (typeof options === 'function') {
      callback = options
      options = {}
    }

    if (!callback) {
      callback = createPromisifiedCallback<ClusterMetadata>()
    }

    const isStale =
      options.forceUpdate ||
      !this.#metadata ||
      this.#metadata.lastUpdate + this.options.metadataMaxAge < Date.now() ||
      topics.some(topic => !this.#metadata?.topics[topic])

    if (!isStale) {
      callback(null, this.#metadata!)
      return callback[kCallbackPromise]
    }

    const autocreateTopics = options.autocreateTopics ?? this.options.autocreateTopics

    return this.performDeduplicated(
      'metadata',
      deduplicateCallback => {
        this.performWithRetry<MetadataResponse>(
          'Fetching metadata failed.',
          retryCallback => {
            this.connections.getFromMultiple(this.bootstrapBrokers, (error, connection) => {
              if (error) {
                retryCallback(error, undefined as unknown as MetadataResponse)
                return
              }

              metadataV12(connection, null, autocreateTopics, true, retryCallback)
            })
          },
          (error: Error | null, metadata: MetadataResponse) => {
            if (error) {
              deduplicateCallback(error, undefined as unknown as ClusterMetadata)
              return
            }

            const brokers: ClusterMetadata['brokers'] = {}
            const topics: ClusterMetadata['topics'] = {}

            for (const broker of metadata.brokers) {
              const { host, port } = broker
              brokers[broker.nodeId] = { host, port }
            }

            for (const { name, topicId: id, partitions: rawPartitions, isInternal } of metadata.topics) {
              if (isInternal) {
                continue
              }

              const partitions: ClusterTopicMetadata['partitions'] = {}

              for (const rawPartition of rawPartitions) {
                partitions[rawPartition.partitionIndex] = {
                  leader: rawPartition.leaderId,
                  replicas: rawPartition.replicaNodes
                }
              }

              topics[name] = { id, partitions, partitionsCount: rawPartitions.length }
            }

            this.#metadata = {
              id: metadata.clusterId!,
              brokers,
              topics,
              lastUpdate: Date.now()
            }

            deduplicateCallback(null, this.#metadata)
          },
          0,
          this.options.retries
        )
      },
      callback
    )
  }

  protected clearMetadata (): void {
    this.#metadata = undefined
  }

  protected parseBroker (broker: Broker | string, index: number): Broker {
    if (typeof broker === 'string') {
      if (broker.includes(':')) {
        const [host, port] = broker.split(':')
        return { host, port: Number(port) }
      } else {
        return { host: broker, port: DEFAULT_PORT }
      }
    } else if (typeof broker.host !== 'string' || !broker.host) {
      throw new UserError(`/bootstrapBrokers/${index}/host must be a non-empty string.`)
    } else if (typeof broker.port !== 'number' || broker.port < 1 || broker.port > 65535) {
      throw new UserError(`/bootstrapBrokers/${index}/port must be a number between 1 and 65535.`)
    }

    return broker
  }

  protected performWithRetry<T>(
    errorMessage: string,
    operation: (callback: Callback<T>) => void,
    callback: CallbackWithPromise<T>,
    attempt: number,
    maxAttempts: number,
    errors: Error[] = [],
    shouldSkipRetry?: (e: Error) => boolean
  ): void | Promise<T> {
    operation((error, result) => {
      if (error) {
        const genericError = error as GenericError
        const retriable = genericError.hasAny('code', NetworkError.code) || genericError.hasAny('canRetry', true)
        errors.push(error)

        if (attempt < maxAttempts && retriable && !shouldSkipRetry?.(error)) {
          setTimeout(() => {
            this.performWithRetry(errorMessage, operation, callback, attempt + 1, maxAttempts, errors, shouldSkipRetry)
          }, this.options.retryDelay)
        } else {
          callback(new MultipleErrors(errorMessage, errors), undefined as T)
        }

        return
      }

      callback(null, result!)
    })

    return callback[kCallbackPromise]
  }

  protected performDeduplicated<T>(
    operationId: string,
    operation: (callback: CallbackWithPromise<T>) => void,
    callback: CallbackWithPromise<T>
  ): void | Promise<T> {
    let inflights = this.#inflightDeduplications.get(operationId)

    if (!inflights) {
      inflights = []
      this.#inflightDeduplications.set(operationId, inflights)
    }

    inflights.push(callback)

    if (inflights.length === 1) {
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
    if (!this.options.strictOptions) {
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
