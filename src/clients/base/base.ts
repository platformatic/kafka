import { type ValidateFunction } from 'ajv'
import { EventEmitter } from 'node:events'
import { type API, type Callback } from '../../apis/definitions.ts'
import * as apis from '../../apis/index.ts'
import {
  api as apiVersionsV3,
  type ApiVersionsResponse,
  type ApiVersionsResponseApi
} from '../../apis/metadata/api-versions-v3.ts'
import { type MetadataRequest, type MetadataResponse } from '../../apis/metadata/metadata-v12.ts'
import {
  baseApisChannel,
  baseMetadataChannel,
  createDiagnosticContext,
  notifyCreation,
  type ClientType
} from '../../diagnostic.ts'
import type { GenericError } from '../../errors.ts'
import { MultipleErrors, NetworkError, UnsupportedApiError, UserError } from '../../errors.ts'
import { ConnectionPool } from '../../network/connection-pool.ts'
import { type Broker, type ConnectionOptions } from '../../network/connection.ts'
import { kInstance } from '../../symbols.ts'
import { ajv, debugDump, loggers } from '../../utils.ts'
import { createPromisifiedCallback, kCallbackPromise, type CallbackWithPromise } from '../callbacks.ts'
import { type Metrics } from '../metrics.ts'
import {
  baseOptionsValidator,
  clientSoftwareName,
  clientSoftwareVersion,
  defaultBaseOptions,
  defaultPort,
  metadataOptionsValidator
} from './options.ts'
import { type BaseOptions, type ClusterMetadata, type ClusterTopicMetadata, type MetadataOptions } from './types.ts'

export const kClientId = Symbol('plt.kafka.base.clientId')
export const kBootstrapBrokers = Symbol('plt.kafka.base.bootstrapBrokers')
export const kApis = Symbol('plt.kafka.base.apis')
export const kGetApi = Symbol('plt.kafka.base.getApi')
export const kOptions = Symbol('plt.kafka.base.options')
export const kConnections = Symbol('plt.kafka.base.connections')
export const kFetchConnections = Symbol('plt.kafka.base.fetchCnnections')
export const kCreateConnectionPool = Symbol('plt.kafka.base.createConnectionPool')
export const kClosed = Symbol('plt.kafka.base.closed')
export const kListApis = Symbol('plt.kafka.base.listApis')
export const kMetadata = Symbol('plt.kafka.base.metadata')
export const kCheckNotClosed = Symbol('plt.kafka.base.checkNotClosed')
export const kClearMetadata = Symbol('plt.kafka.base.clearMetadata')
export const kParseBroker = Symbol('plt.kafka.base.parseBroker')
export const kPerformWithRetry = Symbol('plt.kafka.base.performWithRetry')
export const kPerformDeduplicated = Symbol('plt.kafka.base.performDeduplicated')
export const kValidateOptions = Symbol('plt.kafka.base.validateOptions')
export const kInspect = Symbol('plt.kafka.base.inspect')
export const kFormatValidationErrors = Symbol('plt.kafka.base.formatValidationErrors')
export const kPrometheus = Symbol('plt.kafka.base.prometheus')
export const kClientType = Symbol('plt.kafka.base.clientType')
export const kAfterCreate = Symbol('plt.kafka.base.afterCreate')

let currentInstance = 0

export class Base<OptionsType extends BaseOptions = BaseOptions> extends EventEmitter {
  // This is declared using a symbol (a.k.a protected/friend) to make it available in ConnectionPool and MessagesStream
  [kInstance]: number;

  // General status - Use symbols rather than JS private property to make them "protected" as in C++
  [kClientId]: string;
  [kClientType]: ClientType;
  [kBootstrapBrokers]: Broker[];
  [kApis]: ApiVersionsResponseApi[];
  [kOptions]: OptionsType;
  [kConnections]: ConnectionPool;
  [kClosed]: boolean;
  [kPrometheus]: Metrics | undefined

  #metadata: ClusterMetadata | undefined
  #inflightDeduplications: Map<string, CallbackWithPromise<any>[]>

  constructor (options: OptionsType) {
    super()
    this[kClientType] = 'base'
    this[kInstance] = currentInstance++
    this[kApis] = []

    // Validate options
    this[kOptions] = Object.assign({}, defaultBaseOptions as OptionsType, options) as OptionsType
    this[kValidateOptions](this[kOptions], baseOptionsValidator, '/options')
    this[kClientId] = options.clientId

    // Initialize bootstrap brokers
    this[kBootstrapBrokers] = []
    for (const broker of options.bootstrapBrokers) {
      this[kBootstrapBrokers].push(this[kParseBroker](broker))
    }

    // Initialize main connection pool
    this[kConnections] = this[kCreateConnectionPool]()
    this[kClosed] = false

    this.#inflightDeduplications = new Map()

    // Initialize metrics
    if (options.metrics) {
      this[kPrometheus] = options.metrics
    }
  }

  get instanceId (): number {
    return this[kInstance]
  }

  get clientId (): string {
    return this[kClientId]
  }

  get closed (): boolean {
    return this[kClosed] === true
  }

  get type (): ClientType {
    return this[kClientType]
  }

  emitWithDebug (section: string | null, name: string, ...args: any[]): boolean {
    if (!section) {
      return this.emit(name, ...args)
    }

    loggers[section]?.({ event: name, payload: args })
    return this.emit(`${section}:${name}`, ...args)
  }

  close (callback: CallbackWithPromise<void>): void
  close (): Promise<void>
  close (callback?: CallbackWithPromise<void>): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback<void>()
    }

    this[kClosed] = true
    this[kConnections].close(callback)

    return callback[kCallbackPromise]
  }

  listApis (callback: CallbackWithPromise<ApiVersionsResponseApi[]>): void
  listApis (): Promise<ApiVersionsResponseApi[]>
  listApis (callback?: CallbackWithPromise<ApiVersionsResponseApi[]>): void | Promise<ApiVersionsResponseApi[]> {
    if (!callback) {
      callback = createPromisifiedCallback<ApiVersionsResponseApi[]>()
    }

    baseApisChannel.traceCallback(
      this[kListApis],
      0,
      createDiagnosticContext({ client: this, operation: 'listApis' }),
      this,
      callback
    )

    return callback[kCallbackPromise]
  }

  metadata (options: MetadataOptions, callback: CallbackWithPromise<ClusterMetadata>): void
  metadata (options: MetadataOptions): Promise<ClusterMetadata>
  metadata (options: MetadataOptions, callback?: CallbackWithPromise<ClusterMetadata>): void | Promise<ClusterMetadata> {
    if (!callback) {
      callback = createPromisifiedCallback<ClusterMetadata>()
    }

    const validationError = this[kValidateOptions](options, metadataOptionsValidator, '/options', false)
    if (validationError) {
      callback(validationError, undefined as unknown as ClusterMetadata)
      return callback[kCallbackPromise]
    }

    baseMetadataChannel.traceCallback(
      this[kMetadata],
      1,
      createDiagnosticContext({ client: this, operation: 'metadata' }),
      this,
      options,
      callback
    )

    return callback[kCallbackPromise]
  }

  [kCreateConnectionPool] (): ConnectionPool {
    const pool = new ConnectionPool(this[kClientId], {
      ownerId: this[kInstance],
      ...(this[kOptions] as ConnectionOptions)
    })
    for (const event of ['connect', 'disconnect', 'failed', 'drain']) {
      pool.on(event, payload => this.emitWithDebug('client', `broker:${event}`, payload))
    }

    return pool
  }

  [kListApis] (callback: CallbackWithPromise<ApiVersionsResponseApi[]>): void {
    this[kPerformDeduplicated](
      'listApis',
      deduplicateCallback => {
        this[kPerformWithRetry]<ApiVersionsResponse>(
          'listApis',
          retryCallback => {
            this[kConnections].getFirstAvailable(this[kBootstrapBrokers], (error, connection) => {
              if (error) {
                retryCallback(error, undefined as unknown as ApiVersionsResponse)
                return
              }

              // We use V3 to be able to get APIS from Kafka 2.4.0+
              apiVersionsV3(connection, clientSoftwareName, clientSoftwareVersion, retryCallback)
            })
          },
          (error: Error | null, metadata) => {
            if (error) {
              deduplicateCallback(error, undefined as unknown as ApiVersionsResponseApi[])
              return
            }

            deduplicateCallback(null, metadata.apiKeys)
          },
          0
        )
      },
      callback
    )
  }

  [kMetadata] (options: MetadataOptions, callback: CallbackWithPromise<ClusterMetadata>): void {
    const metadataMaxAge = options.metadataMaxAge ?? this[kOptions].metadataMaxAge!

    const isStale =
      options.forceUpdate ||
      !this.#metadata ||
      Date.now() > this.#metadata.lastUpdate + metadataMaxAge ||
      options.topics.some(topic => !this.#metadata?.topics.has(topic))

    if (!isStale) {
      callback(null, this.#metadata!)
      return
    }

    const autocreateTopics = options.autocreateTopics ?? this[kOptions].autocreateTopics

    this[kPerformDeduplicated](
      'metadata',
      deduplicateCallback => {
        this[kPerformWithRetry]<MetadataResponse>(
          'metadata',
          retryCallback => {
            this[kConnections].getFirstAvailable(this[kBootstrapBrokers], (error, connection) => {
              if (error) {
                retryCallback(error, undefined as unknown as MetadataResponse)
                return
              }

              this[kGetApi]<MetadataRequest, MetadataResponse>('Metadata', (error, api) => {
                if (error) {
                  retryCallback(error, undefined as unknown as MetadataResponse)
                  return
                }

                api(connection, options.topics, autocreateTopics, true, retryCallback)
              })
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
              /* c8 ignore next 3 - Sometimes internal topics might be returned by Kafka */
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

              topics.set(name!, { id, partitions, partitionsCount: rawPartitions.length })
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

  [kCheckNotClosed] (callback: CallbackWithPromise<any>): boolean {
    if (this[kClosed]) {
      const error = new NetworkError('Client is closed.', { closed: true, instance: this[kInstance] })
      callback(error, undefined)
      return true
    }

    return false
  }

  [kClearMetadata] (): void {
    this.#metadata = undefined
  }

  [kParseBroker] (broker: Broker | string): Broker {
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

  [kPerformWithRetry]<ReturnType>(
    operationId: string,
    operation: (callback: Callback<ReturnType>) => void,
    callback: CallbackWithPromise<ReturnType>,
    attempt: number = 0,
    errors: Error[] = [],
    shouldSkipRetry?: (e: Error) => boolean
  ): void | Promise<ReturnType> {
    const retries = this[kOptions].retries!
    this.emitWithDebug('client', 'performWithRetry', operationId, attempt, retries)

    operation((error, result) => {
      if (error) {
        const genericError = error as GenericError
        const retriable = genericError.findBy?.('code', NetworkError.code) || genericError.findBy?.('canRetry', true)
        errors.push(error)

        if (attempt < retries && retriable && !shouldSkipRetry?.(error)) {
          setTimeout(() => {
            this[kPerformWithRetry](operationId, operation, callback, attempt + 1, errors, shouldSkipRetry)
          }, this[kOptions].retryDelay)
        } else {
          if (attempt === 0) {
            callback(error, undefined as ReturnType)
            return
          }

          callback(new MultipleErrors(`${operationId} failed ${attempt + 1} times.`, errors), undefined as ReturnType)
        }

        return
      }

      callback(null, result!)
    })

    return callback[kCallbackPromise]
  }

  [kPerformDeduplicated]<ReturnType>(
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

  [kGetApi]<RequestArguments extends Array<unknown>, ResponseType>(
    name: string,
    callback: Callback<API<RequestArguments, ResponseType>>
  ) {
    // Make sure we have APIs informations
    if (!this[kApis].length) {
      this[kListApis]((error, apis) => {
        if (error) {
          callback(error, undefined as unknown as API<RequestArguments, ResponseType>)
          return
        }

        this[kApis] = apis
        this[kGetApi](name, callback)
      })

      return
    }

    const api = this[kApis].find(api => api.name === name)

    if (!api) {
      callback(
        new UnsupportedApiError(`Unsupported API ${name}.`),
        undefined as unknown as API<RequestArguments, ResponseType>
      )
      return
    }

    const { minVersion, maxVersion } = api

    // Starting from the highest version, we need to find the first one that is supported
    for (let i = maxVersion; i >= minVersion; i--) {
      const apiName = (name.slice(0, 1).toLowerCase() + name.slice(1) + 'V' + i) as keyof typeof apis
      const candidate = apis[apiName] as unknown as { api: API<RequestArguments, ResponseType> }

      if (candidate) {
        callback(null, candidate.api)
        return
      }
    }

    callback(
      new UnsupportedApiError(`No usable implementation found for API ${name}.`, { minVersion, maxVersion }),
      undefined as unknown as API<RequestArguments, ResponseType>
    )
  }

  [kValidateOptions] (
    target: unknown,
    validator: ValidateFunction<unknown>,
    targetName: string,
    throwOnErrors: boolean = true
  ): Error | null {
    if (!this[kOptions].strict) {
      return null
    }

    const valid = validator(target)

    if (!valid) {
      const error = new UserError(this[kFormatValidationErrors](validator, targetName))

      if (throwOnErrors) {
        throw error
      }

      return error
    }

    return null
  }

  /* c8 ignore next 3 -- This is a private API used to debug during development */
  [kInspect] (...args: unknown[]): void {
    debugDump(`client:${this[kInstance]}`, ...args)
  }

  [kFormatValidationErrors] (validator: ValidateFunction<unknown>, targetName: string) {
    return ajv.errorsText(validator.errors, { dataVar: '$dataVar$' }).replaceAll('$dataVar$', targetName) + '.'
  }

  [kAfterCreate] (type: ClientType): void {
    this[kClientType] = type
    notifyCreation(type, this)
  }
}
