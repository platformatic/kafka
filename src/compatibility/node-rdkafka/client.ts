import { EventEmitter } from 'node:events'
import { type ClusterMetadata } from '../../clients/base/types.ts'
import { compatibilityError, toLibrdKafkaError } from './errors.ts'
import { type OauthBearerToken } from './config.ts'
import { type EventListenerMap, type Metadata, type NodeCallback, type RdkafkaConfig, type TopicPartitionOffset, type WatermarkOffsets } from './types.ts'

export const kClientRecreated = Symbol('plt.kafka.nodeRdkafka.clientRecreated')

interface PlatformClient {
  close (callback: (error: Error | null) => void): void
  isConnected (): boolean
  metadata (options: { topics?: string[]; forceUpdate?: boolean }, callback: (error: Error | null, metadata?: ClusterMetadata) => void): void
}

interface OffsetClient extends PlatformClient {
  listOffsets (options: { topics: string[]; partitions?: Record<string, number[]>; timestamp?: bigint }, callback: (error: Error | null, offsets?: Map<string, bigint[]>) => void): void
  listOffsetsWithTimestamps (options: { topics: string[]; partitions?: Record<string, number[]>; timestamp?: bigint }, callback: (error: Error | null, offsets?: Map<string, Map<number, { offset: bigint }>>) => void): void
}

export abstract class Client extends EventEmitter {
  protected client: PlatformClient
  protected readonly globalConfig: RdkafkaConfig
  protected metadataCache: Metadata | null = null
  protected connected = false
  protected connecting = false
  protected openedAt = 0
  protected lastError: Error | null = null
  protected readonly oauthBearerToken: OauthBearerToken | null
  readonly #clientFactory: (() => PlatformClient) | null
  #clientClosed = false

  constructor (client: PlatformClient, config: RdkafkaConfig, oauthBearerToken: OauthBearerToken | null = null, clientFactory: (() => PlatformClient) | null = null) {
    super()
    this.client = client
    this.globalConfig = { ...config }
    this.oauthBearerToken = oauthBearerToken
    this.#clientFactory = clientFactory
  }

  on<Event extends keyof EventListenerMap> (event: Event, listener: (...args: EventListenerMap[Event]) => void): this
  on (event: string | symbol, listener: (...args: unknown[]) => void): this
  on (event: string | symbol, listener: (...args: unknown[]) => void): this {
    return super.on(event, listener)
  }

  once<Event extends keyof EventListenerMap> (event: Event, listener: (...args: EventListenerMap[Event]) => void): this
  once (event: string | symbol, listener: (...args: unknown[]) => void): this
  once (event: string | symbol, listener: (...args: unknown[]) => void): this {
    return super.once(event, listener)
  }

  connect (options: { topic?: string; allTopics?: boolean; timeout?: number } | null = {}, callback?: NodeCallback<Metadata>): this {
    if (this.connected) {
      queueMicrotask(() => callback?.(null, this.metadataCache ?? undefined))
      return this
    }

    if (this.connecting) {
      this.once('ready', (_info, metadata: Metadata) => callback?.(null, metadata))
      return this
    }

    if (this.#clientClosed && this.#clientFactory) {
      try {
        this.client = this.#clientFactory()
        this.#clientClosed = false
        this.emit(kClientRecreated, this.client)
      } catch (error) {
        const converted = toLibrdKafkaError(error)
        this.lastError = converted
        queueMicrotask(() => {
          this.emit('connection.failure', converted, { connectionOpened: this.openedAt })
          callback?.(converted)
        })
        return this
      }
    }

    this.connecting = true
    this.#getMetadata(options ?? {}, (error, metadata) => {
      this.connecting = false
      if (error || !metadata) {
        const converted = toLibrdKafkaError(error)
        this.lastError = converted
        this.emit('connection.failure', converted, { connectionOpened: this.openedAt })
        callback?.(converted)
        return
      }

      this.connected = true
      this.openedAt = Date.now()
      const info = { name: metadata.orig_broker_name }
      this.emit('ready', info, metadata)
      callback?.(null, metadata)
    })
    return this
  }

  disconnect (timeoutOrCallback?: number | NodeCallback<{ connectionOpened: number }>, callback?: NodeCallback<{ connectionOpened: number }>): this {
    if (typeof timeoutOrCallback === 'function') {
      callback = timeoutOrCallback
    }
    const metrics = { connectionOpened: this.openedAt }
    if (!this.connected && !this.connecting) {
      if (this.lastError && !this.#clientClosed) {
        this.client.close(error => {
          if (error) {
            callback?.(toLibrdKafkaError(error))
            return
          }
          this.#clientClosed = true
          this.emit('disconnected', metrics)
          callback?.(null, metrics)
        })
        return this
      }
      queueMicrotask(() => callback?.(null, metrics))
      return this
    }

    this.client.close(error => {
      this.connected = false
      this.connecting = false
      if (error) {
        const converted = toLibrdKafkaError(error)
        this.lastError = converted
        callback?.(converted)
        return
      }
      this.#clientClosed = true
      this.emit('disconnected', metrics)
      callback?.(null, metrics)
    })
    return this
  }

  isConnected (): boolean {
    return this.connected && this.client.isConnected()
  }

  connectedTime (): number {
    return this.connected ? Date.now() - this.openedAt : 0
  }

  getLastError (): Error | null {
    return this.lastError
  }

  getClient (): PlatformClient {
    return this.client
  }

  setOauthBearerToken (token: string): this {
    if (!this.oauthBearerToken) {
      throw compatibilityError('OAuth bearer authentication is not configured')
    }
    if (typeof token !== 'string' || token.length === 0) {
      throw compatibilityError('OAuth bearer token must be a non-empty string', -186)
    }
    this.oauthBearerToken.value = token
    return this
  }

  getMetadata (options: { topic?: string; allTopics?: boolean; timeout?: number } | null = {}, callback?: NodeCallback<Metadata>): this {
    if (!this.isConnected()) {
      queueMicrotask(() => callback?.(compatibilityError('Client is disconnected', -172)))
      return this
    }
    this.#getMetadata(options, callback)
    return this
  }

  offsetsForTimes (partitions: TopicPartitionOffset[], timeout: number, callback?: NodeCallback<TopicPartitionOffset[]>): void
  offsetsForTimes (partitions: TopicPartitionOffset[], callback?: NodeCallback<TopicPartitionOffset[]>): void
  offsetsForTimes (partitions: TopicPartitionOffset[], timeoutOrCallback?: number | NodeCallback<TopicPartitionOffset[]>, callback?: NodeCallback<TopicPartitionOffset[]>): void {
    const timeout = typeof timeoutOrCallback === 'number' ? timeoutOrCallback : 1000
    if (typeof timeoutOrCallback === 'function') {
      callback = timeoutOrCallback
    }
    if (!this.isConnected()) {
      queueMicrotask(() => callback?.(compatibilityError('Client is disconnected', -172)))
      return
    }

    const byTimestamp = new Map<number, Array<{ index: number, partition: TopicPartitionOffset }>>()
    for (const [index, partition] of partitions.entries()) {
      const timestamp = partition.offset ?? -1
      const grouped = byTimestamp.get(timestamp) ?? []
      grouped.push({ index, partition })
      byTimestamp.set(timestamp, grouped)
    }
    const offsetClient = this.client as OffsetClient
    this.#withTimeout(timeout, callback, done => {
      const result = Array<TopicPartitionOffset>(partitions.length)
      Promise.all(Array.from(byTimestamp, ([timestamp, grouped]) => new Promise<void>((resolve, reject) => {
        const requested = new Map<string, number[]>()
        for (const { partition } of grouped) {
          const topicPartitions = requested.get(partition.topic) ?? []
          topicPartitions.push(partition.partition)
          requested.set(partition.topic, topicPartitions)
        }
        offsetClient.listOffsetsWithTimestamps({
          topics: [...requested.keys()],
          partitions: Object.fromEntries(requested),
          timestamp: BigInt(timestamp)
        }, (error, offsets) => {
          if (error) {
            reject(error)
          } else {
            for (const { index, partition } of grouped) {
              result[index] = {
                ...partition,
                offset: Number(offsets?.get(partition.topic)?.get(partition.partition)?.offset ?? -1n)
              }
            }
            resolve()
          }
        })
      }))).then(() => done(null, result), error => done(error as Error))
    })
  }

  #getMetadata (options: { topic?: string; allTopics?: boolean; timeout?: number } | null, callback?: NodeCallback<Metadata>): void {
    const topics = options?.topic === undefined ? [] : [options.topic]
    this.#withTimeout(options?.timeout ?? 5000, callback, done => this.client.metadata({ topics, forceUpdate: true }, (error, metadata) => {
      if (error || !metadata) {
        done(error ?? compatibilityError('Metadata unavailable', -196))
        return
      }
      this.metadataCache = convertMetadata(metadata)
      done(null, this.metadataCache)
    }))
  }

  queryWatermarkOffsets (topic: string, partition: number, timeoutOrCallback: number | NodeCallback<WatermarkOffsets>, callback?: NodeCallback<WatermarkOffsets>): this {
    const timeout = typeof timeoutOrCallback === 'number' ? timeoutOrCallback : 1000
    if (typeof timeoutOrCallback === 'function') {
      callback = timeoutOrCallback
    }
    if (!this.isConnected()) {
      queueMicrotask(() => callback?.(compatibilityError('Client is disconnected', -172)))
      return this
    }
    const offsetClient = this.client as OffsetClient
    const query = (timestamp: bigint): Promise<bigint> => new Promise((resolve, reject) => {
      offsetClient.listOffsets({ topics: [topic], partitions: { [topic]: [partition] }, timestamp }, (error, offsets) => {
        if (error) {
          reject(error)
        } else {
          resolve(offsets?.get(topic)?.[partition] ?? 0n)
        }
      })
    })

    this.#withTimeout(timeout, callback, done => Promise.all([query(-2n), query(-1n)]).then(
      ([low, high]) => done(null, { lowOffset: Number(low), highOffset: Number(high) }),
      error => done(error as Error)
    ))
    return this
  }

  #withTimeout<T> (timeout: number, callback: NodeCallback<T> | undefined, operation: (done: (error: Error | null, value?: T) => void) => void): void {
    let completed = false
    const finish = (error: Error | null, value?: T): void => {
      if (completed) {
        return
      }
      completed = true
      clearTimeout(timer)
      if (error) {
        const converted = toLibrdKafkaError(error)
        this.lastError = converted
        callback?.(converted)
      } else {
        callback?.(null, value)
      }
    }
    const timer = setTimeout(() => finish(compatibilityError('Operation timed out', -185)), timeout)
    timer.unref()
    operation(finish)
  }
}

function convertMetadata (metadata: ClusterMetadata): Metadata {
  const brokers = Array.from(metadata.brokers, ([id, broker]) => ({ id, host: broker.host, port: broker.port }))
  const first = brokers[0]
  return {
    orig_broker_id: first?.id ?? -1,
    orig_broker_name: first ? `${first.host}:${first.port}` : '',
    brokers,
    topics: Array.from(metadata.topics, ([name, topic]) => ({
      name,
      partitions: topic.partitions.map((partition, id) => ({
        id,
        leader: partition.leader,
        replicas: [...partition.replicas],
        isrs: [...partition.isr]
      }))
    }))
  }
}
