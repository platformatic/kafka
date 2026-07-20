import { type Transaction } from '../../clients/producer/transaction.ts'
import { Client, kClientRecreated } from './client.ts'
import { mapConsumerConfig, mapProducerConfig, type OauthBearerToken } from './config.ts'
import { CODES, compatibilityError, toLibrdKafkaError } from './errors.ts'
import { ProducerStream, type WriteStreamOptions } from './streams.ts'
import { type DeliveryReport, type MessageKey as PublicMessageKey, type MessageValue, type NodeCallback, type ProducerGlobalConfig, type ProducerTopicConfig, type RdkafkaConfig } from './types.ts'
import type { KafkaConsumer } from './consumer.ts'
import type { Message } from '../../protocol/records.ts'
import { NodeRdkafkaProducerBridge } from './producer-native.ts'

type MessageKey = Buffer | string | null | undefined
type MessageHeader = Record<string, string | Buffer>
export type Serializer = (value: unknown, callback?: (error: Error | null, value?: Buffer | string | null) => void) => Buffer | string | null | undefined | Promise<Buffer | string | null | undefined> | void
type DeliveryCallback = (error: Error | null, offset?: number | null) => void

interface QueuedDelivery {
  error: Error | null
  report: DeliveryReport
  callback?: DeliveryCallback
}

export class Producer extends Client {
  protected producer: NodeRdkafkaProducerBridge
  protected pending = 0
  protected transaction: Transaction<Buffer | string | null, Buffer | null, string, Buffer | string> | null = null
  protected defaultTopic: string | null
  protected defaultPartition: number
  protected deliveryReports: boolean
  readonly #maxQueueMessages: number
  #deliveryQueue: QueuedDelivery[] = []
  #pollTimer: NodeJS.Timeout | null = null
  #transactionsInitialized = false
  #transactionEnding = false

  constructor (config: ProducerGlobalConfig, topicConfig: ProducerTopicConfig = {}) {
    const internalConfig = config as RdkafkaConfig
    const internalTopicConfig = topicConfig as RdkafkaConfig
    const mergedConfig = { ...internalTopicConfig, ...internalConfig }
    const oauthBearerToken = createOauthBearerToken(internalConfig)
    const options = mapProducerConfig(internalConfig, internalTopicConfig, oauthBearerToken ?? undefined)
    options.serializers = {
      key: key => typeof key === 'string' ? Buffer.from(key) : key === null ? null as unknown as Buffer : key as Buffer | undefined,
      value: message => message as Buffer,
      headerKey: key => Buffer.from(key ?? ''),
      headerValue: header => typeof header === 'string' ? Buffer.from(header) : header
    }
    const offsetRequesterOptions = mapConsumerConfig({ ...internalConfig, 'group.id': 'node-rdkafka-offsets' }, {}, oauthBearerToken ?? undefined)
    const createProducer = (): NodeRdkafkaProducerBridge => new NodeRdkafkaProducerBridge(options, offsetRequesterOptions)
    const producer = createProducer()
    super(producer, internalConfig, oauthBearerToken, createProducer)
    this.producer = producer
    this.on(kClientRecreated, client => {
      this.producer = client as NodeRdkafkaProducerBridge
      this.transaction = null
      this.#transactionsInitialized = false
      this.#transactionEnding = false
    })
    this.defaultTopic = typeof mergedConfig.topic === 'string' ? mergedConfig.topic : null
    this.defaultPartition = typeof mergedConfig.partition === 'number' ? mergedConfig.partition : -1
    this.deliveryReports = !!internalConfig.dr_cb || !!internalConfig.dr_msg_cb
    this.#maxQueueMessages = typeof internalConfig['queue.buffering.max.messages'] === 'number'
      ? internalConfig['queue.buffering.max.messages']
      : 100_000
    if (typeof internalConfig.dr_cb === 'function') {
      this.on('delivery-report', internalConfig.dr_cb as (error: Error | null, report: DeliveryReport) => void)
    }
  }

  static createWriteStream (config: ProducerGlobalConfig, topicConfig: ProducerTopicConfig, options: WriteStreamOptions): ProducerStream {
    return new ProducerStream(new Producer(config, topicConfig), options)
  }

  produce (
    topic: string,
    partition: number | null | undefined,
    message: unknown,
    key?: unknown,
    timestamp?: number | null,
    opaque?: unknown,
    headers?: MessageHeader[]
  ): boolean | void {
    this.produceWithCallback(topic, partition, message, key, timestamp, opaque, headers)
    return true
  }

  protected produceWithCallback (
    topic: string,
    partition: number | null | undefined,
    message: unknown,
    key?: unknown,
    timestamp?: number | null,
    opaque?: unknown,
    headers?: MessageHeader[],
    callback?: DeliveryCallback
  ): void {
    if (!this.connected) {
      throw compatibilityError('Producer not connected', CODES.ERRORS.ERR__STATE)
    }
    if (!topic || typeof topic !== 'string') {
      throw new TypeError('"topic" must be a string')
    }

    if (message !== null && !Buffer.isBuffer(message)) {
      throw new TypeError('"message" must be a Buffer or null')
    }
    if (key !== null && key !== undefined && typeof key !== 'string' && !Buffer.isBuffer(key)) {
      throw new TypeError('"key" must be a string, Buffer, null, or undefined')
    }
    const selectedPartition = partition === null || partition === undefined || partition < 0 ? this.defaultPartition : partition
    if (selectedPartition >= 0) {
      const topicMetadata = this.metadataCache?.topics.find(candidate => candidate.name === topic)
      if (topicMetadata && selectedPartition >= topicMetadata.partitions.length) {
        throw compatibilityError(`Unknown partition ${selectedPartition} for topic ${topic}`, CODES.ERRORS.ERR__UNKNOWN_PARTITION)
      }
    }
    this.send(topic, selectedPartition, message, key as MessageKey, timestamp, opaque, headers, callback)
  }

  protected send (
    topic: string,
    partition: number | null | undefined,
    message: Buffer | null,
    key?: MessageKey,
    timestamp?: number | null,
    opaque?: unknown,
    headers?: MessageHeader[],
    callback?: DeliveryCallback
  ): void {
    if (this.pending + this.#deliveryQueue.length >= this.#maxQueueMessages) {
      throw compatibilityError('Local: Queue full', CODES.ERRORS.ERR__QUEUE_FULL)
    }
    const normalizedPartition = partition === null || partition === undefined || partition < 0 ? undefined : partition
    const normalizedHeaders: Record<string, Buffer | string> = {}
    for (const header of headers ?? []) {
      Object.assign(normalizedHeaders, header)
    }
    const request = {
      messages: [{
        topic: topic || this.defaultTopic!,
        partition: normalizedPartition,
        value: message,
        key: key ?? null,
        timestamp: timestamp === null || timestamp === undefined ? undefined : BigInt(timestamp),
        headers: normalizedHeaders,
        metadata: opaque
      }]
    }
    const report: DeliveryReport = {
      topic,
      partition: normalizedPartition ?? this.defaultPartition,
      offset: -1,
      size: message?.byteLength ?? 0,
      key,
      timestamp: timestamp ?? undefined,
      opaque
    }
    if (this.globalConfig.dr_msg_cb === true) {
      report.value = message
    }

    this.pending++
    let operation: Promise<{ offsets?: { partition: number; offset: bigint }[] }>
    try {
      operation = this.transaction ? this.transaction.send(request) : this.producer.send(request)
    } catch (error) {
      this.pending--
      throw error
    }
    operation.then(result => {
      this.pending--
      const offsetResult = result.offsets?.[0]
      report.partition = offsetResult?.partition ?? report.partition
      report.offset = Number(offsetResult?.offset ?? -1n)
      if (this.deliveryReports || callback) {
        this.#deliveryQueue.push({ error: null, report, callback })
      }
    }, error => {
      this.pending--
      const converted = toLibrdKafkaError(error)
      this.lastError = converted
      if (this.deliveryReports || callback) {
        this.#deliveryQueue.push({ error: converted, report, callback })
      }
    })
  }

  poll (): this {
    if (!this.connected) {
      throw compatibilityError('Producer not connected', CODES.ERRORS.ERR__STATE)
    }
    while (this.#deliveryQueue.length > 0) {
      const { error, report, callback } = this.#deliveryQueue.shift()!
      if (this.deliveryReports) {
        this.emit('delivery-report', error, report)
      }
      callback?.(error, error ? undefined : report.offset >= 0 ? report.offset : null)
    }
    return this
  }

  setPollInterval (interval: number): this {
    if (this.#pollTimer) {
      clearInterval(this.#pollTimer)
      this.#pollTimer = null
    }
    if (interval > 0) {
      this.#pollTimer = setInterval(() => {
        if (this.connected) {
          this.poll()
        }
      }, interval)
      this.#pollTimer.unref()
    }
    return this
  }

  flush (timeoutOrCallback: number | null | NodeCallback = 500, callback?: NodeCallback): this {
    if (!this.connected) {
      throw compatibilityError('Producer not connected', CODES.ERRORS.ERR__STATE)
    }
    const timeout = typeof timeoutOrCallback === 'number' ? timeoutOrCallback : 500
    if (typeof timeoutOrCallback === 'function') {
      callback = timeoutOrCallback
    }
    const started = Date.now()
    const check = (): void => {
      if (this.pending === 0) {
        this.poll()
        callback?.(null)
      } else if (Date.now() - started >= timeout) {
        callback?.(compatibilityError('Failed to flush producer before timeout', CODES.ERRORS.ERR__TIMED_OUT))
      } else {
        setTimeout(check, Math.min(10, timeout)).unref()
      }
    }
    queueMicrotask(check)
    return this
  }

  override disconnect (timeoutOrCallback?: number | NodeCallback<{ connectionOpened: number }>, callback?: NodeCallback<{ connectionOpened: number }>): this {
    const timeout = typeof timeoutOrCallback === 'number' ? timeoutOrCallback : 5000
    if (typeof timeoutOrCallback === 'function') {
      callback = timeoutOrCallback
    }
    if (this.#pollTimer) {
      clearInterval(this.#pollTimer)
      this.#pollTimer = null
    }
    if (!this.connected) {
      super.disconnect(callback)
      return this
    }
    this.flush(timeout, () => super.disconnect(callback))
    return this
  }

  initTransactions (timeoutOrCallback: number | NodeCallback = 5000, callback?: NodeCallback): void {
    const timeout = typeof timeoutOrCallback === 'number' ? timeoutOrCallback : 5000
    if (typeof timeoutOrCallback === 'function') {
      callback = timeoutOrCallback
    }
    this.#withTimeout(timeout, callback, done => {
      this.producer.initIdempotentProducer({}, error => {
        if (!error && done(error)) {
          this.#transactionsInitialized = true
          return
        }
        if (error) {
          done(error)
        }
      })
    })
  }

  beginTransaction (callback: NodeCallback): void {
    if (!this.#transactionsInitialized) {
      queueMicrotask(() => callback(compatibilityError('Transactions have not been initialized', CODES.ERRORS.ERR__STATE)))
      return
    }
    this.producer.beginTransaction({}, (error, transaction) => {
      if (!error && transaction) {
        this.transaction = transaction
      }
      callback(error ? toLibrdKafkaError(error) : null)
    })
  }

  commitTransaction (timeoutOrCallback: number | NodeCallback = 5000, callback?: NodeCallback): void {
    const timeout = typeof timeoutOrCallback === 'number' ? timeoutOrCallback : 5000
    if (typeof timeoutOrCallback === 'function') {
      callback = timeoutOrCallback
    }
    if (!this.transaction || this.#transactionEnding) {
      callback?.(compatibilityError('No active transaction', CODES.ERRORS.ERR__STATE))
      return
    }
    this.#transactionEnding = true
    this.#withTimeout(timeout, callback, (done, isCompleted) => {
      this.#waitForPending(timeout, error => {
        if (error) {
          this.#transactionEnding = false
          done(error)
          return
        }
        if (isCompleted()) {
          return
        }
        this.transaction!.commit(error => {
          if (!error && done(error)) {
            this.transaction = null
            this.#transactionEnding = false
            return
          }
          if (error) {
            this.#transactionEnding = false
            done(error)
          }
        })
      })
    })
  }

  abortTransaction (timeoutOrCallback: number | NodeCallback = 5000, callback?: NodeCallback): void {
    const timeout = typeof timeoutOrCallback === 'number' ? timeoutOrCallback : 5000
    if (typeof timeoutOrCallback === 'function') {
      callback = timeoutOrCallback
    }
    if (!this.transaction || this.#transactionEnding) {
      callback?.(compatibilityError('No active transaction', CODES.ERRORS.ERR__STATE))
      return
    }
    this.#transactionEnding = true
    this.#withTimeout(timeout, callback, (done, isCompleted) => {
      this.#waitForPending(timeout, error => {
        if (error) {
          this.#transactionEnding = false
          done(error)
          return
        }
        if (isCompleted()) {
          return
        }
        this.transaction!.abort(error => {
          if (!error && done(error)) {
            this.transaction = null
            this.#transactionEnding = false
            return
          }
          if (error) {
            this.#transactionEnding = false
            done(error)
          }
        })
      })
    })
  }

  sendOffsetsToTransaction (offsets: { topic: string; partition: number; offset: number }[], consumer: KafkaConsumer, timeoutOrCallback: number | NodeCallback = 5000, callback?: NodeCallback): void {
    const timeout = typeof timeoutOrCallback === 'number' ? timeoutOrCallback : 5000
    if (typeof timeoutOrCallback === 'function') {
      callback = timeoutOrCallback
    }
    if (!this.transaction) {
      callback?.(compatibilityError('No active transaction', CODES.ERRORS.ERR__STATE))
      return
    }
    if (!consumer.isConnected() || !consumer.consumer.memberId || consumer.consumer.coordinatorId === null || consumer.consumer.generationId === undefined) {
      callback?.(compatibilityError('Consumer group membership is not active', CODES.ERRORS.ERR__STATE))
      return
    }
    if (offsets.some(offset => !offset.topic || !Number.isInteger(offset.partition) || offset.partition < 0 || !Number.isSafeInteger(offset.offset) || offset.offset < 0)) {
      callback?.(compatibilityError('Transaction offsets must contain valid topic, partition, and offset values', CODES.ERRORS.ERR__INVALID_ARG))
      return
    }
    const transaction = this.transaction
    this.#withTimeout(timeout, callback, done => transaction.addConsumer(consumer.consumer, error => {
      if (error) {
        done(error)
        return
      }
      const pending = [...offsets]
      const addOffset = (): void => {
        const offset = pending.shift()
        if (!offset) {
          done(null)
          return
        }
        const message = {
          topic: offset.topic,
          partition: offset.partition,
          offset: BigInt(offset.offset - 1),
          metadata: {
            consumer: {
              groupId: consumer.consumer.groupId,
              generationId: consumer.consumer.generationId,
              memberId: consumer.consumer.memberId,
              coordinatorId: consumer.consumer.coordinatorId
            }
          }
        } as unknown as Message<Buffer, Buffer, Buffer, Buffer>
        transaction.addOffset(message, addError => {
          if (addError) {
            done(addError)
            return
          }
          addOffset()
        })
      }
      addOffset()
    }))
  }

  #waitForPending (timeout: number, callback: (error: Error | null) => void): void {
    const started = Date.now()
    const check = (): void => {
      if (this.pending === 0) {
        this.poll()
        callback(null)
      } else if (Date.now() - started >= timeout) {
        callback(compatibilityError('Failed to complete transaction before timeout', CODES.ERRORS.ERR__TIMED_OUT))
      } else {
        setTimeout(check, Math.min(10, timeout)).unref()
      }
    }
    check()
  }

  #withTimeout (timeout: number, callback: NodeCallback | undefined, operation: (done: (error: Error | null) => boolean, isCompleted: () => boolean) => void): void {
    let completed = false
    const finish = (error: Error | null): boolean => {
      if (completed) {
        return false
      }
      completed = true
      clearTimeout(timer)
      callback?.(error ? toLibrdKafkaError(error) : null)
      return true
    }
    const timer = setTimeout(() => finish(compatibilityError('Transaction operation timed out', CODES.ERRORS.ERR__TIMED_OUT)), timeout)
    timer.unref()
    operation(finish, () => completed)
  }
}

export class HighLevelProducer extends Producer {
  #keySerializer: Serializer = value => value as Buffer | string | null
  #valueSerializer: Serializer = value => value as Buffer | null

  constructor (config: ProducerGlobalConfig, topicConfig: ProducerTopicConfig = {}) {
    super({ ...config, dr_cb: true }, topicConfig)
    this.setPollInterval(100)
  }

  setKeySerializer (serializer: (key: unknown, callback: (error: Error | null, key?: PublicMessageKey) => void) => void): void
  setKeySerializer (serializer: (key: unknown) => PublicMessageKey | Promise<PublicMessageKey>): void
  setKeySerializer (serializer: unknown): void {
    this.#keySerializer = serializer as Serializer
  }

  setValueSerializer (serializer: (value: unknown, callback: (error: Error | null, value?: MessageValue) => void) => void): void
  setValueSerializer (serializer: (value: unknown) => MessageValue | Promise<MessageValue>): void
  setValueSerializer (serializer: unknown): void {
    this.#valueSerializer = serializer as Serializer
  }

  override produce (topic: string, partition: number | null | undefined, message: unknown, key: unknown, timestamp: number | null | undefined, callback: (error: Error | null, offset?: number | null) => void): void
  override produce (topic: string, partition: number | null | undefined, message: unknown, key: unknown, timestamp: number | null | undefined, headers: MessageHeader[], callback: (error: Error | null, offset?: number | null) => void): void
  override produce (
    topic: string,
    partition: number | null | undefined,
    message: unknown,
    key: unknown,
    timestamp: number | null | undefined,
    opaqueOrHeadersOrCallback?: unknown,
    headersOrCallback?: MessageHeader[] | ((error: Error | null, offset?: number | null) => void)
  ): void {
    const callback = typeof opaqueOrHeadersOrCallback === 'function'
      ? opaqueOrHeadersOrCallback as (error: Error | null, offset?: number | null) => void
      : typeof headersOrCallback === 'function' ? headersOrCallback : undefined
    const headers = Array.isArray(opaqueOrHeadersOrCallback)
      ? opaqueOrHeadersOrCallback as MessageHeader[]
      : Array.isArray(headersOrCallback) ? headersOrCallback : undefined
    if (!callback) {
      throw new TypeError('Callback must be a function')
    }
    this.pending++
    Promise.all([serialize(this.#valueSerializer, message), serialize(this.#keySerializer, key)]).then(([value, serializedKey]) => {
      this.pending--
      try {
        this.produceWithCallback(topic, partition, value, serializedKey, timestamp, undefined, headers, callback)
      } catch (error) {
        callback(toLibrdKafkaError(error))
      }
    }, error => {
      this.pending--
      setImmediate(() => callback(error as Error))
    })
  }
}

function serialize (serializer: Serializer, value: unknown): Promise<Buffer | string | null> {
  if (serializer.length > 1) {
    return new Promise((resolve, reject) => {
      try {
        serializer(value, (error, result) => {
          if (error) {
            reject(error)
          } else {
            resolve(result ?? null)
          }
        })
      } catch (error) {
        reject(serializeError(error, serializer, value))
      }
    })
  }
  try {
    return Promise.resolve(serializer(value) as Buffer | string | null | Promise<Buffer | string | null>)
  } catch (error) {
    return Promise.reject(serializeError(error, serializer, value))
  }
}

function serializeError (error: unknown, serializer: Serializer, value: unknown): Error {
  const result = new Error(`Could not serialize value: ${error instanceof Error ? error.message : String(error)}`) as Error & { value?: unknown; serializer?: Serializer }
  result.value = value
  result.serializer = serializer
  return result
}

function createOauthBearerToken (config: RdkafkaConfig): OauthBearerToken | null {
  const token = config['oauthbearer.token']
  const mechanism = config['sasl.mechanisms'] ?? config['sasl.mechanism']
  return typeof token === 'string' || (typeof mechanism === 'string' && mechanism.toUpperCase() === 'OAUTHBEARER')
    ? { value: typeof token === 'string' ? token : '' }
    : null
}
