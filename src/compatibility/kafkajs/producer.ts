import type {
  Logger,
  Message as KafkaJSMessage,
  Offsets,
  ProducerBatch,
  ProducerConfig,
  ProducerRecord,
  RecordMetadata,
  Transaction as KafkaTransaction
} from './types.ts'
import type { ProduceResult, ProducerOptions } from '../../clients/producer/types.ts'
import type { MessageToProduce } from '../../protocol/records.ts'
import { connectionsApiChannel, connectionsQueueChannel } from '../../diagnostic.ts'
import { CompatibilityClient, call, retryOptions } from './common.ts'
import { CompressionTypes, type CompressionType } from './constants.ts'
import { KafkaJSError, KafkaJSNonRetriableError } from './errors.ts'
import { createLogger } from './logger.ts'
import { normalizeMessage, toPartitionMetadata } from './partitioners.ts'
import { KafkaJSAdminBridge } from './admin-native.ts'
import { KafkaJSProducerBridge, type KafkaJSSendOptions } from './producer-native.ts'
import { kKafkaJSProduceTimeout } from './symbols.ts'

const producerEvents = {
  CONNECT: 'producer.connect',
  DISCONNECT: 'producer.disconnect',
  REQUEST: 'producer.network.request',
  REQUEST_TIMEOUT: 'producer.network.request_timeout',
  REQUEST_QUEUE_SIZE: 'producer.network.request_queue_size'
} as const

const compressions = {
  [CompressionTypes.None]: 'none',
  [CompressionTypes.GZIP]: 'gzip',
  [CompressionTypes.Snappy]: 'snappy',
  [CompressionTypes.LZ4]: 'lz4',
  [CompressionTypes.ZSTD]: 'zstd'
} as const

let partitionerWarningEmitted = false

type NativeTransaction = Awaited<ReturnType<KafkaJSProducerBridge['beginTransaction']>>

function validateTopicMessages (topicMessages: Array<{ topic: string; messages: KafkaJSMessage[] }> | undefined): void {
  for (const { topic, messages } of topicMessages ?? []) {
    if (!topic) {
      throw new KafkaJSNonRetriableError('Invalid topic')
    }
    if (!messages) {
      throw new KafkaJSNonRetriableError(`Invalid messages array [${messages}] for topic "${topic}"`)
    }
    const message = messages.find(message => message.value === undefined)
    if (message) {
      throw new KafkaJSNonRetriableError(
        `Invalid message without value for topic "${topic}": ${JSON.stringify(message)}`
      )
    }
  }
}

function validateAcks (acks: number, idempotent: boolean): void {
  if (idempotent && acks !== -1) {
    throw new KafkaJSNonRetriableError(
      "Not requiring ack for all messages invalidates the idempotent producer's EoS guarantees"
    )
  }
}

function records (result: ProduceResult): RecordMetadata[] {
  return (result.offsets ?? []).map(offset => {
    const metadata = offset as typeof offset & {
      errorCode?: number
      logAppendTime?: bigint
      logStartOffset?: bigint
    }
    return {
      topicName: metadata.topic,
      partition: metadata.partition,
      errorCode: metadata.errorCode ?? 0,
      baseOffset: metadata.offset.toString(),
      logAppendTime: (metadata.logAppendTime ?? -1n).toString(),
      logStartOffset: (metadata.logStartOffset ?? -1n).toString()
    }
  })
}

function sendOptions (record: ProducerRecord, timeout: number, usePartitioner: boolean): {
  messages: MessageToProduce<Buffer, Buffer, Buffer, Buffer>[]
  acks: number
  compression?: (typeof compressions)[CompressionType]
  [kKafkaJSProduceTimeout]: number
} {
  return {
    messages: record.messages.map(message => ({ topic: record.topic, ...normalizeMessage(message, usePartitioner) })),
    acks: record.acks ?? -1,
    compression: record.compression === undefined ? undefined : compressions[record.compression],
    [kKafkaJSProduceTimeout]: timeout
  }
}

export class Transaction implements KafkaTransaction {
  private readonly transaction: NativeTransaction
  private readonly ensureMetadata: (topics: string[]) => Promise<void>
  private readonly sendTransactionOffsets: (offsets: Offsets & { consumerGroupId: string }) => Promise<void>
  private readonly requestTimeout: number
  private readonly usePartitioner: boolean

  constructor (
    transaction: NativeTransaction,
    ensureMetadata: (topics: string[]) => Promise<void> = async () => {},
    requestTimeout = 30_000,
    usePartitioner = false,
    sendTransactionOffsets: (offsets: Offsets & { consumerGroupId: string }) => Promise<void> = async () => {
      throw new KafkaJSNonRetriableError('Transactional offset sender is not configured')
    }
  ) {
    this.transaction = transaction
    this.ensureMetadata = ensureMetadata
    this.requestTimeout = requestTimeout
    this.usePartitioner = usePartitioner
    this.sendTransactionOffsets = sendTransactionOffsets
  }

  private active (): void {
    if (!this.isActive()) {
      throw new KafkaJSNonRetriableError('Cannot continue to use transaction once ended')
    }
  }

  async send (record: ProducerRecord): Promise<RecordMetadata[]> {
    this.active()
    validateTopicMessages([{ topic: record.topic, messages: record.messages }])
    validateAcks(record.acks ?? -1, true)
    await this.ensureMetadata([record.topic])
    return records(await call(this.transaction.send(sendOptions(record, record.timeout ?? this.requestTimeout, this.usePartitioner))))
  }

  async sendBatch (batch: ProducerBatch): Promise<RecordMetadata[]> {
    this.active()
    validateTopicMessages(batch.topicMessages)
    validateAcks(batch.acks ?? -1, true)
    const messages = (batch.topicMessages ?? []).flatMap(entry =>
      entry.messages.map(message => ({ topic: entry.topic, ...normalizeMessage(message, this.usePartitioner) })))
    await this.ensureMetadata((batch.topicMessages ?? []).map(entry => entry.topic))
    return records(
      await call(
        this.transaction.send({
          messages,
          acks: batch.acks ?? -1,
          compression: batch.compression === undefined ? undefined : compressions[batch.compression],
          [kKafkaJSProduceTimeout]: batch.timeout ?? this.requestTimeout
        } as KafkaJSSendOptions)
      )
    )
  }

  async sendOffsets (offsets: Offsets & { consumerGroupId: string }): Promise<void> {
    this.active()
    await this.sendTransactionOffsets(offsets)
  }

  async commit (): Promise<void> {
    this.active()
    await call(this.transaction.commit())
  }

  async abort (): Promise<void> {
    this.active()
    await call(this.transaction.abort())
  }

  isActive (): boolean {
    return !this.transaction.completed
  }
}

export class Producer extends CompatibilityClient {
  readonly events = producerEvents
  private native: KafkaJSProducerBridge
  private readonly nativeOptions: ProducerOptions<Buffer, Buffer, Buffer, Buffer>
  private readonly idempotent: boolean
  private readonly transactionalId?: string
  private readonly requestTimeout: number
  private readonly usePartitioner: boolean
  private readonly log: Logger
  private state: 'disconnected' | 'connecting' | 'connected' | 'disconnecting' = 'disconnected'
  private connectPromise?: Promise<void>
  private disconnectPromise?: Promise<void>
  private activeTransaction?: Transaction
  private transactionBridge?: KafkaJSAdminBridge
  private readonly diagnosticContext = {}
  private diagnosticsSubscribed = false
  private readonly onRequestDiagnostic = (rawDiagnostic: unknown): void => {
    const diagnostic = rawDiagnostic as Record<string, unknown>
    const connection = diagnostic.connection as { context?: unknown } | undefined
    if (connection?.context === this.diagnosticContext) {
      this.emit(this.events.REQUEST, {
        apiKey: diagnostic.apiKey,
        apiName: diagnostic.apiName,
        apiVersion: diagnostic.apiVersion,
        broker: diagnostic.broker,
        clientId: diagnostic.clientId,
        correlationId: diagnostic.correlationId,
        createdAt: diagnostic.createdAt,
        sentAt: diagnostic.sentAt,
        pendingDuration: diagnostic.pendingDuration,
        duration: diagnostic.duration,
        size: diagnostic.size
      })
    }
  }

  private readonly onRequestTimeoutDiagnostic = (rawDiagnostic: unknown): void => {
    const diagnostic = rawDiagnostic as {
      connection?: { context?: unknown }
      apiKey?: number
      apiName?: string
      apiVersion?: number
      broker?: string
      clientId?: string
      correlationId?: number
      createdAt?: number
      sentAt?: number
      pendingDuration?: number
      error?: Record<string, unknown> & { code?: string }
    }
    if (diagnostic.connection?.context === this.diagnosticContext && diagnostic.error?.code === 'PLT_KFK_TIMEOUT') {
      this.emit(this.events.REQUEST_TIMEOUT, {
        apiKey: diagnostic.apiKey,
        apiName: diagnostic.apiName,
        apiVersion: diagnostic.apiVersion,
        broker: diagnostic.broker ?? diagnostic.error.broker,
        clientId: diagnostic.clientId ?? diagnostic.error.clientId,
        correlationId: diagnostic.correlationId ?? diagnostic.error.correlationId,
        createdAt: diagnostic.createdAt ?? diagnostic.error.createdAt,
        sentAt: diagnostic.sentAt ?? diagnostic.error.sentAt,
        pendingDuration: diagnostic.pendingDuration ?? diagnostic.error.pendingDuration
      })
    }
  }

  private readonly onQueueDiagnostic = (rawDiagnostic: unknown): void => {
    const diagnostic = rawDiagnostic as {
      connection?: { context?: unknown; host?: string; port?: number }
      queueSize?: number
    }
    if (diagnostic.connection?.context === this.diagnosticContext) {
      this.emit(this.events.REQUEST_QUEUE_SIZE, {
        broker: `${diagnostic.connection.host}:${diagnostic.connection.port}`,
        clientId: this.native.clientId,
        queueSize: diagnostic.queueSize
      })
    }
  }

  constructor (baseOptions: ProducerOptions<Buffer, Buffer, Buffer, Buffer>, config: ProducerConfig, log: Logger) {
    super()
    this.log = log.namespace('Producer')
    this.idempotent = config.idempotent === true
    this.transactionalId = config.transactionalId
    this.requestTimeout = baseOptions.requestTimeout ?? 30_000
    this.usePartitioner = config.createPartitioner !== undefined
    if (this.idempotent && config.retry?.retries !== undefined && config.retry.retries < 1) {
      throw new KafkaJSNonRetriableError('Idempotent producer must allow retries to protect against transient errors')
    }
    if (this.idempotent && config.retry?.retries !== undefined && config.retry.retries < Number.MAX_SAFE_INTEGER) {
      this.log.warn('Limiting retries for the idempotent producer may invalidate EoS guarantees')
    }
    if (!config.createPartitioner && !partitionerWarningEmitted && process.env.KAFKAJS_NO_PARTITIONER_WARNING == null) {
      partitionerWarningEmitted = true
      this.log.warn(
        'KafkaJS v2.0.0 switched default partitioner. To retain the same partitioning behavior as in previous versions, create the producer with the option "createPartitioner: Partitioners.LegacyPartitioner". See the migration guide at https://kafka.js.org/docs/migration-guide-v2.0.0#producer-new-default-partitioner for details. Silence this warning by setting the environment variable "KAFKAJS_NO_PARTITIONER_WARNING=1"'
      )
    }
    let partitioner: ProducerOptions<Buffer, Buffer, Buffer, Buffer>['partitioner']
    if (config.createPartitioner) {
      const kafkaPartitioner = config.createPartitioner()
      partitioner = (message, _key, { metadata }) => {
        const topicMetadata = metadata?.topics.get(message.topic)
        const kafkaJSMessage = (message.metadata as { kafkaJSMessage?: KafkaJSMessage } | undefined)?.kafkaJSMessage
        const partition = kafkaPartitioner({
          topic: message.topic,
          message: kafkaJSMessage ?? (message as never),
          partitionMetadata: toPartitionMetadata(topicMetadata?.partitions ?? [])
        })
        if (
          !Number.isInteger(partition) ||
          partition < 0 ||
          (topicMetadata && partition >= topicMetadata.partitionsCount)
        ) {
          throw new KafkaJSNonRetriableError(`Invalid partitioner result: ${partition}`)
        }
        return partition
      }
    }
    this.nativeOptions = {
      ...baseOptions,
      ...retryOptions(config.retry),
      context: this.diagnosticContext,
      metadataMaxAge: config.metadataMaxAge ?? 300_000,
      autocreateTopics: config.allowAutoTopicCreation ?? true,
      maxInflights: config.maxInFlightRequests ?? Number.MAX_SAFE_INTEGER,
      transactionalId: config.transactionalId,
      timeout: config.transactionTimeout ?? baseOptions.timeout,
      idempotent: this.idempotent || config.transactionalId !== undefined,
      partitioner
    }
    this.native = this.createNative()
  }

  private createNative (): KafkaJSProducerBridge {
    return new KafkaJSProducerBridge({ ...this.nativeOptions })
  }

  async connect (): Promise<void> {
    if (this.state === 'connecting') {
      return this.connectPromise
    }
    if (this.state === 'disconnecting') {
      await this.disconnectPromise
    }
    this.connectPromise = (async () => {
      this.state = 'connecting'
      if (!this.diagnosticsSubscribed) {
        connectionsApiChannel.asyncEnd.subscribe(this.onRequestDiagnostic)
        connectionsApiChannel.error.subscribe(this.onRequestTimeoutDiagnostic)
        connectionsQueueChannel.subscribe(this.onQueueDiagnostic)
        this.diagnosticsSubscribed = true
      }
      if (this.native.closed) {
        this.native = this.createNative()
      }
      try {
        await call(this.native.metadata({ topics: [] }))
        this.connected = true
        this.state = 'connected'
        this.emit(this.events.CONNECT)
      } catch (error) {
        connectionsApiChannel.asyncEnd.unsubscribe(this.onRequestDiagnostic)
        connectionsApiChannel.error.unsubscribe(this.onRequestTimeoutDiagnostic)
        connectionsQueueChannel.unsubscribe(this.onQueueDiagnostic)
        this.diagnosticsSubscribed = false
        this.connected = false
        this.state = 'disconnected'
        throw error
      }
    })()
    return this.connectPromise
  }

  async disconnect (): Promise<void> {
    if (this.state === 'disconnecting') {
      return this.disconnectPromise
    }
    if (this.state === 'connecting') {
      await this.connectPromise
    }
    this.disconnectPromise = (async () => {
      this.state = 'disconnecting'
      let disconnected = false
      try {
        await Promise.all([
          this.native.resetConnections(),
          this.transactionBridge?.close() as Promise<void> | undefined
        ])
        this.transactionBridge = undefined
        disconnected = true
      } finally {
        if (this.diagnosticsSubscribed) {
          connectionsApiChannel.asyncEnd.unsubscribe(this.onRequestDiagnostic)
          connectionsApiChannel.error.unsubscribe(this.onRequestTimeoutDiagnostic)
          connectionsQueueChannel.unsubscribe(this.onQueueDiagnostic)
          this.diagnosticsSubscribed = false
        }
        this.connected = false
        this.state = 'disconnected'
        if (disconnected) {
          this.emit(this.events.DISCONNECT)
        }
      }
    })()
    return this.disconnectPromise
  }

  isIdempotent (): boolean {
    return this.idempotent
  }

  async send (record: ProducerRecord): Promise<RecordMetadata[]> {
    validateTopicMessages([{ topic: record.topic, messages: record.messages }])
    validateAcks(record.acks ?? -1, this.idempotent)
    this.validateConnected()
    await this.ensureMetadata([record.topic])
    return records(await call(this.native.send(sendOptions(record, record.timeout ?? this.requestTimeout, this.usePartitioner))))
  }

  async sendBatch (batch: ProducerBatch): Promise<RecordMetadata[]> {
    validateTopicMessages(batch.topicMessages)
    validateAcks(batch.acks ?? -1, this.idempotent)
    this.validateConnected()
    const messages = (batch.topicMessages ?? []).flatMap(entry =>
      entry.messages.map(message => ({ topic: entry.topic, ...normalizeMessage(message, this.usePartitioner) })))
    await this.ensureMetadata((batch.topicMessages ?? []).map(entry => entry.topic))
    return records(
      await call(
        this.native.send({
          messages,
          acks: batch.acks ?? -1,
          compression: batch.compression === undefined ? undefined : compressions[batch.compression],
          [kKafkaJSProduceTimeout]: batch.timeout ?? this.requestTimeout
        } as KafkaJSSendOptions)
      )
    )
  }

  async transaction (): Promise<Transaction> {
    if (!this.transactionalId) {
      throw new KafkaJSNonRetriableError('Must provide transactional id for transactional producer')
    }
    this.validateConnected()
    if (this.activeTransaction?.isActive()) {
      throw new KafkaJSNonRetriableError(
        'There is already an ongoing transaction for this producer. Please end the transaction before beginning another.'
      )
    }
    this.activeTransaction = new Transaction(
      await call(this.native.beginTransaction()),
      topics => this.ensureMetadata(topics),
      this.requestTimeout,
      this.usePartitioner,
      offsets => this.sendOffsetsToTransaction(offsets)
    )
    return this.activeTransaction
  }

  logger (): Logger {
    return this.log
  }

  private validateConnected (): void {
    if (this.state === 'disconnecting') {
      throw new KafkaJSNonRetriableError(
        "The producer is disconnecting; therefore, it can't safely accept messages anymore"
      )
    }
    if (this.state !== 'connected') {
      throw new KafkaJSError('The producer is disconnected')
    }
  }

  private async ensureMetadata (topics: string[]): Promise<void> {
    if (topics.length > 0) {
      await call(this.native.metadata({ topics: [...new Set(topics)] }))
    }
  }

  private async sendOffsetsToTransaction (offsets: Offsets & { consumerGroupId: string }): Promise<void> {
    if (!offsets.consumerGroupId) {
      throw new KafkaJSNonRetriableError('consumerGroupId must be a non-empty string')
    }
    const producerId = this.native.producerId
    const producerEpoch = this.native.producerEpoch
    if (producerId === undefined || producerEpoch === undefined || !this.transactionalId) {
      throw new KafkaJSNonRetriableError('Transactional producer is not initialized')
    }
    let topics
    try {
      topics = offsets.topics.map(topic => ({
        name: topic.topic,
        partitions: topic.partitions.map(partition => ({
          partitionIndex: partition.partition,
          committedOffset: BigInt(partition.offset),
          committedLeaderEpoch: -1,
          committedMetadata: null
        }))
      }))
    } catch (error) {
      throw new KafkaJSNonRetriableError('Invalid transactional offset', { cause: error as Error })
    }
    this.transactionBridge ??= new KafkaJSAdminBridge(this.nativeOptions)
    await call(
      this.transactionBridge.sendOffsetsToTransactionRaw(
        this.transactionalId,
        producerId,
        producerEpoch,
        offsets.consumerGroupId,
        topics
      )
    )
  }
}

export function defaultProducerLogger (): Logger {
  return createLogger()
}
