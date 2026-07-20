import { setTimeout as sleep } from 'node:timers/promises'
import type {
  Batch as KafkaBatch,
  ConsumerConfig,
  ConsumerRunConfig,
  ConsumerSubscribeTopic,
  ConsumerSubscribeTopics,
  GroupDescription,
  KafkaMessage,
  Logger,
  OffsetsByTopicPartition,
  TopicPartitionOffset,
  TopicPartitionOffsetAndMetadata,
  TopicPartitions
} from './types.ts'
import { kSeekPartition, type MessagesStream } from '../../clients/consumer/messages-stream.ts'
import { MessagesStreamFallbackModes, MessagesStreamModes, type ConsumerOptions } from '../../clients/consumer/types.ts'
import type { BaseOptions } from '../../clients/base/types.ts'
import { kMembershipVersion } from '../../clients/consumer/consumer.ts'
import { connectionsApiChannel, connectionsQueueChannel, consumerFetchesChannel } from '../../diagnostic.ts'
import { CompatibilityClient, call, retryOptions } from './common.ts'
import { KafkaJSNonRetriableError, wrapError } from './errors.ts'
import { KafkaJSAdminBridge } from './admin-native.ts'
import {
  KafkaJSConsumerBridge,
  type KafkaJSConsumeOptions
} from './consumer-native.ts'
import type { KafkaJSFetchProgressMarker, KafkaJSNativeMessage as NativeMessage } from './messages-stream.ts'
import {
  kKafkaJSCommitMetadata,
  kKafkaJSCreateAssignments,
  kKafkaJSFallbackModes,
  kKafkaJSFetchBatch,
  kKafkaJSFetchProgress
} from './symbols.ts'
import { createPartitionAssignerOptions } from './partitioners.ts'

const consumerEvents = {
  HEARTBEAT: 'consumer.heartbeat',
  COMMIT_OFFSETS: 'consumer.commit_offsets',
  GROUP_JOIN: 'consumer.group_join',
  FETCH_START: 'consumer.fetch_start',
  FETCH: 'consumer.fetch',
  START_BATCH_PROCESS: 'consumer.start_batch_process',
  END_BATCH_PROCESS: 'consumer.end_batch_process',
  CONNECT: 'consumer.connect',
  DISCONNECT: 'consumer.disconnect',
  STOP: 'consumer.stop',
  CRASH: 'consumer.crash',
  REBALANCING: 'consumer.rebalancing',
  RECEIVED_UNSUBSCRIBED_TOPICS: 'consumer.received_unsubscribed_topics',
  REQUEST: 'consumer.network.request',
  REQUEST_TIMEOUT: 'consumer.network.request_timeout',
  REQUEST_QUEUE_SIZE: 'consumer.network.request_queue_size'
} as const

const EARLIEST_OFFSET = -2n
const LATEST_OFFSET = -1n

function partitionKey (topic: string, partition: number): string {
  return `${topic}:${partition}`
}

function kafkaMessage (message: NativeMessage): KafkaMessage {
  const headers: Record<string, Buffer | Buffer[]> = {}
  for (const [key, rawValue] of message.headers) {
    const name = Buffer.isBuffer(key) ? key.toString() : String(key)
    const value = Buffer.isBuffer(rawValue) ? rawValue : Buffer.from(String(rawValue))
    const previous = headers[name]
    if (previous === undefined) {
      headers[name] = value
    } else if (Array.isArray(previous)) {
      previous.push(value)
    } else {
      headers[name] = [previous, value]
    }
  }
  return {
    key: message.key ?? null,
    value: message.value ?? null,
    timestamp: message.timestamp.toString(),
    attributes: 0,
    offset: message.offset.toString(),
    headers
  }
}

class Batch implements KafkaBatch {
  readonly topic: string
  readonly partition: number
  readonly highWatermark: string
  readonly messages: KafkaMessage[]
  private readonly rawLastOffset: bigint

  constructor (topic: string, partition: number, messages: NativeMessage[], highWatermark: bigint, rawLastOffset: bigint) {
    this.topic = topic
    this.partition = partition
    this.highWatermark = highWatermark.toString()
    this.messages = messages.map(kafkaMessage)
    this.rawLastOffset = rawLastOffset
  }

  isEmpty (): boolean {
    return this.messages.length === 0
  }

  firstOffset (): string | null {
    return this.messages[0]?.offset ?? null
  }

  lastOffset (): string {
    return this.rawLastOffset.toString()
  }

  offsetLag (): string {
    return (BigInt(this.highWatermark) - BigInt(this.lastOffset()) - 1n).toString()
  }

  offsetLagLow (): string {
    if (this.isEmpty()) {
      return '0'
    }
    return (BigInt(this.highWatermark) - BigInt(this.firstOffset() ?? this.highWatermark) - 1n).toString()
  }
}

interface PendingBatch {
  topic: string
  partition: number
  messages: NativeMessage[]
  highWatermark: bigint
  lastOffset: bigint
  seekVersion: number
  membershipVersion?: number
}

export class Consumer extends CompatibilityClient {
  readonly events = consumerEvents
  private native: KafkaJSConsumerBridge
  private readonly nativeOptions: ConsumerOptions<Buffer, Buffer, Buffer, Buffer>
  private readonly baseOptions: BaseOptions
  private readonly subscriptions = new Map<string, boolean>()
  private stream?: MessagesStream<Buffer, Buffer, Buffer, Buffer>
  private runTask?: Promise<void>
  private running = false
  private restartStream = false
  private readonly pausedPartitions = new Map<string, Set<number> | undefined>()
  private readonly resumedWholeTopicPartitions = new Map<string, Set<number>>()
  private readonly pauseWaiters = new Set<() => void>()
  private readonly pendingSeeks = new Map<string, bigint>()
  private readonly seekVersions = new Map<string, number>()
  private readonly resolvedOffsets = new Map<string, bigint>()
  private readonly committedOffsets = new Map<string, bigint>()
  private resolvedCount = 0
  private lastCommit = Date.now()
  private autoCommitEnabled = true
  private readonly log: Logger
  private readonly restartOnFailure?: (error: Error) => Promise<boolean>
  private readonly restartDelay: number
  private readonly fetchStarts = new Map<bigint, number>()
  private fetchDiagnosticsSubscribed = false
  private readonly diagnosticContext = {}
  private requestDiagnosticsSubscribed = false
  private restartAbort?: AbortController
  private signalRunStarted?: () => void

  constructor (baseOptions: ConsumerOptions<Buffer, Buffer, Buffer, Buffer>, config: ConsumerConfig, log: Logger) {
    super()
    if (!config.groupId) {
      throw new KafkaJSNonRetriableError('Consumer groupId must be a non-empty string.')
    }
    const sessionTimeout = config.sessionTimeout ?? 30_000
    const heartbeatInterval = config.heartbeatInterval ?? 3_000
    if (heartbeatInterval >= sessionTimeout) {
      throw new KafkaJSNonRetriableError(
        `Consumer heartbeatInterval (${heartbeatInterval}) must be lower than sessionTimeout (${sessionTimeout}). It is recommended to set heartbeatInterval to approximately a third of the sessionTimeout.`
      )
    }
    this.log = log
    this.baseOptions = baseOptions
    this.restartOnFailure = config.retry?.restartOnFailure
    this.restartDelay = config.retry?.initialRetryTime ?? 300
    const partitionAssignerOptions = config.partitionAssigners?.length
      ? createPartitionAssignerOptions(config.partitionAssigners, config.groupId, log)
      : { createAssignments: undefined, protocols: [{ name: 'RoundRobinAssigner', version: 0 }] }
    const { createAssignments, ...groupOptions } = partitionAssignerOptions
    this.nativeOptions = {
      ...this.baseOptions,
      ...retryOptions(config.retry),
      groupId: config.groupId,
      metadataMaxAge: config.metadataMaxAge ?? 300_000,
      sessionTimeout,
      rebalanceTimeout: config.rebalanceTimeout ?? 60_000,
      heartbeatInterval,
      maxBytesPerPartition: config.maxBytesPerPartition ?? 1_048_576,
      minBytes: config.minBytes ?? 1,
      maxBytes: config.maxBytes ?? 10_485_760,
      maxWaitTime: config.maxWaitTimeInMs ?? 5_000,
      autocreateTopics: config.allowAutoTopicCreation ?? true,
      maxInflights: config.maxInFlightRequests ?? Number.MAX_SAFE_INTEGER,
      isolationLevel: config.readUncommitted ? 0 : 1,
      clientRack: config.rackId,
      context: this.diagnosticContext,
      ...groupOptions,
      ...(createAssignments ? { [kKafkaJSCreateAssignments]: createAssignments } : {})
    } as ConsumerOptions<Buffer, Buffer, Buffer, Buffer>
    this.native = this.createNative()
  }

  private createNative (): KafkaJSConsumerBridge {
    const native = new KafkaJSConsumerBridge(this.nativeOptions)
    this.forwardNativeEvents(native)
    return native
  }

  private forwardNativeEvents (native: KafkaJSConsumerBridge): void {
    native.on('consumer:heartbeat:end', payload => {
      this.emit(this.events.HEARTBEAT, {
        groupId: native.groupId,
        memberId: payload?.memberId ?? native.memberId ?? '',
        groupGenerationId: payload?.generationId ?? native.generationId
      })
    })
    native.on('consumer:group:rebalance', () => {
      this.emit(this.events.REBALANCING, { groupId: native.groupId, memberId: native.memberId ?? '' })
    })
    native.on('consumer:group:join', payload => {
      this.resolvedOffsets.clear()
      this.committedOffsets.clear()
      this.resolvedCount = 0
      this.lastCommit = Date.now()
      const memberAssignment = Object.fromEntries(
        (payload.assignments ?? []).map(({ topic, partitions }) => [topic, partitions])
      )
      this.emit(this.events.GROUP_JOIN, {
        duration: payload.duration ?? 0,
        groupId: native.groupId,
        isLeader: payload.isLeader ?? false,
        leaderId: payload.leaderId ?? '',
        groupProtocol: payload.protocol ?? '',
        memberId: payload.memberId,
        memberAssignment
      })
      const assignedTopics = Object.keys(memberAssignment)
      const topicsNotSubscribed = assignedTopics.filter(topic => !this.subscriptions.has(topic))
      if (topicsNotSubscribed.length > 0) {
        this.emit(this.events.RECEIVED_UNSUBSCRIBED_TOPICS, {
          groupId: native.groupId,
          generationId: payload.generationId ?? native.generationId,
          memberId: payload.memberId,
          assignedTopics,
          topicsSubscribed: Array.from(this.subscriptions.keys()),
          topicsNotSubscribed
        })
      }
      this.applyPendingSeeks().catch(error => this.stream?.destroy(error))
    })
  }

  async connect (): Promise<void> {
    if (!this.connected) {
      this.startRequestInstrumentation()
      this.startFetchInstrumentation()
      if (this.native.closed) {
        this.native = this.createNative()
      }
      try {
        await call(this.native.metadata({ topics: [] }))
        this.connected = true
        this.emit(this.events.CONNECT)
      } catch (error) {
        this.stopRequestInstrumentation()
        this.stopFetchInstrumentation()
        throw error
      }
    }
  }

  async disconnect (): Promise<void> {
    try {
      await this.stop()
      await call(this.native.close(true) as unknown as Promise<void>)
      if (this.connected) {
        this.connected = false
        this.emit(this.events.DISCONNECT)
      }
    } finally {
      this.stopRequestInstrumentation()
      this.stopFetchInstrumentation()
    }
  }

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
    const diagnostic = rawDiagnostic as Record<string, unknown>
    const connection = diagnostic.connection as { context?: unknown } | undefined
    const error = diagnostic.error as { code?: string } | undefined
    if (connection?.context === this.diagnosticContext && error?.code === 'PLT_KFK_TIMEOUT') {
      this.emit(this.events.REQUEST_TIMEOUT, {
        apiKey: diagnostic.apiKey,
        apiName: diagnostic.apiName,
        apiVersion: diagnostic.apiVersion,
        broker: diagnostic.broker,
        clientId: diagnostic.clientId,
        correlationId: diagnostic.correlationId,
        createdAt: diagnostic.createdAt,
        sentAt: diagnostic.sentAt,
        pendingDuration: diagnostic.pendingDuration
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

  private startRequestInstrumentation (): void {
    if (!this.requestDiagnosticsSubscribed) {
      connectionsApiChannel.asyncEnd.subscribe(this.onRequestDiagnostic)
      connectionsApiChannel.error.subscribe(this.onRequestTimeoutDiagnostic)
      connectionsQueueChannel.subscribe(this.onQueueDiagnostic)
      this.requestDiagnosticsSubscribed = true
    }
  }

  private stopRequestInstrumentation (): void {
    if (this.requestDiagnosticsSubscribed) {
      connectionsApiChannel.asyncEnd.unsubscribe(this.onRequestDiagnostic)
      connectionsApiChannel.error.unsubscribe(this.onRequestTimeoutDiagnostic)
      connectionsQueueChannel.unsubscribe(this.onQueueDiagnostic)
      this.requestDiagnosticsSubscribed = false
    }
  }

  async subscribe (subscription: ConsumerSubscribeTopics | ConsumerSubscribeTopic): Promise<void> {
    if (this.running) {
      throw new KafkaJSNonRetriableError('Cannot subscribe to topic while consumer is running')
    }
    if (!subscription || (!('topic' in subscription) && !('topics' in subscription))) {
      throw new KafkaJSNonRetriableError('Missing required argument "topics"')
    }
    const topics = 'topics' in subscription ? subscription.topics : [subscription.topic]
    if (!Array.isArray(topics)) {
      throw new KafkaJSNonRetriableError('Argument "topics" must be an array')
    }
    if (topics.length === 0) {
      throw new KafkaJSNonRetriableError('Invalid topics array, it cannot be empty')
    }
    for (const topic of topics) {
      if (typeof topic !== 'string' && !(topic instanceof RegExp)) {
        throw new KafkaJSNonRetriableError(
          `Invalid topic ${String(topic)} (${typeof topic}), the topic name has to be a String or a RegExp`
        )
      }
    }

    const metadata = topics.some(topic => topic instanceof RegExp)
      ? await call(this.native.metadata({ topics: [] }))
      : undefined
    for (const subscriptionTopic of topics) {
      if (typeof subscriptionTopic === 'string') {
        this.subscriptions.set(subscriptionTopic, subscription.fromBeginning ?? false)
      } else {
        for (const topic of metadata!.topics.keys()) {
          subscriptionTopic.lastIndex = 0
          if (subscriptionTopic.test(topic)) {
            this.subscriptions.set(topic, subscription.fromBeginning ?? false)
          }
        }
      }
    }
  }

  async run (config: ConsumerRunConfig = {}): Promise<void> {
    if (this.running) {
      return
    }
    this.startRequestInstrumentation()
    const concurrency = config.partitionsConsumedConcurrently ?? 1
    if (!Number.isSafeInteger(concurrency) || concurrency < 1) {
      throw new KafkaJSNonRetriableError('partitionsConsumedConcurrently must be a positive integer')
    }

    this.running = true
    this.restartStream = false
    this.resetRunState()
    this.autoCommitEnabled = config.autoCommit !== false
    const startup = Promise.withResolvers<void>()
    const signalStarted = () => {
      startup.resolve()
      if (this.signalRunStarted === signalStarted) {
        this.signalRunStarted = undefined
      }
    }
    this.signalRunStarted = signalStarted
    this.runTask = this.runLoop(config, concurrency, signalStarted)
    this.runTask.catch(() => {})
    await startup.promise
  }

  private async createStream (): Promise<MessagesStream<Buffer, Buffer, Buffer, Buffer>> {
    const topics = Array.from(this.subscriptions.keys())
    const stream = await call(
      this.native.consume({
        topics,
        mode: MessagesStreamModes.COMMITTED,
        fallbackMode: MessagesStreamFallbackModes.LATEST,
        [kKafkaJSFallbackModes]: Object.fromEntries(
          Array.from(this.subscriptions, ([topic, fromBeginning]) => [
            topic,
            fromBeginning ? MessagesStreamFallbackModes.EARLIEST : MessagesStreamFallbackModes.LATEST
          ])
        ),
        autocommit: false
      } as KafkaJSConsumeOptions)
    )
    await this.applyFromBeginning(stream)
    await this.applyPendingSeeks(stream)
    return stream
  }

  private async applyFromBeginning (stream: MessagesStream<Buffer, Buffer, Buffer, Buffer>): Promise<void> {
    const topics = Array.from(this.subscriptions, ([topic, fromBeginning]) => (fromBeginning ? topic : '')).filter(
      Boolean
    )
    if (topics.length === 0) {
      return
    }
    const assignments = this.native.assignments?.filter(assignment => topics.includes(assignment.topic)) ?? []
    if (assignments.length === 0) {
      return
    }
    const [committed, earliest] = await Promise.all([
      call(this.native.listCommittedOffsets({ topics: assignments })),
      call(this.native.listOffsets({ topics, timestamp: EARLIEST_OFFSET }))
    ])
    for (const assignment of assignments) {
      for (const partition of assignment.partitions) {
        if ((committed.get(assignment.topic)?.[partition] ?? -1n) < 0n) {
          const offset = earliest.get(assignment.topic)?.[partition]
          if (offset !== undefined) {
            stream.offsetsToFetch.set(partitionKey(assignment.topic, partition), offset)
          }
        }
      }
    }
  }

  private async runLoop (config: ConsumerRunConfig, concurrency: number, signalStarted: () => void): Promise<void> {
    while (this.running) {
      try {
        this.stream = await this.createStream()
        signalStarted()
        await this.pump(this.stream, config, concurrency)
        if (!this.restartStream) {
          break
        }
        this.restartStream = false
      } catch (error) {
        if (!this.running) {
          signalStarted()
          return
        }
        const wrapped = wrapError(error)
        let restart = wrapped.retriable
        if (restart && this.restartOnFailure) {
          try {
            restart = await this.restartOnFailure(wrapped)
          } catch (callbackError) {
            this.log.error('Caught error from restartOnFailure; restarting consumer', { error: callbackError })
          }
        }
        this.emit(this.events.CRASH, { error: wrapped, groupId: this.native.groupId, restart })
        await this.stream?.close().catch(() => {})
        await call(this.native.leaveGroup(false) as unknown as Promise<void>).catch(() => {})
        this.resetRunState()
        signalStarted()
        if (!restart) {
          this.running = false
          throw wrapped
        }
        if (!this.running) {
          return
        }
        const controller = new AbortController()
        this.restartAbort = controller
        try {
          await sleep(this.restartDelay, undefined, { signal: controller.signal })
        } catch (error) {
          if (!this.running && (error as { name?: string }).name === 'AbortError') {
            return
          }
          throw error
        } finally {
          if (this.restartAbort === controller) {
            this.restartAbort = undefined
          }
        }
      }
    }
  }

  private async pump (
    stream: MessagesStream<Buffer, Buffer, Buffer, Buffer>,
    config: ConsumerRunConfig,
    concurrency: number
  ): Promise<void> {
    const active = new Map<string, Promise<void>>()
    const failure = Promise.withResolvers<never>()
    const batches = this.batches(stream)
    try {
      while (true) {
        const next = await Promise.race([batches.next(), failure.promise])
        if (next.done) {
          break
        }
        const pending = next.value
        if (!this.running || this.restartStream) {
          break
        }
        const key = partitionKey(pending.topic, pending.partition)
        const previous = active.get(key)
        if (previous) {
          await previous
        }
        while (active.size >= concurrency) {
          await Promise.race([...active.values(), failure.promise])
        }
        const task = this.processBatch(pending, config)
          .catch(error => failure.reject(error))
          .finally(() => active.delete(key))
        active.set(key, task)
      }
      await Promise.race([Promise.all(active.values()), failure.promise])
    } catch (error) {
      await stream.close().catch(() => {})
      await Promise.allSettled(active.values())
      throw error
    }
  }

  private async * batches (stream: MessagesStream<Buffer, Buffer, Buffer, Buffer>): AsyncGenerator<PendingBatch> {
    const iterator = stream[Symbol.asyncIterator]()
    let pending: IteratorResult<NativeMessage | KafkaJSFetchProgressMarker> | undefined
    while (this.running && !this.restartStream) {
      const next = (await (pending ?? iterator.next())) as IteratorResult<NativeMessage | KafkaJSFetchProgressMarker>
      pending = undefined
      if (next.done) {
        return
      }
      if (kKafkaJSFetchProgress in next.value) {
        const progress = next.value[kKafkaJSFetchProgress]
        yield {
          topic: progress.topic,
          partition: progress.partition,
          messages: [],
          highWatermark: progress.highWatermark,
          lastOffset: progress.lastOffset,
          seekVersion: progress.seekVersion ?? 0,
          ...(progress.membershipVersion === undefined ? {} : { membershipVersion: progress.membershipVersion })
        }
        continue
      }

      const first = next.value
      const messages = [first]
      while (true) {
        const result = (await iterator.next()) as IteratorResult<NativeMessage | KafkaJSFetchProgressMarker>
        if (result.done) {
          yield {
            topic: first.topic,
            partition: first.partition,
            messages,
            highWatermark: first.offset + 1n,
            lastOffset: messages[messages.length - 1].offset,
            seekVersion: first[kKafkaJSFetchBatch]?.seekVersion ?? 0,
            membershipVersion: first[kKafkaJSFetchBatch]?.membershipVersion ?? 0
          }
          return
        }
        if (kKafkaJSFetchProgress in result.value) {
          const progress = result.value[kKafkaJSFetchProgress]
          if (progress.topic === first.topic && progress.partition === first.partition) {
            yield {
              topic: first.topic,
              partition: first.partition,
              messages,
              highWatermark: progress.highWatermark,
              lastOffset: progress.lastOffset,
              seekVersion: progress.seekVersion ?? 0,
              ...(progress.membershipVersion === undefined ? {} : { membershipVersion: progress.membershipVersion })
            }
            break
          }
          pending = result
          yield {
            topic: first.topic,
            partition: first.partition,
            messages,
            highWatermark: first.offset + 1n,
            lastOffset: messages[messages.length - 1].offset,
            seekVersion: first[kKafkaJSFetchBatch]?.seekVersion ?? 0,
            membershipVersion: first[kKafkaJSFetchBatch]?.membershipVersion ?? 0
          }
          break
        }
        const message = result.value
        if (message.topic !== first.topic || message.partition !== first.partition) {
          pending = result
          yield {
            topic: first.topic,
            partition: first.partition,
            messages,
            highWatermark: first.offset + 1n,
            lastOffset: messages[messages.length - 1].offset,
            seekVersion: first[kKafkaJSFetchBatch]?.seekVersion ?? 0,
            membershipVersion: first[kKafkaJSFetchBatch]?.membershipVersion ?? 0
          }
          break
        }
        messages.push(message)
      }
    }
  }

  private async processBatch (pending: PendingBatch, config: ConsumerRunConfig): Promise<void> {
    const key = partitionKey(pending.topic, pending.partition)
    await this.waitUntilResumed(pending.topic, pending.partition)
    if (this.isStale(pending, key)) {
      return
    }
    const seekOffset = this.pendingSeeks.get(key)
    let messages = pending.messages
    if (seekOffset !== undefined) {
      const index = messages.findIndex(message => message.offset >= seekOffset)
      if (index === -1) {
        return
      }
      messages = messages.slice(index)
      this.pendingSeeks.delete(key)
    }
    const batch = new Batch(pending.topic, pending.partition, messages, pending.highWatermark, pending.lastOffset)
    const started = Date.now()
    const eventPayload = this.batchEvent(batch)
    this.emit(this.events.START_BATCH_PROCESS, eventPayload)
    try {
      if (messages.length === 0) {
        this.resolve(pending.topic, pending.partition, pending.lastOffset)
      } else if (config.eachMessage) {
        for (const message of messages) {
          if (!this.running || this.isPaused(message.topic, message.partition)) {
            break
          }
          await config.eachMessage({
            topic: message.topic,
            partition: message.partition,
            message: kafkaMessage(message),
            heartbeat: () => this.heartbeat(),
            pause: () => this.pauseOne(message.topic, message.partition)
          })
          if (this.isStale(pending, key)) {
            break
          }
          if (this.isStale(pending, key)) {
            break
          }
          this.resolve(message.topic, message.partition, message.offset)
          await this.commitOffsetsIfNecessary(config)
          if (this.isStale(pending, key)) {
            break
          }
          if (this.isPaused(message.topic, message.partition)) {
            const seekVersion = (this.seekVersions.get(key) ?? 0) + 1
            this.seekVersions.set(key, seekVersion)
            this.applySeek(message.topic, message.partition, message.offset + 1n, seekVersion)
            break
          }
        }
      } else if (config.eachBatch) {
        await config.eachBatch({
          batch,
          resolveOffset: offset => {
            if (this.isStale(pending, key)) {
              return
            }
            const resolved = BigInt(offset)
            this.resolve(
              batch.topic,
              batch.partition,
              resolved === messages[messages.length - 1].offset ? pending.lastOffset : resolved
            )
          },
          heartbeat: () => this.heartbeat(),
          pause: () => this.pauseOne(batch.topic, batch.partition),
          commitOffsetsIfNecessary: async offsets => {
            if (this.isStale(pending, key)) {
              return
            }
            if (offsets) {
              await this.commitOffsets(
                offsets.topics.flatMap(topic =>
                  topic.partitions.map(partition => ({ topic: topic.topic, ...partition })))
              )
            } else {
              await this.commitOffsetsIfNecessary(config)
            }
          },
          uncommittedOffsets: () => this.uncommitted(),
          isRunning: () => this.running,
          isStale: () => this.isStale(pending, key)
        })
        const stale = this.isStale(pending, key)
        if (!stale && config.eachBatchAutoResolve !== false && messages.length > 0) {
          this.resolve(batch.topic, batch.partition, pending.lastOffset)
        }
        if (stale) {
          return
        }
      }
      if (config.autoCommit !== false && !this.isStale(pending, key)) {
        await this.commitResolved()
      }
    } catch (error) {
      if (config.autoCommit !== false && !this.isStale(pending, key)) {
        await this.commitResolved()
      }
      throw error
    } finally {
      this.emit(this.events.END_BATCH_PROCESS, { ...eventPayload, duration: Date.now() - started })
    }
  }

  private batchEvent (batch: Batch) {
    return {
      topic: batch.topic,
      partition: batch.partition,
      highWatermark: batch.highWatermark,
      offsetLag: batch.offsetLag(),
      offsetLagLow: batch.offsetLagLow(),
      batchSize: batch.messages.length,
      firstOffset: batch.firstOffset() ?? '',
      lastOffset: batch.lastOffset()
    }
  }

  private isStale (pending: PendingBatch, key: string): boolean {
    return (
      !this.running ||
      pending.seekVersion !== (this.seekVersions.get(key) ?? 0) ||
      (pending.membershipVersion !== undefined && pending.membershipVersion !== this.native[kMembershipVersion]())
    )
  }

  private resetRunState (): void {
    this.pausedPartitions.clear()
    this.resumedWholeTopicPartitions.clear()
    this.pendingSeeks.clear()
    this.seekVersions.clear()
    this.resolvedOffsets.clear()
    this.committedOffsets.clear()
    this.resolvedCount = 0
    this.lastCommit = Date.now()
    this.wakePaused()
  }

  private resolve (topic: string, partition: number, offset: bigint): void {
    const key = partitionKey(topic, partition)
    const nextOffset = offset + 1n
    const previous = this.resolvedOffsets.get(key)
    if (previous === undefined || nextOffset > previous) {
      this.resolvedOffsets.set(key, nextOffset)
      this.resolvedCount += Number(previous === undefined ? 1n : nextOffset - previous)
    }
  }

  private uncommitted (): OffsetsByTopicPartition {
    const topics = new Map<string, Array<{ partition: number; offset: string }>>()
    for (const [key, offset] of this.resolvedOffsets) {
      if (this.committedOffsets.get(key) === offset) {
        continue
      }
      const separator = key.lastIndexOf(':')
      const topic = key.slice(0, separator)
      const partition = Number(key.slice(separator + 1))
      const partitions = topics.get(topic) ?? []
      partitions.push({ partition, offset: offset.toString() })
      topics.set(topic, partitions)
    }
    return { topics: Array.from(topics, ([topic, partitions]) => ({ topic, partitions })) }
  }

  private async commitOffsetsIfNecessary (config: ConsumerRunConfig): Promise<void> {
    if (config.autoCommit === false) {
      return
    }
    const intervalReached =
      config.autoCommitInterval != null && Date.now() >= this.lastCommit + config.autoCommitInterval
    const thresholdReached = config.autoCommitThreshold != null && this.resolvedCount >= config.autoCommitThreshold
    if (intervalReached || thresholdReached) {
      await this.commitResolved()
    }
  }

  private async commitResolved (): Promise<void> {
    const offsets = this.uncommitted().topics.flatMap(topic =>
      topic.partitions.map(partition => ({ topic: topic.topic, ...partition })))
    if (offsets.length === 0) {
      this.lastCommit = Date.now()
      return
    }
    await this.commit(offsets)
    for (const offset of offsets) {
      this.committedOffsets.set(partitionKey(offset.topic, offset.partition), BigInt(offset.offset))
    }
    this.resolvedCount = 0
    this.lastCommit = Date.now()
  }

  private async heartbeat (): Promise<void> {
    await this.native.heartbeat()
  }

  private pauseOne (topic: string, partition: number): () => void {
    this.pause([{ topic, partitions: [partition] }])
    return () => this.resume([{ topic, partitions: [partition] }])
  }

  private isPaused (topic: string, partition: number): boolean {
    if (!this.pausedPartitions.has(topic)) {
      return false
    }
    const partitions = this.pausedPartitions.get(topic)
    if (partitions === undefined) {
      return !this.resumedWholeTopicPartitions.get(topic)?.has(partition)
    }
    return partitions.has(partition)
  }

  private async waitUntilResumed (topic: string, partition: number): Promise<void> {
    while (this.running && this.isPaused(topic, partition)) {
      await new Promise<void>(resolve => this.pauseWaiters.add(resolve))
    }
  }

  async stop (): Promise<void> {
    if (!this.running && !this.stream && !this.runTask) {
      return
    }
    this.running = false
    this.restartStream = false
    this.signalRunStarted?.()
    this.restartAbort?.abort()
    this.restartAbort = undefined
    this.wakePaused()
    await this.stream?.close()
    try {
      await this.runTask
    } catch {}
    await call(this.native.leaveGroup(false) as unknown as Promise<void>).catch(() => {})
    this.stream = undefined
    this.runTask = undefined
    this.emit(this.events.STOP)
  }

  async commitOffsets (offsets: TopicPartitionOffsetAndMetadata[] = []): Promise<void> {
    for (const offset of offsets) {
      this.validateOffset(offset, false)
    }
    if (!this.running) {
      throw new KafkaJSNonRetriableError('Consumer group was not initialized, consumer#run must be called first')
    }
    await this.commit(offsets)
  }

  private async commit (offsets: TopicPartitionOffsetAndMetadata[]): Promise<void> {
    await call(
      this.native.commit({
        offsets: offsets.map(offset => ({
          topic: offset.topic,
          partition: offset.partition,
          offset: BigInt(offset.offset),
          leaderEpoch: -1,
          [kKafkaJSCommitMetadata]: offset.metadata
        }))
      })
    )
    const topics = new Map<string, Array<{ partition: number; offset: string }>>()
    for (const offset of offsets) {
      const partitions = topics.get(offset.topic) ?? []
      partitions.push({ partition: offset.partition, offset: offset.offset })
      topics.set(offset.topic, partitions)
      this.committedOffsets.set(partitionKey(offset.topic, offset.partition), BigInt(offset.offset))
    }
    this.emit(this.events.COMMIT_OFFSETS, {
      groupId: this.native.groupId,
      memberId: this.native.memberId ?? '',
      groupGenerationId: this.native.generationId,
      topics: Array.from(topics, ([topic, partitions]) => ({ topic, partitions }))
    })
  }

  seek (offset: TopicPartitionOffset): void {
    this.validateOffset(offset, true)
    if (!this.running) {
      throw new KafkaJSNonRetriableError('Consumer group was not initialized, consumer#run must be called first')
    }
    const value = BigInt(offset.offset)
    const key = partitionKey(offset.topic, offset.partition)
    const seekVersion = (this.seekVersions.get(key) ?? 0) + 1
    this.seekVersions.set(key, seekVersion)
    if (value === EARLIEST_OFFSET || value === LATEST_OFFSET) {
      const stream = this.stream
      this.resolveSpecialSeek(offset, value, seekVersion).catch(error => {
        if (this.running && this.stream === stream) {
          stream?.destroy(error as Error)
        }
      })
      return
    }
    this.applySeek(offset.topic, offset.partition, value, seekVersion)
  }

  private async resolveSpecialSeek (offset: TopicPartitionOffset, value: bigint, seekVersion: number): Promise<void> {
    const offsets = await call(
      this.native.listOffsets({
        topics: [offset.topic],
        partitions: { [offset.topic]: [offset.partition] },
        timestamp: value
      })
    )
    const key = partitionKey(offset.topic, offset.partition)
    if (this.seekVersions.get(key) !== seekVersion) {
      return
    }
    const resolved = offsets.get(offset.topic)?.[offset.partition]
    if (resolved === undefined) {
      throw new KafkaJSNonRetriableError(
        `Unable to resolve seek offset for topic ${offset.topic} partition ${offset.partition}`
      )
    }
    this.applySeek(offset.topic, offset.partition, resolved, seekVersion)
  }

  private applySeek (topic: string, partition: number, offset: bigint, seekVersion: number): void {
    const key = partitionKey(topic, partition)
    if (this.seekVersions.get(key) !== seekVersion) {
      return
    }
    this.pendingSeeks.set(key, offset)
    this.resolvedOffsets.delete(key)
    this.applyPendingSeeks().catch(error => this.stream?.destroy(error))
  }

  private async applyPendingSeeks (stream = this.stream): Promise<void> {
    if (!stream) {
      return
    }
    for (const [key, offset] of this.pendingSeeks) {
      const separator = key.lastIndexOf(':')
      const topic = key.slice(0, separator)
      const partition = Number(key.slice(separator + 1))
      const assignment = this.native.assignments?.find(assignment => assignment.topic === topic)
      if (!assignment?.partitions.includes(partition)) {
        continue
      }
      const seekVersion = this.seekVersions.get(key) ?? 0
      if (this.autoCommitEnabled) {
        await this.commit([{ topic, partition, offset: offset.toString() }])
        if (this.seekVersions.get(key) !== seekVersion) {
          continue
        }
      }
      stream[kSeekPartition](topic, partition, offset, seekVersion)
      if (this.seekVersions.get(key) === seekVersion) {
        this.pendingSeeks.delete(key)
      }
    }
  }

  private validateOffset (offset: TopicPartitionOffsetAndMetadata, special: boolean): void {
    if (!offset.topic) {
      throw new KafkaJSNonRetriableError(`Invalid topic ${offset.topic}`)
    }
    if (!Number.isSafeInteger(offset.partition)) {
      throw new KafkaJSNonRetriableError(`Invalid partition, expected a number received ${offset.partition}`)
    }
    let value: bigint
    try {
      value = BigInt(offset.offset)
    } catch {
      throw new KafkaJSNonRetriableError(`Invalid offset, expected a long received ${offset.offset}`)
    }
    if (value < 0n && (!special || (value !== EARLIEST_OFFSET && value !== LATEST_OFFSET))) {
      throw new KafkaJSNonRetriableError('Offset must not be a negative number')
    }
    if (
      'metadata' in offset &&
      offset.metadata !== undefined &&
      offset.metadata !== null &&
      typeof offset.metadata !== 'string'
    ) {
      throw new KafkaJSNonRetriableError(
        `Invalid offset metadata, expected string or null, received ${String(offset.metadata)}`
      )
    }
  }

  async describeGroup (): Promise<GroupDescription> {
    const admin = new KafkaJSAdminBridge(this.baseOptions)
    try {
      const responses = await call(admin.describeGroupsRaw([this.native.groupId]))
      const group = responses.flatMap(response => response.groups).find(group => group.groupId === this.native.groupId)
      if (!group) {
        throw new KafkaJSNonRetriableError(`Consumer group ${this.native.groupId} was not found`)
      }
      const states: Record<string, GroupDescription['state']> = {
        STABLE: 'Stable',
        EMPTY: 'Empty',
        DEAD: 'Dead',
        PREPARING_REBALANCE: 'PreparingRebalance',
        COMPLETING_REBALANCE: 'CompletingRebalance'
      }
      return {
        groupId: group.groupId,
        state: states[group.groupState] ?? 'Unknown',
        protocol: group.protocolData,
        protocolType: group.protocolType,
        members: group.members.map(member => ({
          clientHost: member.clientHost,
          clientId: member.clientId,
          memberId: member.memberId,
          memberAssignment: member.memberAssignment,
          memberMetadata: member.memberMetadata
        }))
      }
    } finally {
      await admin.close().catch(() => {})
    }
  }

  pause (topics: Array<{ topic: string; partitions?: number[] }> = []): void {
    this.validateTopicPartitions(topics, 'pause')
    if (!this.running) {
      throw new KafkaJSNonRetriableError('Consumer group was not initialized, consumer#run must be called first')
    }
    for (const topic of topics) {
      if (!topic.partitions) {
        this.pausedPartitions.set(topic.topic, undefined)
        this.resumedWholeTopicPartitions.delete(topic.topic)
        continue
      }
      const current = this.pausedPartitions.get(topic.topic)
      if (this.pausedPartitions.has(topic.topic) && current === undefined) {
        const resumed = this.resumedWholeTopicPartitions.get(topic.topic)
        for (const partition of topic.partitions) {
          resumed?.delete(partition)
        }
      } else {
        this.pausedPartitions.set(topic.topic, new Set([...(current ?? []), ...topic.partitions]))
      }
    }
  }

  paused (): TopicPartitions[] {
    if (!this.running) {
      return []
    }
    return Array.from(this.pausedPartitions, ([topic, partitions]) => {
      if (partitions) {
        return { topic, partitions: Array.from(partitions) }
      }
      const resumed = this.resumedWholeTopicPartitions.get(topic) ?? new Set()
      const assignment = this.native.assignments?.find(assignment => assignment.topic === topic)
      return { topic, partitions: (assignment?.partitions ?? []).filter(partition => !resumed.has(partition)) }
    }).filter(topic => topic.partitions.length > 0)
  }

  resume (topics: Array<{ topic: string; partitions?: number[] }> = []): void {
    this.validateTopicPartitions(topics, 'resume')
    if (!this.running) {
      throw new KafkaJSNonRetriableError('Consumer group was not initialized, consumer#run must be called first')
    }
    for (const topic of topics) {
      if (!topic.partitions) {
        this.pausedPartitions.delete(topic.topic)
        this.resumedWholeTopicPartitions.delete(topic.topic)
        continue
      }
      const paused = this.pausedPartitions.get(topic.topic)
      if (paused === undefined) {
        if (this.pausedPartitions.has(topic.topic)) {
          const resumed = this.resumedWholeTopicPartitions.get(topic.topic) ?? new Set<number>()
          for (const partition of topic.partitions) {
            resumed.add(partition)
          }
          this.resumedWholeTopicPartitions.set(topic.topic, resumed)
        }
        continue
      }
      for (const partition of topic.partitions) {
        paused.delete(partition)
      }
      if (paused.size === 0) {
        this.pausedPartitions.delete(topic.topic)
      }
    }
    this.wakePaused()
  }

  private validateTopicPartitions (topics: Array<{ topic: string; partitions?: number[] }>, operation: string): void {
    for (const topic of topics) {
      if (!topic?.topic) {
        throw new KafkaJSNonRetriableError(`Invalid topic ${topic?.topic ?? topic}`)
      }
      if (
        topic.partitions !== undefined &&
        (!Array.isArray(topic.partitions) || topic.partitions.some(partition => !Number.isSafeInteger(partition)))
      ) {
        throw new KafkaJSNonRetriableError(
          `Array of valid partitions required to ${operation} specific partitions instead of ${String(topic.partitions)}`
        )
      }
    }
  }

  private wakePaused (): void {
    for (const resolve of this.pauseWaiters) {
      resolve()
    }
    this.pauseWaiters.clear()
  }

  private startFetchInstrumentation (): void {
    if (this.fetchDiagnosticsSubscribed) {
      return
    }
    consumerFetchesChannel.start.subscribe(this.onFetchStart)
    consumerFetchesChannel.asyncEnd.subscribe(this.onFetchEnd)
    this.fetchDiagnosticsSubscribed = true
  }

  private stopFetchInstrumentation (): void {
    if (!this.fetchDiagnosticsSubscribed) {
      return
    }
    consumerFetchesChannel.start.unsubscribe(this.onFetchStart)
    consumerFetchesChannel.asyncEnd.unsubscribe(this.onFetchEnd)
    this.fetchDiagnosticsSubscribed = false
    this.fetchStarts.clear()
  }

  private readonly onFetchStart = (rawDiagnostic: unknown): void => {
    const diagnostic = rawDiagnostic as {
      client: KafkaJSConsumerBridge
      operationId: bigint
      options: { node: number }
    }
    if (diagnostic.client === this.native) {
      this.fetchStarts.set(diagnostic.operationId, Date.now())
      this.emit(this.events.FETCH_START, { nodeId: diagnostic.options.node })
    }
  }

  private readonly onFetchEnd = (rawDiagnostic: unknown): void => {
    const diagnostic = rawDiagnostic as {
      client: KafkaJSConsumerBridge
      operationId: bigint
      options: { node: number }
    }
    if (diagnostic.client === this.native) {
      const started = this.fetchStarts.get(diagnostic.operationId) ?? Date.now()
      this.fetchStarts.delete(diagnostic.operationId)
      this.emit(this.events.FETCH, {
        numberOfBatches: 0,
        duration: Date.now() - started,
        nodeId: diagnostic.options.node
      })
    }
  }

  logger (): Logger {
    return this.log
  }
}
