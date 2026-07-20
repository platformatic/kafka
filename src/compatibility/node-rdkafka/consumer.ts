import type { EventEmitter } from 'node:events'
import { deprecate } from 'node:util'
import { kResumeFetch, kSeekPartition, type MessagesStream } from '../../clients/consumer/messages-stream.ts'
import { type Message as PlatformMessage } from '../../protocol/records.ts'
import { Client, kClientRecreated } from './client.ts'
import { mapConsumerConfig, type OauthBearerToken } from './config.ts'
import {
  kNodeRdkafkaAssignmentFilter,
  kNodeRdkafkaAssignments,
  kNodeRdkafkaPartitionEof,
  kNodeRdkafkaPausedPartitions,
  NodeRdkafkaConsumerBridge,
  type NodeRdkafkaConsumeOptions
} from './consumer-native.ts'
import { CODES, compatibilityError, LibrdKafkaError, toLibrdKafkaError } from './errors.ts'
import { ConsumerStream, type ReadStreamOptions } from './streams.ts'
import { type Assignment, type ConsumerGlobalConfig, type ConsumerTopicConfig, type Message, type Metadata, type NodeCallback, type RdkafkaConfig, type TopicPartition, type TopicPartitionOffset, type WatermarkOffsets } from './types.ts'

type Subscription = string | RegExp
type BatchConsumeCallback = (error: Error | null, messages?: Message[]) => void
type FlowingConsumeCallback = (error: Error | null, message?: Message) => void

interface ConsumeRequest {
  count: number
  messages: Message[]
  callback: BatchConsumeCallback
  timer: NodeJS.Timeout
}

export class KafkaConsumer extends Client {
  consumer: NodeRdkafkaConsumerBridge
  readonly topicConfig: RdkafkaConfig
  #subscriptions: Subscription[] = []
  #assignments: Assignment[] = []
  readonly #callbackAssignments = new Set<string>()
  #manualAssignment = false
  #inRebalanceCallback = false
  #positions = new Map<string, number>()
  #storedOffsets = new Map<string, number>()
  #watermarks = new Map<string, WatermarkOffsets>()
  #paused = new Set<string>()
  #stream: MessagesStream<Buffer, Buffer | null, Buffer, Buffer> | null = null
  #streamPromise: Promise<void> | null = null
  #requests: ConsumeRequest[] = []
  #flowing = false
  #flowingCallback: FlowingConsumeCallback | null = null
  #consumeTimeout = 1000
  #generation = 0
  #seekVersion = 0
  #seekOffsets = new Map<string, number>()
  readonly #autoOffsetStore: boolean
  readonly #autoCommitInterval: number | null
  #autoCommitTimer: NodeJS.Timeout | null = null
  #consumeLoopDelay = 0
  #consumeLoopTimer: NodeJS.Timeout | null = null

  constructor (config: ConsumerGlobalConfig, topicConfig: ConsumerTopicConfig = {}) {
    const internalConfig = config as unknown as RdkafkaConfig
    const internalTopicConfig = topicConfig as RdkafkaConfig
    const oauthBearerToken = createOauthBearerToken(internalConfig)
    const options = mapConsumerConfig(internalConfig, internalTopicConfig, oauthBearerToken ?? undefined)
    const createConsumer = (): NodeRdkafkaConsumerBridge => new NodeRdkafkaConsumerBridge(options)
    const consumer = createConsumer()
    super(consumer, internalConfig, oauthBearerToken, createConsumer)
    this.consumer = consumer
    this.topicConfig = { ...internalTopicConfig }
    this.#autoOffsetStore = internalConfig['enable.auto.offset.store'] !== false
    this.#autoCommitInterval = internalConfig['enable.auto.commit'] === false
      ? null
      : typeof internalConfig['auto.commit.interval.ms'] === 'number' ? internalConfig['auto.commit.interval.ms'] : 5000

    const bindConsumer = (consumer: NodeRdkafkaConsumerBridge): void => {
      consumer.on('consumer:group:join', payload => {
        if (this.#manualAssignment) {
          return
        }
        const previousAssignments = this.#assignments
        const assignments = payload.assignments?.flatMap(assignment => assignment.partitions.map(partition => ({
          topic: assignment.topic,
          partition
        }))) ?? []
        const callbackOwnsAssignments = typeof internalConfig.rebalance_cb === 'function'
        if (!callbackOwnsAssignments) {
          this.#replaceAssignments(assignments)
        }
        const revoked = previousAssignments
        if (revoked.length > 0 && internalConfig.rebalance_cb) {
          const error = new LibrdKafkaError('Local: Revoke partitions', CODES.ERRORS.ERR__REVOKE_PARTITIONS)
          this.emit('rebalance', error, revoked)
          if (callbackOwnsAssignments) {
            const rebalanceCallback = internalConfig.rebalance_cb as unknown as (error: Error, assignments: TopicPartition[]) => void
            this.#invokeRebalanceCallback(rebalanceCallback, error, revoked)
          }
        }
        if (internalConfig.rebalance_cb) {
          const error = new LibrdKafkaError('Local: Assign partitions', CODES.ERRORS.ERR__ASSIGN_PARTITIONS)
          this.emit('rebalance', error, assignments)
        }
        if (callbackOwnsAssignments) {
          const error = new LibrdKafkaError('Local: Assign partitions', CODES.ERRORS.ERR__ASSIGN_PARTITIONS)
          const rebalanceCallback = internalConfig.rebalance_cb as unknown as (error: Error, assignments: TopicPartition[]) => void
          this.#invokeRebalanceCallback(rebalanceCallback, error, assignments)
        }
      })
    }
    bindConsumer(consumer)
    this.on(kClientRecreated, client => {
      this.consumer = client as NodeRdkafkaConsumerBridge
      bindConsumer(this.consumer)
    })
  }

  static createReadStream (config: ConsumerGlobalConfig, topicConfig: ConsumerTopicConfig, options: ReadStreamOptions | number): ConsumerStream {
    return new ConsumerStream(new KafkaConsumer(config, topicConfig), options)
  }

  subscribe (topics: Subscription[]): this {
    if (!Array.isArray(topics)) {
      throw new TypeError('Topics must be an array')
    }
    if (!this.isConnected()) {
      throw compatibilityError('Local: Erroneous state', CODES.ERRORS.ERR__STATE)
    }
    this.#resetConsumption()
    this.#assignments = []
    this.#callbackAssignments.clear()
    this.#paused.clear()
    this.#manualAssignment = false
    this.#positions.clear()
    this.#storedOffsets.clear()
    this.#subscriptions = [...topics]
    this.#closeStream().catch(error => this.#emitError(error))
    this.emit('subscribed', topics)
    return this
  }

  subscription (): string[] {
    return this.#subscriptions.map(topic => topic.toString())
  }

  unsubscribe (): this {
    this.#resetConsumption()
    this.#subscriptions = []
    this.#assignments = []
    this.#callbackAssignments.clear()
    this.#paused.clear()
    this.#manualAssignment = false
    this.#closeStream().catch(error => this.#emitError(error))
    this.emit('unsubscribed', [])
    this.emit('unsubscribe', [])
    return this
  }

  assign (assignments: Assignment[]): this {
    validateManualAssignments(assignments)
    if (this.#inRebalanceCallback) {
      this.#replaceAssignments(assignments)
      return this
    }
    this.#resetConsumption()
    this.#assignments = assignments.map(assignment => ({ ...assignment }))
    this.#prunePaused(this.#assignments)
    this.#manualAssignment = true
    this.#positions.clear()
    this.#subscriptions = []
    this.#closeStream().catch(error => this.#emitError(error))
    return this
  }

  incrementalAssign (assignments: Assignment[]): this {
    validateManualAssignments(assignments)
    if (this.#inRebalanceCallback) {
      const byPartition = new Map(this.#assignments.map(assignment => [keyOf(assignment), assignment]))
      for (const assignment of assignments) {
        byPartition.set(keyOf(assignment), { ...assignment })
      }
      this.#replaceAssignments(Array.from(byPartition.values()), false)
      return this
    }
    this.#resetConsumption()
    this.#manualAssignment = true
    const byPartition = new Map(this.#assignments.map(assignment => [keyOf(assignment), assignment]))
    for (const assignment of assignments) {
      byPartition.set(keyOf(assignment), { ...assignment })
    }
    this.#assignments = Array.from(byPartition.values())
    this.#closeStream().catch(error => this.#emitError(error))
    return this
  }

  unassign (): this {
    if (this.#inRebalanceCallback) {
      this.#replaceAssignments([])
      return this
    }
    this.#resetConsumption()
    this.#assignments = []
    this.#callbackAssignments.clear()
    this.#paused.clear()
    this.#manualAssignment = false
    this.#positions.clear()
    this.#closeStream().catch(error => this.#emitError(error))
    return this
  }

  incrementalUnassign (assignments: TopicPartition[]): this {
    if (this.#inRebalanceCallback) {
      const removed = new Set(assignments.map(keyOf))
      this.#replaceAssignments(this.#assignments.filter(assignment => !removed.has(keyOf(assignment))), false)
      return this
    }
    this.#resetConsumption()
    const removed = new Set(assignments.map(keyOf))
    this.#assignments = this.#assignments.filter(assignment => !removed.has(keyOf(assignment)))
    for (const key of removed) {
      this.#positions.delete(key)
      this.#paused.delete(key)
    }
    this.#closeStream().catch(error => this.#emitError(error))
    return this
  }

  assignments (): Assignment[] {
    return this.#assignments.map(assignment => ({ ...assignment }))
  }

  position (partitions: TopicPartition[] = this.#assignments): TopicPartitionOffset[] {
    return partitions.map(partition => {
      return { ...partition, offset: this.#positions.get(keyOf(partition)) ?? -1001 }
    })
  }

  consume (count: number, callback?: BatchConsumeCallback): void
  consume (callback: FlowingConsumeCallback): void
  consume (): void
  consume (numberOrCallback?: number | FlowingConsumeCallback, callback?: BatchConsumeCallback): void {
    if (typeof numberOrCallback !== 'number' || numberOrCallback === 0) {
      if (!this.isConnected()) {
        throw new Error('Connect must be called before consume')
      }
      this.#flowing = true
      this.#flowingCallback = typeof numberOrCallback === 'function'
        ? numberOrCallback
        : typeof callback === 'function' ? callback as unknown as FlowingConsumeCallback : null
      this.#ensureStream().then(() => this.#pump(), error => this.#flowingCallback?.(toLibrdKafkaError(error)))
      return
    }

    if (!Number.isInteger(numberOrCallback) || numberOrCallback < 1) {
      throw new TypeError('The consume count must be a positive integer')
    }
    if (callback !== undefined && typeof callback !== 'function') {
      throw new TypeError('Callback must be a function')
    }
    const batchCallback = callback ?? (() => {})
    if (!this.isConnected()) {
      queueMicrotask(() => batchCallback(compatibilityError('KafkaConsumer is not connected', CODES.ERRORS.ERR__STATE)))
      return
    }
    this.#ensureStream().then(() => {
      const request: ConsumeRequest = {
        count: numberOrCallback,
        messages: [],
        callback: batchCallback,
        timer: setTimeout(() => {
          this.#finishRequest(request)
          this.#pump()
        }, this.#consumeTimeout)
      }
      this.#requests.push(request)
      this.#pump()
    }, error => batchCallback(toLibrdKafkaError(error)))
  }

  commit (partitions?: TopicPartitionOffset | TopicPartitionOffset[]): this {
    if (!this.isConnected()) {
      throw new Error('KafkaConsumer is disconnected')
    }
    const offsets = normalizeOffsets(partitions, this.#commitFallback())
    this.consumer.commit({ offsets: offsets.map(offset => ({ ...offset, offset: BigInt(offset.offset ?? -1), leaderEpoch: 0 })) }, error => {
      const converted = error ? toLibrdKafkaError(error) : null
      if (converted) {
        this.lastError = converted
      }
      if (this.globalConfig.offset_commit_cb) {
        this.emit('offset.commit', converted, offsets)
      }
      if (typeof this.globalConfig.offset_commit_cb === 'function') {
        const offsetCommitCallback = this.globalConfig.offset_commit_cb as unknown as (error: Error | null, offsets: TopicPartitionOffset[]) => void
        offsetCommitCallback.call(this, converted, offsets)
      }
    })
    return this
  }

  commitMessage (message: TopicPartitionOffset): this {
    return this.commit({ ...message, offset: (message.offset ?? -1) + 1 })
  }

  commitSync (_partitions: TopicPartitionOffset | TopicPartitionOffset[] | null = null): this {
    throw compatibilityError('Synchronous commits are not supported', CODES.ERRORS.ERR__NOT_IMPLEMENTED)
  }

  commitMessageSync (_message: TopicPartitionOffset): this {
    throw compatibilityError('Synchronous commits are not supported', CODES.ERRORS.ERR__NOT_IMPLEMENTED)
  }

  committed (partitions: TopicPartition[], timeout: number, callback: NodeCallback<TopicPartitionOffset[]>): this
  committed (timeout: number, callback: NodeCallback<TopicPartitionOffset[]>): this
  committed (partitionsOrTimeout: TopicPartition[] | number | null, timeoutOrCallback: number | NodeCallback<TopicPartitionOffset[]>, callback?: NodeCallback<TopicPartitionOffset[]>): this {
    let partitions: TopicPartition[]
    let timeout = 1000
    if (typeof partitionsOrTimeout === 'number') {
      partitions = this.#assignments
      timeout = partitionsOrTimeout
      callback = timeoutOrCallback as NodeCallback<TopicPartitionOffset[]>
    } else {
      partitions = partitionsOrTimeout ?? this.#assignments
      if (typeof timeoutOrCallback === 'number') {
        timeout = timeoutOrCallback
      }
    }
    if (typeof timeoutOrCallback === 'function') {
      callback = timeoutOrCallback
    }
    if (partitions.length === 0) {
      queueMicrotask(() => callback?.(null, []))
      return this
    }
    const topics = new Map<string, number[]>()
    for (const partition of partitions) {
      const list = topics.get(partition.topic) ?? []
      list.push(partition.partition)
      topics.set(partition.topic, list)
    }
    this.#withTimeout(timeout, callback, done => {
      this.consumer.listCommittedOffsets({ topics: Array.from(topics, ([topic, partitions]) => ({ topic, partitions })) }, (error, offsets) => {
        if (error || !offsets) {
          done(error ?? compatibilityError('Committed offsets unavailable', CODES.ERRORS.ERR__FAIL))
          return
        }
        done(null, partitions.map(partition => ({
          ...partition,
          offset: toOffset(offsets.get(partition.topic)?.[partition.partition])
        })))
      })
    })
    return this
  }

  offsetsStore (partitions: TopicPartitionOffset[]): this {
    if (!this.isConnected()) {
      throw new Error('Client is disconnected')
    }
    for (const partition of partitions) {
      this.#storedOffsets.set(keyOf(partition), partition.offset ?? -1)
    }
    return this
  }

  seek (partition: TopicPartitionOffset, timeout: number | null, callback: NodeCallback): this {
    const assigned = this.#assignments.some(assignment => keyOf(assignment) === keyOf(partition))
    if (!assigned) {
      queueMicrotask(() => callback(compatibilityError('Seek partition is not assigned', CODES.ERRORS.ERR__STATE)))
      return this
    }
    if (partition.offset === undefined || (partition.offset < 0 && partition.offset !== -2 && partition.offset !== -1)) {
      queueMicrotask(() => callback(compatibilityError('Seek requires a concrete non-negative offset', CODES.ERRORS.ERR__INVALID_ARG)))
      return this
    }

    let completed = false
    let timer: NodeJS.Timeout | undefined
    const finish = (error: unknown, offset?: number): void => {
      if (completed) {
        return
      }
      completed = true
      if (timer) {
        clearTimeout(timer)
      }
      if (error || offset === undefined) {
        callback(toLibrdKafkaError(error ?? compatibilityError('Unable to resolve seek offset', CODES.ERRORS.ERR__FAIL)))
        return
      }
      this.#applySeek({ ...partition, offset })
      callback()
    }
    if (timeout !== null && timeout > 0) {
      timer = setTimeout(() => finish(compatibilityError('Seek operation timed out', CODES.ERRORS.ERR__TIMED_OUT)), timeout)
      timer.unref()
    }
    if (partition.offset >= 0) {
      queueMicrotask(() => finish(null, partition.offset))
      return this
    }
    this.consumer.listOffsets({
      topics: [partition.topic],
      partitions: { [partition.topic]: [partition.partition] },
      timestamp: BigInt(partition.offset)
    }, (error, offsets) => {
      const offset = offsets?.get(partition.topic)?.[partition.partition]
      finish(error, offset === undefined ? undefined : Number(offset))
    })
    return this
  }

  pause (partitions: TopicPartition[]): this {
    if (!this.isConnected()) {
      throw new Error('Client is disconnected')
    }
    for (const partition of partitions) {
      this.#paused.add(keyOf(partition))
    }
    return this
  }

  resume (partitions: TopicPartition[]): this {
    if (!this.isConnected()) {
      throw new Error('Client is disconnected')
    }
    for (const partition of partitions) {
      this.#paused.delete(keyOf(partition))
    }
    this.#stream?.[kResumeFetch]()
    return this
  }

  getWatermarkOffsets (topic: string, partition: number): WatermarkOffsets {
    if (!this.isConnected()) {
      throw new Error('Client is disconnected')
    }
    const offsets = this.#watermarks.get(keyOf({ topic, partition }))
    if (!offsets) {
      throw compatibilityError('No cached watermark offsets', CODES.ERRORS.ERR__NO_OFFSET)
    }
    return { ...offsets }
  }

  setDefaultConsumeTimeout (timeout: number): void {
    this.#consumeTimeout = timeout
  }

  setDefaultConsumeLoopTimeoutDelay (timeout: number): void {
    if (!Number.isFinite(timeout) || timeout < 0) {
      throw new TypeError('The consume loop timeout delay must be a non-negative number')
    }
    this.#consumeLoopDelay = timeout
  }

  rebalanceProtocol (): string {
    if (!this.connected) {
      return 'NONE'
    }
    return 'EAGER'
  }

  override disconnect (timeoutOrCallback?: number | NodeCallback<{ connectionOpened: number }>, callback?: NodeCallback<{ connectionOpened: number }>): this {
    if (typeof timeoutOrCallback === 'function') {
      callback = timeoutOrCallback
    }
    this.#resetConsumption()
    if (this.#autoCommitTimer) {
      clearInterval(this.#autoCommitTimer)
      this.#autoCommitTimer = null
    }
    let cleanupError: unknown
    this.#commitStoredOffsets()
      .catch(error => {
        cleanupError = error
      })
      .then(() => this.#closeStream())
      .catch(error => {
        cleanupError ??= error
      })
      .then(() => super.disconnect((error, metrics) => callback?.(cleanupError ? toLibrdKafkaError(cleanupError) : error, metrics)))
    return this
  }

  async #ensureStream (): Promise<void> {
    if (this.#stream) {
      return
    }
    if (this.#streamPromise) {
      await this.#streamPromise
      if (!this.#stream) {
        return this.#ensureStream()
      }
      return
    }
    this.#streamPromise = this.#createStream()
    try {
      await this.#streamPromise
    } finally {
      this.#streamPromise = null
    }
  }

  async #createStream (): Promise<void> {
    const generation = this.#generation
    let topics = this.#subscriptions.filter(topic => typeof topic === 'string') as string[]
    const patterns = this.#subscriptions.filter(topic => topic instanceof RegExp) as RegExp[]
    if (patterns.length > 0) {
      const metadata = await new Promise<Metadata>((resolve, reject) => {
        this.getMetadata({}, (error, result) => error || !result ? reject(error) : resolve(result))
      })
      topics = topics.concat(metadata.topics.map(topic => topic.name).filter(topic => patterns.some(pattern => {
        pattern.lastIndex = 0
        const matches = pattern.test(topic)
        pattern.lastIndex = 0
        return matches
      })))
    }
    if (topics.length === 0) {
      topics = [...new Set(this.#assignments.map(assignment => assignment.topic))]
    }
    if (topics.length === 0) {
      throw compatibilityError('No topics subscribed or assigned', CODES.ERRORS.ERR__STATE)
    }
    const manual = this.#manualAssignment && this.#assignments.length > 0
    const offsets = manual ? await this.#resolveManualOffsets() : []
    const reset = this.topicConfig['auto.offset.reset'] ?? this.globalConfig['auto.offset.reset']
    const options: NodeRdkafkaConsumeOptions = {
      topics,
      mode: manual ? 'manual' : 'committed',
      fallbackMode: resetFallback(reset),
      offsets: manual ? offsets : undefined,
      autocommit: false,
      [kNodeRdkafkaAssignmentFilter]: typeof this.globalConfig.rebalance_cb === 'function' && !manual
        ? this.#callbackAssignments
        : undefined,
      [kNodeRdkafkaAssignments]: manual
        ? this.#assignments.map(assignment => ({ topic: assignment.topic, partitions: [assignment.partition] }))
        : undefined,
      [kNodeRdkafkaPausedPartitions]: this.#paused,
      [kNodeRdkafkaPartitionEof]: this.globalConfig['enable.partition.eof'] === true
    }
    const stream = await (this.consumer.consume(options) as Promise<MessagesStream<Buffer, Buffer | null, Buffer, Buffer>>)
    if (generation !== this.#generation) {
      await stream.close()
      return
    }
    this.#stream = stream
    const eventStream = stream as unknown as EventEmitter
    eventStream.on('partition.eof', eof => this.emit('partition.eof', eof))
    eventStream.on('watermark', watermark => {
      const offsets = watermark as WatermarkOffsets & TopicPartition
      this.#watermarks.set(keyOf(offsets), { lowOffset: offsets.lowOffset, highOffset: offsets.highOffset })
    })
    if (this.#autoCommitInterval && this.#autoCommitInterval > 0 && !this.#autoCommitTimer) {
      this.#autoCommitTimer = setInterval(() => this.commit(), this.#autoCommitInterval)
      this.#autoCommitTimer.unref()
    }
    this.#stream.on('readable', () => this.#pump())
    this.#stream.on('error', error => {
      const converted = toLibrdKafkaError(error)
      this.lastError = converted
      this.emit('event.error', converted)
      this.#flowingCallback?.(converted)
      for (const request of this.#requests.splice(0)) {
        clearTimeout(request.timer)
        request.callback(converted)
      }
    })
  }

  #convertMessage (source: PlatformMessage<Buffer, Buffer | null, Buffer, Buffer>): Message {
    const headers: Record<string, Buffer>[] = []
    for (const [key, value] of source.headers) {
      headers.push({ [key.toString()]: value })
    }
    return {
      topic: source.topic,
      partition: source.partition,
      offset: Number(source.offset),
      value: source.value,
      key: source.key,
      timestamp: Number(source.timestamp),
      size: source.value?.byteLength ?? 0,
      headers
    }
  }

  #deliver (message: Message): void {
    this.#setPosition({ ...message, offset: message.offset + 1 })
    if (this.#autoOffsetStore) {
      this.#storedOffsets.set(keyOf(message), message.offset + 1)
    }
    const watermark = this.#watermarks.get(keyOf(message)) ?? { lowOffset: 0, highOffset: 0 }
    watermark.highOffset = Math.max(watermark.highOffset, message.offset + 1)
    this.#watermarks.set(keyOf(message), watermark)
    this.emit('data', message)
  }

  #pump (): void {
    if (!this.#stream) {
      return
    }
    while (this.#requests.length > 0) {
      for (const request of [...this.#requests]) {
        if (!this.#requests.includes(request)) {
          continue
        }
        const message = this.#readMessage()
        if (!message) {
          return
        }
        request.messages.push(message)
        if (request.messages.length === request.count) {
          this.#finishRequest(request)
        }
      }
    }
    if (this.#flowing) {
      let message
      let delivered = false
      while ((message = this.#readMessage())) {
        delivered = true
        this.#deliver(message)
        this.#flowingCallback?.(null, message)
      }
      if (!delivered && this.#consumeLoopDelay > 0 && !this.#consumeLoopTimer) {
        this.#consumeLoopTimer = setTimeout(() => {
          this.#consumeLoopTimer = null
          this.#pump()
        }, this.#consumeLoopDelay)
        this.#consumeLoopTimer.unref()
      }
    }
  }

  #readMessage (): Message | null {
    if (!this.#stream) {
      return null
    }
    let source: PlatformMessage<Buffer, Buffer | null, Buffer, Buffer> | null
    while ((source = this.#stream.read()) !== null) {
      const sourceKey = keyOf(source)
      const seekOffset = this.#seekOffsets.get(sourceKey)
      if (seekOffset !== undefined && Number(source.offset) < seekOffset) {
        continue
      }
      if (!this.#manualAssignment || this.#assignments.some(assignment => keyOf(assignment) === sourceKey)) {
        return this.#convertMessage(source)
      }
    }
    return null
  }

  #finishRequest (request: ConsumeRequest): void {
    const index = this.#requests.indexOf(request)
    if (index !== -1) {
      this.#requests.splice(index, 1)
    }
    clearTimeout(request.timer)
    for (const message of request.messages) {
      this.#deliver(message)
    }
    request.callback(null, request.messages)
  }

  #setPosition (partition: TopicPartitionOffset): void {
    if (partition.offset !== undefined) {
      this.#positions.set(keyOf(partition), partition.offset)
    }
  }

  #applySeek (partition: TopicPartitionOffset): void {
    this.#setPosition(partition)
    this.#seekOffsets.set(keyOf(partition), partition.offset)
    this.#discardBufferedMessages()
    this.#stream?.[kSeekPartition](partition.topic, partition.partition, BigInt(partition.offset), ++this.#seekVersion)
  }

  #withTimeout<T> (timeout: number, callback: NodeCallback<T> | undefined, operation: (done: (error: unknown, value?: T) => void) => void): void {
    let completed = false
    const finish = (error: unknown, value?: T): void => {
      if (completed) {
        return
      }
      completed = true
      clearTimeout(timer)
      callback?.(error ? toLibrdKafkaError(error) : null, value)
    }
    const timer = setTimeout(() => finish(compatibilityError('Consumer operation timed out', CODES.ERRORS.ERR__TIMED_OUT)), timeout)
    timer.unref()
    operation(finish)
  }

  #commitFallback (): TopicPartitionOffset[] {
    if (!this.#autoOffsetStore) {
      return this.#assignments.flatMap(assignment => {
        const offset = this.#storedOffsets.get(keyOf(assignment))
        return offset === undefined ? [] : [{ ...assignment, offset }]
      })
    }
    return this.#assignments.flatMap(assignment => {
      const offset = this.#storedOffsets.get(keyOf(assignment)) ?? this.#positions.get(keyOf(assignment))
      return offset === undefined ? [] : [{ ...assignment, offset }]
    })
  }

  #commitStoredOffsets (): Promise<void> {
    const offsets = this.#commitFallback()
    if (offsets.length === 0 || this.#autoCommitInterval === null) {
      return Promise.resolve()
    }
    return new Promise((resolve, reject) => {
      this.consumer.commit({ offsets: offsets.map(offset => ({ ...offset, offset: BigInt(offset.offset!), leaderEpoch: 0 })) }, error => {
        if (error) {
          reject(error)
        } else {
          resolve()
        }
      })
    })
  }

  async #resolveManualOffsets (): Promise<{ topic: string; partition: number; offset: bigint }[]> {
    const resolved: { topic: string; partition: number; offset: bigint }[] = []
    for (const assignment of this.#assignments) {
      const offset = assignment.offset
      if (typeof offset === 'number' && offset >= 0) {
        resolved.push({ ...assignment, offset: BigInt(offset) })
        continue
      }
      const timestamp = offset === -2 || offset === 'earliest' || offset === 'beginning'
        ? -2n
        : offset === -1 || offset === 'latest' || offset === 'end' ? -1n : null
      if (timestamp !== null) {
        const offsets = await new Promise<Map<string, bigint[]>>((resolve, reject) => {
          this.consumer.listOffsets({ topics: [assignment.topic], partitions: { [assignment.topic]: [assignment.partition] }, timestamp }, (error, result) => {
            if (error || !result) {
              reject(error ?? compatibilityError('Unable to resolve assignment offset', CODES.ERRORS.ERR__FAIL))
              return
            }
            resolve(result)
          })
        })
        resolved.push({ ...assignment, offset: offsets.get(assignment.topic)?.[assignment.partition] ?? -1n })
        continue
      }
      const committed = await new Promise<bigint | undefined>((resolve, reject) => {
        this.consumer.listCommittedOffsets({ topics: [{ topic: assignment.topic, partitions: [assignment.partition] }] }, (error, result) => {
          if (error) {
            reject(error)
            return
          }
          resolve(result?.get(assignment.topic)?.[assignment.partition])
        })
      })
      if (committed !== undefined && committed >= 0n) {
        resolved.push({ ...assignment, offset: committed })
        continue
      }
      const reset = this.topicConfig['auto.offset.reset'] ?? this.globalConfig['auto.offset.reset']
      const timestampForReset = reset === 'earliest' || reset === 'smallest' ? -2n : -1n
      const offsets = await new Promise<Map<string, bigint[]>>((resolve, reject) => {
        this.consumer.listOffsets({ topics: [assignment.topic], partitions: { [assignment.topic]: [assignment.partition] }, timestamp: timestampForReset }, (error, result) => {
          if (error || !result) {
            reject(error ?? compatibilityError('Unable to resolve assignment offset', CODES.ERRORS.ERR__FAIL))
            return
          }
          resolve(result)
        })
      })
      resolved.push({ ...assignment, offset: offsets.get(assignment.topic)?.[assignment.partition] ?? -1n })
    }
    return resolved
  }

  async #closeStream (): Promise<void> {
    const stream = this.#stream
    this.#stream = null
    if (stream) {
      await stream.close()
    }
  }

  #resetConsumption (): void {
    this.#generation++
    this.#flowing = false
    this.#flowingCallback = null
    if (this.#consumeLoopTimer) {
      clearTimeout(this.#consumeLoopTimer)
      this.#consumeLoopTimer = null
    }
    for (const request of this.#requests.splice(0)) {
      clearTimeout(request.timer)
      request.callback(compatibilityError('Consumption changed while a consume request was pending', CODES.ERRORS.ERR__STATE))
    }
  }

  #prunePaused (assignments: TopicPartition[]): void {
    const assigned = new Set(assignments.map(keyOf))
    for (const key of this.#paused) {
      if (!assigned.has(key)) {
        this.#paused.delete(key)
      }
    }
  }

  #replaceAssignments (assignments: Assignment[], clearOffsets = true): void {
    this.#assignments = assignments.map(assignment => ({ ...assignment }))
    this.#callbackAssignments.clear()
    for (const assignment of assignments) {
      this.#callbackAssignments.add(keyOf(assignment))
    }
    this.#prunePaused(assignments)
    if (clearOffsets) {
      this.#positions.clear()
      this.#storedOffsets.clear()
    }
  }

  #invokeRebalanceCallback (
    callback: (error: Error, assignments: TopicPartition[]) => void,
    error: Error,
    assignments: TopicPartition[]
  ): void {
    this.#inRebalanceCallback = true
    try {
      callback.call(this, error, assignments)
    } finally {
      this.#inRebalanceCallback = false
    }
  }

  #emitError (error: unknown): void {
    const converted = toLibrdKafkaError(error)
    this.lastError = converted
    this.emit('event.error', converted)
  }

  #discardBufferedMessages (): void {
    for (const request of this.#requests) {
      request.messages.length = 0
    }
    let message
    do {
      message = this.#stream?.read()
    } while (message !== null && message !== undefined)
  }
}

function keyOf (partition: TopicPartition): string {
  return `${partition.topic}:${partition.partition}`
}

function normalizeOffsets (partitions: TopicPartitionOffset | TopicPartitionOffset[] | undefined, fallback: TopicPartitionOffset[]): TopicPartitionOffset[] {
  if (!partitions) {
    return fallback
  }
  return Array.isArray(partitions) ? partitions : [partitions]
}

function toOffset (offset: bigint | undefined): number {
  return offset === undefined || offset < 0n ? -1001 : Number(offset)
}

function validateManualAssignments (assignments: Assignment[]): void {
  if (assignments.some(assignment => assignment.offset !== undefined && typeof assignment.offset !== 'number' && assignment.offset !== 'earliest' && assignment.offset !== 'beginning' && assignment.offset !== 'latest' && assignment.offset !== 'end' && assignment.offset !== 'stored')) {
    throw compatibilityError(
      'Manual assignment has an invalid offset',
      CODES.ERRORS.ERR__NOT_IMPLEMENTED
    )
  }
}

function resetFallback (reset: unknown): 'earliest' | 'latest' | 'fail' {
  if (reset === 'earliest' || reset === 'smallest') {
    return 'earliest'
  }
  if (reset === 'error' || reset === 'fail') {
    return 'fail'
  }
  return 'latest'
}

export const Consumer = deprecate(KafkaConsumer, 'Use KafkaConsumer instead. This may be changed in a later version')

function createOauthBearerToken (config: RdkafkaConfig): OauthBearerToken | null {
  const token = config['oauthbearer.token']
  const mechanism = config['sasl.mechanisms'] ?? config['sasl.mechanism']
  return typeof token === 'string' || (typeof mechanism === 'string' && mechanism.toUpperCase() === 'OAUTHBEARER')
    ? { value: typeof token === 'string' ? token : '' }
    : null
}
