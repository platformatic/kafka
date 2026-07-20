import { EventEmitter } from 'node:events'
import type { Readable, ReadableOptions, Writable, WritableOptions } from 'node:stream'
import type {
  ConsumerGlobalConfig,
  ConsumerTopicConfig,
  GlobalConfig,
  ProducerGlobalConfig,
  ProducerTopicConfig,
  TopicConfig
} from './config.js'

export * from './config.js'
export * from './errors.js'

export interface LibrdKafkaError {
  message: string
  code: number
  errno: number
  origin: string
  stack?: string
  isFatal?: boolean
  isRetriable?: boolean
  isTxnRequiresAbort?: boolean
}

export interface ReadyInfo {
  name: string
}

export interface ClientMetrics {
  connectionOpened: number
}

export interface MetadataOptions {
  topic?: string
  allTopics?: boolean
  timeout?: number
}

export interface BrokerMetadata {
  id: number
  host: string
  port: number
}

export interface PartitionMetadata {
  id: number
  leader: number
  replicas: number[]
  isrs: number[]
}

export interface TopicMetadata {
  name: string
  partitions: PartitionMetadata[]
}

export interface Metadata {
  orig_broker_id: number
  orig_broker_name: string
  topics: TopicMetadata[]
  brokers: BrokerMetadata[]
}

export interface WatermarkOffsets {
  lowOffset: number
  highOffset: number
}

export interface TopicPartition {
  topic: string
  partition: number
}

export interface TopicPartitionOffset extends TopicPartition {
  offset: number
}

export type TopicPartitionTime = TopicPartitionOffset
export type EofEvent = TopicPartitionOffset
export type Assignment = TopicPartition | TopicPartitionOffset
export type NumberNullUndefined = number | null | undefined
export type MessageKey = Buffer | string | null | undefined
export type MessageHeader = { [key: string]: string | Buffer }
export type MessageValue = Buffer | null
export type SubscribeTopic = string | RegExp
export type SubscribeTopicList = SubscribeTopic[]

export interface DeliveryReport extends TopicPartitionOffset {
  value?: MessageValue
  size: number
  key?: MessageKey
  timestamp?: number
  opaque?: any
}

export interface Message extends TopicPartitionOffset {
  value: MessageValue
  size: number
  topic: string
  key?: MessageKey
  timestamp?: number
  headers?: MessageHeader[]
  opaque?: any
}

export interface ReadStreamOptions extends ReadableOptions {
  topics: SubscribeTopicList | SubscribeTopic | ((metadata: Metadata) => SubscribeTopicList)
  waitInterval?: number
  fetchSize?: number
  objectMode?: boolean
  highWaterMark?: number
  autoClose?: boolean
  streamAsBatch?: boolean
  connectOptions?: any
  initOauthBearerToken?: string
}

export interface WriteStreamOptions extends WritableOptions {
  encoding?: string
  objectMode?: boolean
  topic?: string
  autoClose?: boolean
  pollInterval?: number
  connectOptions?: any
}

export interface ProducerStream extends Writable {
  producer: Producer
  connect(metadataOptions?: MetadataOptions): void
  close(callback?: () => void): void
}

export interface ConsumerStream extends Readable {
  consumer: KafkaConsumer
  connect(options: ConsumerGlobalConfig): void
  refreshOauthBearerToken(token: string): void
  close(callback?: () => void): void
}

export type KafkaClientEvents = 'disconnected' | 'ready' | 'connection.failure' | 'event.error' | 'event.stats' | 'event.log' | 'event.event' | 'event.throttle'
export type KafkaConsumerEvents = 'data' | 'partition.eof' | 'rebalance' | 'rebalance.error' | 'subscribed' | 'unsubscribed' | 'unsubscribe' | 'offset.commit' | KafkaClientEvents
export type KafkaProducerEvents = 'delivery-report' | KafkaClientEvents

type EventListenerMap = {
  disconnected: (metrics: ClientMetrics) => void
  ready: (info: ReadyInfo, metadata: Metadata) => void
  'connection.failure': (error: LibrdKafkaError, metrics: ClientMetrics) => void
  'event.error': (error: LibrdKafkaError) => void
  'event.stats': (eventData: any) => void
  'event.log': (eventData: any) => void
  'event.event': (eventData: any) => void
  'event.throttle': (eventData: any) => void
  data: (message: Message) => void
  'partition.eof': (event: EofEvent) => void
  rebalance: (error: LibrdKafkaError, assignments: TopicPartition[]) => void
  'rebalance.error': (error: Error) => void
  subscribed: (topics: SubscribeTopicList) => void
  unsubscribe: () => void
  unsubscribed: () => void
  'offset.commit': (error: LibrdKafkaError, partitions: TopicPartitionOffset[]) => void
  'delivery-report': (error: LibrdKafkaError, report: DeliveryReport) => void
}

type EventListener<Event extends string> = Event extends keyof EventListenerMap ? EventListenerMap[Event] : never

export abstract class Client<Events extends string> extends EventEmitter {
  constructor(globalConfig: GlobalConfig, SubClientType: any, topicConfig: TopicConfig)
  connect(metadataOptions?: MetadataOptions, callback?: (error: LibrdKafkaError, metadata: Metadata) => any): this
  setOauthBearerToken(token: string): this
  getClient(): any
  connectedTime(): number
  getLastError(): LibrdKafkaError
  disconnect(callback?: (error: any, metrics: ClientMetrics) => any): this
  disconnect(timeout: number, callback?: (error: any, metrics: ClientMetrics) => any): this
  isConnected(): boolean
  getMetadata(metadataOptions?: MetadataOptions, callback?: (error: LibrdKafkaError, metadata: Metadata) => any): any
  queryWatermarkOffsets(topic: string, partition: number, timeout: number, callback?: (error: LibrdKafkaError, offsets: WatermarkOffsets) => any): any
  queryWatermarkOffsets(topic: string, partition: number, callback?: (error: LibrdKafkaError, offsets: WatermarkOffsets) => any): any
  on<Event extends Events>(event: Event, listener: EventListener<Event>): this
  once<Event extends Events>(event: Event, listener: EventListener<Event>): this
}

export class KafkaConsumer extends Client<KafkaConsumerEvents> {
  constructor(config: ConsumerGlobalConfig, topicConfig: ConsumerTopicConfig)
  assign(assignments: Assignment[]): this
  incrementalAssign(assignments: Assignment[]): this
  assignments(): Assignment[]
  commit(partition: TopicPartitionOffset | TopicPartitionOffset[]): this
  commit(): this
  commitMessage(message: TopicPartitionOffset): this
  commitMessageSync(message: TopicPartitionOffset): this
  commitSync(partition: TopicPartitionOffset | TopicPartitionOffset[] | null): this
  committed(partitions: TopicPartition[], timeout: number, callback: (error: LibrdKafkaError, partitions: TopicPartitionOffset[]) => void): this
  committed(timeout: number, callback: (error: LibrdKafkaError, partitions: TopicPartitionOffset[]) => void): this
  consume(count: number, callback?: (error: LibrdKafkaError, messages: Message[]) => void): void
  consume(callback: (error: LibrdKafkaError, messages: Message[]) => void): void
  consume(): void
  getWatermarkOffsets(topic: string, partition: number): WatermarkOffsets
  offsetsStore(partitions: TopicPartitionOffset[]): any
  pause(partitions: TopicPartition[]): any
  position(partitions?: TopicPartition[]): TopicPartitionOffset[]
  resume(partitions: TopicPartition[]): any
  seek(partition: TopicPartitionOffset, timeout: number | null, callback: (error: LibrdKafkaError) => void): this
  setDefaultConsumeTimeout(timeout: number): void
  setDefaultConsumeLoopTimeoutDelay(timeout: number): void
  subscribe(topics: SubscribeTopicList): this
  subscription(): string[]
  unassign(): this
  incrementalUnassign(assignments: Assignment[]): this
  unsubscribe(): this
  offsetsForTimes(partitions: TopicPartitionTime[], timeout: number, callback?: (error: LibrdKafkaError, offsets: TopicPartitionOffset[]) => any): void
  offsetsForTimes(partitions: TopicPartitionTime[], callback?: (error: LibrdKafkaError, offsets: TopicPartitionOffset[]) => any): void
  rebalanceProtocol(): string
  static createReadStream(config: ConsumerGlobalConfig, topicConfig: ConsumerTopicConfig, options: ReadStreamOptions | number): ConsumerStream
}

export class Producer extends Client<KafkaProducerEvents> {
  constructor(config: ProducerGlobalConfig, topicConfig?: ProducerTopicConfig)
  flush(timeout?: NumberNullUndefined, callback?: (error: LibrdKafkaError) => void): this
  poll(): this
  produce(topic: string, partition: NumberNullUndefined, message: MessageValue, key?: MessageKey, timestamp?: NumberNullUndefined, opaque?: any, headers?: MessageHeader[]): any
  setPollInterval(interval: number): this
  static createWriteStream(config: ProducerGlobalConfig, topicConfig: ProducerTopicConfig, options: WriteStreamOptions): ProducerStream
  initTransactions(callback: (error: LibrdKafkaError) => void): void
  initTransactions(timeout: number, callback: (error: LibrdKafkaError) => void): void
  beginTransaction(callback: (error: LibrdKafkaError) => void): void
  commitTransaction(callback: (error: LibrdKafkaError) => void): void
  commitTransaction(timeout: number, callback: (error: LibrdKafkaError) => void): void
  abortTransaction(callback: (error: LibrdKafkaError) => void): void
  abortTransaction(timeout: number, callback: (error: LibrdKafkaError) => void): void
  sendOffsetsToTransaction(offsets: TopicPartitionOffset[], consumer: KafkaConsumer, callback: (error: LibrdKafkaError) => void): void
  sendOffsetsToTransaction(offsets: TopicPartitionOffset[], consumer: KafkaConsumer, timeout: number, callback: (error: LibrdKafkaError) => void): void
}

export class HighLevelProducer extends Producer {
  produce(topic: string, partition: NumberNullUndefined, message: any, key: any, timestamp: NumberNullUndefined, callback: (error: any, offset?: NumberNullUndefined) => void): any
  produce(topic: string, partition: NumberNullUndefined, message: any, key: any, timestamp: NumberNullUndefined, headers: MessageHeader[], callback: (error: any, offset?: NumberNullUndefined) => void): any
  setKeySerializer(serializer: (key: any, callback: (error: any, key: MessageKey) => void) => void): void
  setKeySerializer(serializer: (key: any) => MessageKey | Promise<MessageKey>): void
  setValueSerializer(serializer: (value: any, callback: (error: any, value: MessageValue) => void) => void): void
  setValueSerializer(serializer: (value: any) => MessageValue | Promise<MessageValue>): void
}

export declare const features: string[]
export declare const librdkafkaVersion: string
export declare function createReadStream(config: ConsumerGlobalConfig, topicConfig: ConsumerTopicConfig, options: ReadStreamOptions | number): ConsumerStream
export declare function createWriteStream(config: ProducerGlobalConfig, topicConfig: ProducerTopicConfig, options: WriteStreamOptions): ProducerStream

export interface NewTopic {
  topic: string
  num_partitions: number
  replication_factor: number
  config?: Record<string, string>
}

export interface IAdminClient {
  refreshOauthBearerToken(token: string): void
  createTopic(topic: NewTopic, callback?: (error: LibrdKafkaError) => void): void
  createTopic(topic: NewTopic, timeout?: number, callback?: (error: LibrdKafkaError) => void): void
  deleteTopic(topic: string, callback?: (error: LibrdKafkaError) => void): void
  deleteTopic(topic: string, timeout?: number, callback?: (error: LibrdKafkaError) => void): void
  createPartitions(topic: string, desiredPartitions: number, callback?: (error: LibrdKafkaError) => void): void
  createPartitions(topic: string, desiredPartitions: number, timeout?: number, callback?: (error: LibrdKafkaError) => void): void
  disconnect(): void
}

export abstract class AdminClient {
  static create(config: GlobalConfig, initialOauthBearerToken?: string): IAdminClient
}
