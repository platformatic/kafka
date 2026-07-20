import type { LibrdKafkaError } from './errors.ts'

export type RdkafkaConfigValue = string | number | boolean | Buffer | ((...args: never[]) => unknown) | undefined
export type RdkafkaConfig = Record<string, RdkafkaConfigValue>

export interface GlobalConfig {
  [key: string]: unknown
  'bootstrap.servers'?: string
  'metadata.broker.list'?: string
  'client.id'?: string
  'security.protocol'?: 'plaintext' | 'ssl' | 'sasl_plaintext' | 'sasl_ssl'
  'socket.timeout.ms'?: number
  'request.timeout.ms'?: number
  'topic.metadata.refresh.interval.ms'?: number
  'allow.auto.create.topics'?: boolean
  'max.in.flight.requests.per.connection'?: number
  'retry.backoff.ms'?: number
  retries?: number
  'ssl.ca.location'?: string
  'ssl.certificate.location'?: string
  'ssl.key.location'?: string
  'ssl.ca.pem'?: string
  'ssl.certificate.pem'?: string
  'ssl.key.pem'?: string
  'ssl.key.password'?: string
  'enable.ssl.certificate.verification'?: boolean
  'sasl.mechanisms'?: string
  'sasl.mechanism'?: string
  'sasl.username'?: string
  'sasl.password'?: string
  'oauthbearer.token'?: string
  error_cb?: boolean | ((error: LibrdKafkaError) => void)
  log_cb?: boolean | ((level: number, facility: string, message: string) => void)
  stats_cb?: boolean | ((statistics: string) => void)
  event_cb?: boolean
}

export interface TopicConfig {
  'auto.offset.reset'?: 'earliest' | 'latest' | 'smallest' | 'largest' | 'error'
  'message.timeout.ms'?: number
  'compression.codec'?: 'none' | 'gzip' | 'snappy' | 'lz4' | 'zstd' | 'inherit'
  'compression.type'?: 'none' | 'gzip' | 'snappy' | 'lz4' | 'zstd' | 'inherit'
}

export interface ProducerGlobalConfig extends GlobalConfig {
  'transactional.id'?: string
  'enable.idempotence'?: boolean
  'message.send.max.retries'?: number
  'queue.buffering.max.messages'?: number
  'queue.buffering.max.kbytes'?: number
  'queue.buffering.max.ms'?: number
  'request.required.acks'?: number | 'all'
  acks?: number | 'all'
  dr_cb?: boolean | ((error: LibrdKafkaError | null, report: DeliveryReport) => void)
  dr_msg_cb?: boolean
}

export interface ProducerTopicConfig extends TopicConfig {}

export interface ConsumerGlobalConfig extends GlobalConfig {
  'group.id'?: string
  'group.instance.id'?: string
  'enable.auto.commit'?: boolean
  'enable.auto.offset.store'?: boolean
  'auto.commit.interval.ms'?: number
  'session.timeout.ms'?: number
  'heartbeat.interval.ms'?: number
  'max.poll.interval.ms'?: number
  'fetch.min.bytes'?: number
  'fetch.max.bytes'?: number
  'max.partition.fetch.bytes'?: number
  'fetch.message.max.bytes'?: number
  'fetch.wait.max.ms'?: number
  'isolation.level'?: 'read_committed' | 'read_uncommitted'
  'enable.partition.eof'?: boolean
  'partition.assignment.strategy'?: string
  rebalance_cb?: boolean | ((error: LibrdKafkaError, assignments: TopicPartition[]) => void)
  offset_commit_cb?: boolean | ((error: LibrdKafkaError | null, offsets: TopicPartitionOffset[]) => void)
}

export interface ConsumerTopicConfig extends TopicConfig {}
export type NumberNullUndefined = number | null | undefined
export type MessageKey = Buffer | string | null | undefined
export type MessageValue = Buffer | null
export type MessageHeader = Record<string, string | Buffer>
export type SubscribeTopic = string | RegExp
export type SubscribeTopicList = SubscribeTopic[]

export interface TopicPartition {
  topic: string
  partition: number
}

export interface TopicPartitionOffset extends TopicPartition {
  offset: number
}

export interface TopicPartitionTime extends TopicPartition {
  offset: number
}

export interface Assignment extends TopicPartition {
  offset?: number | 'earliest' | 'beginning' | 'latest' | 'end' | 'stored'
}

export interface Message extends TopicPartitionOffset {
  offset: number
  value: Buffer | null
  size: number
  key?: Buffer | string | null
  timestamp?: number
  headers?: Record<string, string | Buffer>[]
  opaque?: unknown
}

export interface DeliveryReport extends TopicPartitionOffset {
  offset: number
  value?: Buffer | null
  size: number
  key?: Buffer | string | null
  timestamp?: number
  opaque?: unknown
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

export interface MetadataOptions {
  topic?: string
  allTopics?: boolean
  timeout?: number
}

export interface ReadyInfo {
  name: string
}

export interface ClientMetrics {
  connectionOpened: number
}

export interface EofEvent extends TopicPartitionOffset {
  offset: number
}

export interface WatermarkOffsets {
  lowOffset: number
  highOffset: number
}

export type NodeCallback<T = void> = (error?: LibrdKafkaError | null, value?: T) => void

export type KafkaClientEvents =
  | 'ready'
  | 'disconnected'
  | 'connection.failure'
  | 'event.error'
  | 'event.stats'
  | 'event.log'
  | 'event.event'
  | `event.${string}`
export type KafkaConsumerEvents = KafkaClientEvents | 'data' | 'partition.eof' | 'rebalance' | 'rebalance.error' | 'offset.commit' | 'subscribed' | 'unsubscribed' | 'unsubscribe' | 'warning'
export type KafkaProducerEvents = KafkaClientEvents | 'delivery-report'

export interface EventListenerMap {
  ready: [ReadyInfo, Metadata]
  disconnected: [ClientMetrics]
  'connection.failure': [LibrdKafkaError, ClientMetrics]
  'event.error': [LibrdKafkaError]
  'event.stats': [string]
  'event.log': [number, string, string]
  'event.event': [unknown]
  'event.throttle': [unknown]
  data: [Message]
  'partition.eof': [EofEvent]
  rebalance: [LibrdKafkaError, TopicPartition[]]
  'rebalance.error': [LibrdKafkaError]
  'offset.commit': [LibrdKafkaError | null, TopicPartitionOffset[]]
  'delivery-report': [LibrdKafkaError | null, DeliveryReport]
  subscribed: [SubscribeTopicList]
  unsubscribed: [TopicPartition[]]
  unsubscribe: [TopicPartition[]]
  warning: [LibrdKafkaError]
}

export interface NewTopic {
  topic: string
  num_partitions: number
  replication_factor: number
  config?: Record<string, string>
}

export interface IAdminClient {
  createTopic (topic: NewTopic, callback?: NodeCallback): void
  createTopic (topic: NewTopic, timeout?: number, callback?: NodeCallback): void
  deleteTopic (topic: string, callback?: NodeCallback): void
  deleteTopic (topic: string, timeout?: number, callback?: NodeCallback): void
  createPartitions (topic: string, desiredPartitions: number, callback?: NodeCallback): void
  createPartitions (topic: string, desiredPartitions: number, timeout?: number, callback?: NodeCallback): void
  refreshOauthBearerToken (token: string): void
  disconnect (): void
}
