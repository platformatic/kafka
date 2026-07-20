import type { Socket } from 'node:net'
import type { ConnectionOptions } from 'node:tls'

type Without<T, U> = { [P in Exclude<keyof T, keyof U>]?: never }
type XOR<T, U> = T | U extends object ? (Without<T, U> & U) | (Without<U, T> & T) : T | U
type ValueOf<T> = T[keyof T]

export type BrokersFunction = () => string[] | Promise<string[]>
export type OauthbearerProviderResponse = { value: string }
export type Authenticator = { authenticate: () => Promise<void> }
export type SaslAuthenticateArgs<Result> = {
  request: { encode: () => Buffer | Promise<Buffer> }
  response?: { decode: (rawResponse: Buffer) => Buffer | Promise<Buffer>; parse: (data: Buffer) => Result }
}
export type AuthenticationProviderArgs = {
  host: string
  port: number
  logger: Logger
  saslAuthenticate: <Result>(args: SaslAuthenticateArgs<Result>) => Promise<Result | void>
}
export type Mechanism = {
  mechanism: string
  authenticationProvider: (args: AuthenticationProviderArgs) => Authenticator
}
export type SASLMechanism = 'plain' | 'scram-sha-256' | 'scram-sha-512' | 'aws' | 'oauthbearer'
type SASLMechanismOptionsMap = {
  plain: { username: string; password: string }
  'scram-sha-256': { username: string; password: string }
  'scram-sha-512': { username: string; password: string }
  aws: {
    authorizationIdentity: string
    accessKeyId: string
    secretAccessKey: string
    sessionToken?: string
  }
  oauthbearer: { oauthBearerProvider: () => Promise<OauthbearerProviderResponse> }
}
type SASLMechanismOptions<T> = T extends SASLMechanism ? { mechanism: T } & SASLMechanismOptionsMap[T] : never
export type SASLOptions = SASLMechanismOptions<SASLMechanism>

export interface ISocketFactoryArgs {
  host: string
  port: number
  ssl: ConnectionOptions
  onConnect: () => void
}
export type ISocketFactory = (args: ISocketFactoryArgs) => Socket

export interface KafkaConfig {
  brokers: string[] | BrokersFunction
  ssl?: ConnectionOptions | boolean
  sasl?: SASLOptions | Mechanism
  clientId?: string
  connectionTimeout?: number
  authenticationTimeout?: number
  reauthenticationThreshold?: number
  requestTimeout?: number
  enforceRequestTimeout?: boolean
  retry?: RetryOptions
  socketFactory?: ISocketFactory
  logLevel?: logLevel
  logCreator?: logCreator
}

export interface RetryOptions {
  maxRetryTime?: number
  initialRetryTime?: number
  factor?: number
  multiplier?: number
  retries?: number
  restartOnFailure?: (error: Error) => Promise<boolean>
}

export interface ProducerConfig {
  createPartitioner?: ICustomPartitioner
  retry?: RetryOptions
  metadataMaxAge?: number
  allowAutoTopicCreation?: boolean
  idempotent?: boolean
  transactionalId?: string
  transactionTimeout?: number
  maxInFlightRequests?: number
}

export interface ConsumerConfig {
  groupId: string
  partitionAssigners?: PartitionAssigner[]
  metadataMaxAge?: number
  sessionTimeout?: number
  rebalanceTimeout?: number
  heartbeatInterval?: number
  maxBytesPerPartition?: number
  minBytes?: number
  maxBytes?: number
  maxWaitTimeInMs?: number
  retry?: RetryOptions
  allowAutoTopicCreation?: boolean
  maxInFlightRequests?: number
  readUncommitted?: boolean
  rackId?: string
}

export interface AdminConfig {
  retry?: RetryOptions
}

export interface Message {
  key?: Buffer | string | null
  value: Buffer | string | null
  partition?: number
  headers?: IHeaders
  timestamp?: string
}
export interface IHeaders {
  [key: string]: Buffer | string | Array<Buffer | string> | undefined
}
export type PartitionMetadata = {
  partitionErrorCode: number
  partitionId: number
  leader: number
  replicas: number[]
  isr: number[]
  offlineReplicas?: number[]
}
export interface PartitionerArgs {
  topic: string
  partitionMetadata: PartitionMetadata[]
  message: Message
}
export type ICustomPartitioner = () => (args: PartitionerArgs) => number
export type DefaultPartitioner = ICustomPartitioner
export type LegacyPartitioner = ICustomPartitioner

export type Assignment = Record<string, number[]>
export type GroupMember = { memberId: string; memberMetadata: Buffer }
export type GroupMemberAssignment = { memberId: string; memberAssignment: Buffer }
export type GroupState = { name: string; metadata: Buffer }
export type Assigner = {
  name: string
  version: number
  assign: (group: { members: GroupMember[]; topics: string[] }) => Promise<GroupMemberAssignment[]>
  protocol: (subscription: { topics: string[] }) => GroupState
}
export type Cluster = {
  getNodeIds: () => number[]
  metadata: () => Promise<BrokerMetadata>
  removeBroker: (options: { host: string; port: number }) => void
  addMultipleTargetTopics: (topics: string[]) => Promise<void>
  isConnected: () => boolean
  connect: () => Promise<void>
  disconnect: () => Promise<void>
  refreshMetadata: () => Promise<void>
  refreshMetadataIfNecessary: () => Promise<void>
  addTargetTopic: (topic: string) => Promise<void>
  findBroker: (node: { nodeId: string }) => Promise<Broker>
  findControllerBroker: () => Promise<Broker>
  findTopicPartitionMetadata: (topic: string) => PartitionMetadata[]
  findLeaderForPartitions: (topic: string, partitions: number[]) => Record<string, number[]>
  findGroupCoordinator: (group: { groupId: string }) => Promise<Broker>
  findGroupCoordinatorMetadata: (group: { groupId: string }) => Promise<CoordinatorMetadata>
  defaultOffset: (config: { fromBeginning: boolean }) => number
  fetchTopicsOffset: (
    topics: Array<
      { topic: string; partitions: Array<{ partition: number }> } & XOR<
        { fromBeginning: boolean },
        { fromTimestamp: number }
      >
    >
  ) => Promise<TopicOffsets[]>
}
export type PartitionAssigner = (config: { cluster: Cluster; groupId: string; logger: Logger }) => Assigner

export interface ISerializer<Value> {
  encode: (value: Value) => Buffer
  decode: (buffer: Buffer) => Value | null
}
export type MemberMetadata = { version: number; topics: string[]; userData: Buffer }
export type MemberAssignment = { version: number; assignment: Assignment; userData: Buffer }

export type logLevel = 0 | 1 | 2 | 4 | 5
export interface LoggerEntryContent {
  readonly timestamp: string
  readonly message: string
  [key: string]: unknown
}
export interface LogEntry {
  namespace: string
  level: logLevel
  label: string
  log: LoggerEntryContent
}
export type logCreator = (level: logLevel) => (entry: LogEntry) => void
export type Logger = {
  info: (message: string, extra?: object) => void
  error: (message: string, extra?: object) => void
  warn: (message: string, extra?: object) => void
  debug: (message: string, extra?: object) => void
  namespace: (namespace: string, level?: logLevel) => Logger
  setLogLevel: (level: logLevel) => void
}

export type CompressionTypes = 0 | 1 | 2 | 3 | 4
export interface ProducerRecord {
  topic: string
  messages: Message[]
  acks?: number
  timeout?: number
  compression?: CompressionTypes
}
export interface TopicMessages {
  topic: string
  messages: Message[]
}
export interface ProducerBatch {
  acks?: number
  timeout?: number
  compression?: CompressionTypes
  topicMessages?: TopicMessages[]
}
export type RecordMetadata = {
  topicName: string
  partition: number
  errorCode: number
  offset?: string
  timestamp?: string
  baseOffset?: string
  logAppendTime?: string
  logStartOffset?: string
}
export interface PartitionOffset {
  partition: number
  offset: string
}
export interface TopicOffsets {
  topic: string
  partitions: PartitionOffset[]
}
export interface Offsets {
  topics: TopicOffsets[]
}
export interface Sender {
  send: (record: ProducerRecord) => Promise<RecordMetadata[]>
  sendBatch: (batch: ProducerBatch) => Promise<RecordMetadata[]>
}
export interface Transaction extends Sender {
  sendOffsets: (offsets: Offsets & { consumerGroupId: string }) => Promise<void>
  commit: () => Promise<void>
  abort: () => Promise<void>
  isActive: () => boolean
}

export type InstrumentationEvent<Payload> = {
  id: string
  type: string
  timestamp: number
  payload: Payload
}
export type RemoveInstrumentationEventListener<EventName> = (() => void) & { readonly eventName?: EventName }
export type ConnectEvent = InstrumentationEvent<null>
export type DisconnectEvent = InstrumentationEvent<null>
export type RequestEvent = InstrumentationEvent<{
  apiKey: number
  apiName: string
  apiVersion: number
  broker: string
  clientId: string
  correlationId: number
  createdAt: number
  duration: number
  pendingDuration: number
  sentAt: number
  size: number
}>
export type RequestTimeoutEvent = InstrumentationEvent<{
  apiKey: number
  apiName: string
  apiVersion: number
  broker: string
  clientId: string
  correlationId: number
  createdAt: number
  pendingDuration: number
  sentAt: number
}>
export type RequestQueueSizeEvent = InstrumentationEvent<{ broker: string; clientId: string; queueSize: number }>
export type ProducerEvents = {
  CONNECT: 'producer.connect'
  DISCONNECT: 'producer.disconnect'
  REQUEST: 'producer.network.request'
  REQUEST_TIMEOUT: 'producer.network.request_timeout'
  REQUEST_QUEUE_SIZE: 'producer.network.request_queue_size'
}
export type AdminEvents = {
  CONNECT: 'admin.connect'
  DISCONNECT: 'admin.disconnect'
  REQUEST: 'admin.network.request'
  REQUEST_TIMEOUT: 'admin.network.request_timeout'
  REQUEST_QUEUE_SIZE: 'admin.network.request_queue_size'
}
export type ConsumerEvents = {
  HEARTBEAT: 'consumer.heartbeat'
  COMMIT_OFFSETS: 'consumer.commit_offsets'
  GROUP_JOIN: 'consumer.group_join'
  FETCH_START: 'consumer.fetch_start'
  FETCH: 'consumer.fetch'
  START_BATCH_PROCESS: 'consumer.start_batch_process'
  END_BATCH_PROCESS: 'consumer.end_batch_process'
  CONNECT: 'consumer.connect'
  DISCONNECT: 'consumer.disconnect'
  STOP: 'consumer.stop'
  CRASH: 'consumer.crash'
  REBALANCING: 'consumer.rebalancing'
  RECEIVED_UNSUBSCRIBED_TOPICS: 'consumer.received_unsubscribed_topics'
  REQUEST: 'consumer.network.request'
  REQUEST_TIMEOUT: 'consumer.network.request_timeout'
  REQUEST_QUEUE_SIZE: 'consumer.network.request_queue_size'
}

export interface MessageSetEntry {
  key: Buffer | null
  value: Buffer | null
  timestamp: string
  attributes: number
  offset: string
  size: number
  headers?: never
}
export interface RecordBatchEntry {
  key: Buffer | null
  value: Buffer | null
  timestamp: string
  attributes: number
  offset: string
  headers: IHeaders
  size?: never
}
export type KafkaMessage = MessageSetEntry | RecordBatchEntry
export type TopicPartition = { topic: string; partition: number }
export type TopicPartitionOffset = TopicPartition & { offset: string }
export type TopicPartitionOffsetAndMetadata = TopicPartitionOffset & { metadata?: string | null }
export type TopicPartitions = { topic: string; partitions: number[] }
export type Batch = {
  topic: string
  partition: number
  highWatermark: string
  messages: KafkaMessage[]
  isEmpty: () => boolean
  firstOffset: () => string | null
  lastOffset: () => string
  offsetLag: () => string
  offsetLagLow: () => string
}
export interface EachMessagePayload {
  topic: string
  partition: number
  message: KafkaMessage
  heartbeat: () => Promise<void>
  pause: () => () => void
}
export interface EachBatchPayload {
  batch: Batch
  resolveOffset: (offset: string) => void
  heartbeat: () => Promise<void>
  pause: () => () => void
  commitOffsetsIfNecessary: (offsets?: Offsets) => Promise<void>
  uncommittedOffsets: () => OffsetsByTopicPartition
  isRunning: () => boolean
  isStale: () => boolean
}
export type ConsumerEachMessagePayload = EachMessagePayload
export type ConsumerEachBatchPayload = EachBatchPayload
export type EachMessageHandler = (payload: EachMessagePayload) => Promise<void>
export type EachBatchHandler = (payload: EachBatchPayload) => Promise<void>
export type ConsumerRunConfig = {
  autoCommit?: boolean
  autoCommitInterval?: number | null
  autoCommitThreshold?: number | null
  eachBatchAutoResolve?: boolean
  partitionsConsumedConcurrently?: number
  eachBatch?: EachBatchHandler
  eachMessage?: EachMessageHandler
}
export type ConsumerSubscribeTopic = { topic: string | RegExp; fromBeginning?: boolean }
export type ConsumerSubscribeTopics = { topics: Array<string | RegExp>; fromBeginning?: boolean }
export interface OffsetsByTopicPartition {
  topics: TopicOffsets[]
}
export type ConsumerHeartbeatEvent = InstrumentationEvent<{
  groupId: string
  memberId: string
  groupGenerationId: number
}>
export type ConsumerCommitOffsetsEvent = InstrumentationEvent<{
  groupId: string
  memberId: string
  groupGenerationId: number
  topics: TopicOffsets[]
}>
export interface IMemberAssignment {
  [key: string]: number[]
}
export type ConsumerGroupJoinEvent = InstrumentationEvent<{
  duration: number
  groupId: string
  isLeader: boolean
  leaderId: string
  groupProtocol: string
  memberId: string
  memberAssignment: IMemberAssignment
}>
export type ConsumerFetchStartEvent = InstrumentationEvent<{ nodeId: number }>
export type ConsumerFetchEvent = InstrumentationEvent<{
  numberOfBatches: number
  duration: number
  nodeId: number
}>
export interface IBatchProcessEvent {
  topic: string
  partition: number
  highWatermark: string
  offsetLag: string
  offsetLagLow: string
  batchSize: number
  firstOffset: string
  lastOffset: string
}
export type ConsumerStartBatchProcessEvent = InstrumentationEvent<IBatchProcessEvent>
export type ConsumerEndBatchProcessEvent = InstrumentationEvent<IBatchProcessEvent & { duration: number }>
export type ConsumerCrashEvent = InstrumentationEvent<{ error: Error; groupId: string; restart: boolean }>
export type ConsumerRebalancingEvent = InstrumentationEvent<{ groupId: string; memberId: string }>
export type ConsumerReceivedUnsubcribedTopicsEvent = InstrumentationEvent<{
  groupId: string
  generationId: number
  memberId: string
  assignedTopics: string[]
  topicsSubscribed: string[]
  topicsNotSubscribed: string[]
}>

export type Producer = Sender & {
  connect: () => Promise<void>
  disconnect: () => Promise<void>
  isIdempotent: () => boolean
  readonly events: ProducerEvents
  on: {
    (
      eventName: ProducerEvents['CONNECT'],
      listener: (event: ConnectEvent) => void
    ): RemoveInstrumentationEventListener<typeof eventName>
    (
      eventName: ProducerEvents['DISCONNECT'],
      listener: (event: DisconnectEvent) => void
    ): RemoveInstrumentationEventListener<typeof eventName>
    (
      eventName: ProducerEvents['REQUEST'],
      listener: (event: RequestEvent) => void
    ): RemoveInstrumentationEventListener<typeof eventName>
    (
      eventName: ProducerEvents['REQUEST_QUEUE_SIZE'],
      listener: (event: RequestQueueSizeEvent) => void
    ): RemoveInstrumentationEventListener<typeof eventName>
    (
      eventName: ProducerEvents['REQUEST_TIMEOUT'],
      listener: (event: RequestTimeoutEvent) => void
    ): RemoveInstrumentationEventListener<typeof eventName>
    (
      eventName: ValueOf<ProducerEvents>,
      listener: (event: InstrumentationEvent<any>) => void
    ): RemoveInstrumentationEventListener<typeof eventName>
  }
  transaction: () => Promise<Transaction>
  logger: () => Logger
}

export type Consumer = {
  connect: () => Promise<void>
  disconnect: () => Promise<void>
  subscribe: (subscription: ConsumerSubscribeTopics | ConsumerSubscribeTopic) => Promise<void>
  stop: () => Promise<void>
  run: (config?: ConsumerRunConfig) => Promise<void>
  commitOffsets: (topicPartitions: TopicPartitionOffsetAndMetadata[]) => Promise<void>
  seek: (topicPartitionOffset: TopicPartitionOffset) => void
  describeGroup: () => Promise<GroupDescription>
  pause: (topics: Array<{ topic: string; partitions?: number[] }>) => void
  paused: () => TopicPartitions[]
  resume: (topics: Array<{ topic: string; partitions?: number[] }>) => void
  on: {
    (
      eventName: ConsumerEvents['HEARTBEAT'],
      listener: (event: ConsumerHeartbeatEvent) => void
    ): RemoveInstrumentationEventListener<typeof eventName>
    (
      eventName: ConsumerEvents['COMMIT_OFFSETS'],
      listener: (event: ConsumerCommitOffsetsEvent) => void
    ): RemoveInstrumentationEventListener<typeof eventName>
    (
      eventName: ConsumerEvents['GROUP_JOIN'],
      listener: (event: ConsumerGroupJoinEvent) => void
    ): RemoveInstrumentationEventListener<typeof eventName>
    (
      eventName: ConsumerEvents['FETCH_START'],
      listener: (event: ConsumerFetchStartEvent) => void
    ): RemoveInstrumentationEventListener<typeof eventName>
    (
      eventName: ConsumerEvents['FETCH'],
      listener: (event: ConsumerFetchEvent) => void
    ): RemoveInstrumentationEventListener<typeof eventName>
    (
      eventName: ConsumerEvents['START_BATCH_PROCESS'],
      listener: (event: ConsumerStartBatchProcessEvent) => void
    ): RemoveInstrumentationEventListener<typeof eventName>
    (
      eventName: ConsumerEvents['END_BATCH_PROCESS'],
      listener: (event: ConsumerEndBatchProcessEvent) => void
    ): RemoveInstrumentationEventListener<typeof eventName>
    (
      eventName: ConsumerEvents['CONNECT'],
      listener: (event: ConnectEvent) => void
    ): RemoveInstrumentationEventListener<typeof eventName>
    (
      eventName: ConsumerEvents['DISCONNECT'],
      listener: (event: DisconnectEvent) => void
    ): RemoveInstrumentationEventListener<typeof eventName>
    (
      eventName: ConsumerEvents['STOP'],
      listener: (event: InstrumentationEvent<null>) => void
    ): RemoveInstrumentationEventListener<typeof eventName>
    (
      eventName: ConsumerEvents['CRASH'],
      listener: (event: ConsumerCrashEvent) => void
    ): RemoveInstrumentationEventListener<typeof eventName>
    (
      eventName: ConsumerEvents['REBALANCING'],
      listener: (event: ConsumerRebalancingEvent) => void
    ): RemoveInstrumentationEventListener<typeof eventName>
    (
      eventName: ConsumerEvents['RECEIVED_UNSUBSCRIBED_TOPICS'],
      listener: (event: ConsumerReceivedUnsubcribedTopicsEvent) => void
    ): RemoveInstrumentationEventListener<typeof eventName>
    (
      eventName: ConsumerEvents['REQUEST'],
      listener: (event: RequestEvent) => void
    ): RemoveInstrumentationEventListener<typeof eventName>
    (
      eventName: ConsumerEvents['REQUEST_TIMEOUT'],
      listener: (event: RequestTimeoutEvent) => void
    ): RemoveInstrumentationEventListener<typeof eventName>
    (
      eventName: ConsumerEvents['REQUEST_QUEUE_SIZE'],
      listener: (event: RequestQueueSizeEvent) => void
    ): RemoveInstrumentationEventListener<typeof eventName>
    (
      eventName: ValueOf<ConsumerEvents>,
      listener: (event: InstrumentationEvent<any>) => void
    ): RemoveInstrumentationEventListener<typeof eventName>
  }
  logger: () => Logger
  readonly events: ConsumerEvents
}

export type ConsumerGroupState = 'Unknown' | 'PreparingRebalance' | 'CompletingRebalance' | 'Stable' | 'Dead' | 'Empty'
export type MemberDescription = {
  clientHost: string
  clientId: string
  memberId: string
  memberAssignment: Buffer
  memberMetadata: Buffer
}
export type GroupDescription = {
  groupId: string
  members: MemberDescription[]
  protocol: string
  protocolType: string
  state: ConsumerGroupState
}
export type GroupDescriptions = { groups: GroupDescription[] }
export type GroupOverview = { groupId: string; protocolType: string }
export type DeleteGroupsResult = { groupId: string; errorCode?: number; error?: KafkaJSProtocolError }

export type ConfigResourceTypes = 0 | 2 | 4 | 8
export type ConfigSource = 0 | 1 | 2 | 3 | 4 | 5 | 6
export type AclResourceTypes = 0 | 1 | 2 | 3 | 4 | 5 | 6
export type AclOperationTypes = 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12
export type AclPermissionTypes = 0 | 1 | 2 | 3
export type ResourcePatternTypes = 0 | 1 | 2 | 3 | 4
export interface ITopicConfig {
  topic: string
  numPartitions?: number
  replicationFactor?: number
  replicaAssignment?: ReplicaAssignment[]
  configEntries?: IResourceConfigEntry[]
}
export interface ITopicPartitionConfig {
  topic: string
  count: number
  assignments?: number[][]
}
export interface ReplicaAssignment {
  partition: number
  replicas: number[]
}
export interface PartitionReassignment {
  topic: string
  partitionAssignment: ReplicaAssignment[]
}
export interface ITopicMetadata {
  name: string
  partitions: PartitionMetadata[]
}
export interface IResourceConfigEntry {
  name: string
  value: string
}
export interface IResourceConfig {
  type: ConfigResourceTypes
  name: string
  configEntries: IResourceConfigEntry[]
}
export interface ResourceConfigQuery {
  type: ConfigResourceTypes
  name: string
  configNames?: string[]
}
export interface ConfigSynonyms {
  configName: string
  configValue: string
  configSource: ConfigSource
}
export interface ConfigEntries {
  configName: string
  configValue: string
  isDefault: boolean
  configSource: ConfigSource
  isSensitive: boolean
  readOnly: boolean
  configSynonyms: ConfigSynonyms[]
}
export interface DescribeConfigResponse {
  resources: Array<{
    configEntries: ConfigEntries[]
    errorCode: number
    errorMessage: string
    resourceName: string
    resourceType: ConfigResourceTypes
  }>
  throttleTime: number
}
export interface Acl {
  principal: string
  host: string
  operation: AclOperationTypes
  permissionType: AclPermissionTypes
}
export interface AclResource {
  resourceType: AclResourceTypes
  resourceName: string
  resourcePatternType: ResourcePatternTypes
}
export type AclEntry = Acl & AclResource
export interface AclFilter {
  resourceType: AclResourceTypes
  resourceName?: string
  resourcePatternType: ResourcePatternTypes
  principal?: string
  host?: string
  operation: AclOperationTypes
  permissionType: AclPermissionTypes
}
export type DescribeAclResource = AclResource & { acls: Acl[] }
export interface DescribeAclResponse {
  throttleTime: number
  errorCode: number
  errorMessage?: string
  resources: DescribeAclResource[]
}
export interface MatchingAcl extends AclEntry {
  errorCode: number
  errorMessage?: string
}
export interface DeleteAclFilterResponses {
  errorCode: number
  errorMessage?: string
  matchingAcls: MatchingAcl[]
}
export interface DeleteAclResponse {
  throttleTime: number
  filterResponses: DeleteAclFilterResponses[]
}
export type SeekEntry = PartitionOffset
export type FetchOffsetsPartition = PartitionOffset & { metadata: string | null }
export interface OngoingPartitionReassignment {
  partitionIndex: number
  replicas: number[]
  addingReplicas?: number[]
  removingReplicas?: number[]
}
export interface OngoingTopicReassignment {
  topic: string
  partitions: OngoingPartitionReassignment[]
}
export interface ListPartitionReassignmentsResponse {
  topics: OngoingTopicReassignment[]
}

export type Admin = {
  connect: () => Promise<void>
  disconnect: () => Promise<void>
  listTopics: () => Promise<string[]>
  createTopics: (options: {
    validateOnly?: boolean
    waitForLeaders?: boolean
    timeout?: number
    topics: ITopicConfig[]
  }) => Promise<boolean>
  deleteTopics: (options: { topics: string[]; timeout?: number }) => Promise<void>
  createPartitions: (options: {
    validateOnly?: boolean
    timeout?: number
    topicPartitions: ITopicPartitionConfig[]
  }) => Promise<boolean>
  fetchTopicMetadata: (options?: { topics: string[] }) => Promise<{ topics: ITopicMetadata[] }>
  fetchOffsets: (options: {
    groupId: string
    topics?: string[]
    resolveOffsets?: boolean
  }) => Promise<Array<{ topic: string; partitions: FetchOffsetsPartition[] }>>
  fetchTopicOffsets: (topic: string) => Promise<Array<SeekEntry & { high: string; low: string }>>
  fetchTopicOffsetsByTimestamp: (topic: string, timestamp?: number) => Promise<SeekEntry[]>
  describeCluster: () => Promise<{
    brokers: Array<{ nodeId: number; host: string; port: number }>
    controller: number | null
    clusterId: string
  }>
  setOffsets: (options: { groupId: string; topic: string; partitions: SeekEntry[] }) => Promise<void>
  resetOffsets: (options: { groupId: string; topic: string; earliest: boolean }) => Promise<void>
  describeConfigs: (configs: {
    resources: ResourceConfigQuery[]
    includeSynonyms: boolean
  }) => Promise<DescribeConfigResponse>
  alterConfigs: (configs: { validateOnly: boolean; resources: IResourceConfig[] }) => Promise<any>
  listGroups: () => Promise<{ groups: GroupOverview[] }>
  deleteGroups: (groupIds: string[]) => Promise<DeleteGroupsResult[]>
  describeGroups: (groupIds: string[]) => Promise<GroupDescriptions>
  describeAcls: (options: AclFilter) => Promise<DescribeAclResponse>
  deleteAcls: (options: { filters: AclFilter[] }) => Promise<DeleteAclResponse>
  createAcls: (options: { acl: AclEntry[] }) => Promise<boolean>
  deleteTopicRecords: (options: { topic: string; partitions: SeekEntry[] }) => Promise<void>
  alterPartitionReassignments: (request: { topics: PartitionReassignment[]; timeout?: number }) => Promise<void>
  listPartitionReassignments: (request: {
    topics?: TopicPartitions[]
    timeout?: number
  }) => Promise<ListPartitionReassignmentsResponse>
  logger: () => Logger
  on: {
    (
      eventName: AdminEvents['CONNECT'],
      listener: (event: ConnectEvent) => void
    ): RemoveInstrumentationEventListener<typeof eventName>
    (
      eventName: AdminEvents['DISCONNECT'],
      listener: (event: DisconnectEvent) => void
    ): RemoveInstrumentationEventListener<typeof eventName>
    (
      eventName: AdminEvents['REQUEST'],
      listener: (event: RequestEvent) => void
    ): RemoveInstrumentationEventListener<typeof eventName>
    (
      eventName: AdminEvents['REQUEST_QUEUE_SIZE'],
      listener: (event: RequestQueueSizeEvent) => void
    ): RemoveInstrumentationEventListener<typeof eventName>
    (
      eventName: AdminEvents['REQUEST_TIMEOUT'],
      listener: (event: RequestTimeoutEvent) => void
    ): RemoveInstrumentationEventListener<typeof eventName>
    (
      eventName: ValueOf<AdminEvents>,
      listener: (event: InstrumentationEvent<any>) => void
    ): RemoveInstrumentationEventListener<typeof eventName>
  }
  readonly events: AdminEvents
}

export interface BrokerMetadata {
  brokers: Array<{ nodeId: number; host: string; port: number; rack?: string }>
  topicMetadata: Array<{ topicErrorCode: number; topic: string; partitionMetadata: PartitionMetadata[] }>
}
export interface ApiVersions {
  [apiKey: number]: { minVersion: number; maxVersion: number }
}
export type Broker = {
  isConnected: () => boolean
  connect: () => Promise<void>
  disconnect: () => Promise<void>
  apiVersions: () => Promise<ApiVersions>
  metadata: (topics: string[]) => Promise<BrokerMetadata>
  describeGroups: (options: { groupIds: string[] }) => Promise<any>
  offsetCommit: (request: {
    groupId: string
    groupGenerationId: number
    memberId: string
    retentionTime?: number
    topics: TopicOffsets[]
  }) => Promise<any>
  offsetFetch: (request: { groupId: string; topics: TopicOffsets[] }) => Promise<{ responses: TopicOffsets[] }>
  fetch: (request: {
    replicaId?: number
    isolationLevel?: number
    maxWaitTime?: number
    minBytes?: number
    maxBytes?: number
    topics: Array<{
      topic: string
      partitions: Array<{ partition: number; fetchOffset: string; maxBytes: number }>
    }>
    rackId?: string
  }) => Promise<any>
  produce: (request: {
    topicData: Array<{
      topic: string
      partitions: Array<{ partition: number; firstSequence?: number; messages: Message[] }>
    }>
    transactionalId?: string
    producerId?: number
    producerEpoch?: number
    acks?: number
    timeout?: number
    compression?: CompressionTypes
  }) => Promise<any>
  alterPartitionReassignments: (request: { topics: PartitionReassignment[]; timeout?: number }) => Promise<any>
  listPartitionReassignments: (request: {
    topics?: TopicPartitions[]
    timeout?: number
  }) => Promise<ListPartitionReassignmentsResponse>
}
export interface CoordinatorMetadata {
  errorCode: number
  coordinator: { nodeId: number; host: string; port: number }
}
export type ConsumerGroup = {
  groupId: string
  generationId: number
  memberId: string
  coordinator: Broker
}

export interface KafkaJSError extends Error {
  readonly retriable: boolean
  readonly helpUrl?: string
  readonly cause?: Error
}
export interface KafkaJSNonRetriableError extends KafkaJSError {}
export interface KafkaJSProtocolError extends KafkaJSError {
  readonly code: number
  readonly type: string
}
export interface KafkaJSAggregateError extends Error {
  readonly errors: Array<Error | string>
}
export interface KafkaJSOffsetOutOfRange extends KafkaJSProtocolError {
  readonly topic: string
  readonly partition: number
}
export interface KafkaJSAlterPartitionReassignmentsError extends KafkaJSProtocolError {
  readonly topic?: string
  readonly partition?: number
}
export interface KafkaJSNumberOfRetriesExceeded extends KafkaJSNonRetriableError {
  readonly retryCount: number
  readonly retryTime: number
}
export interface KafkaJSConnectionError extends KafkaJSError {
  readonly broker: string
}
export interface KafkaJSRequestTimeoutError extends KafkaJSError {
  readonly broker: string
  readonly correlationId: number
  readonly createdAt: number
  readonly sentAt: number
  readonly pendingDuration: number
}
export interface KafkaJSMetadataNotLoaded extends KafkaJSError {}
export interface KafkaJSTopicMetadataNotLoaded extends KafkaJSMetadataNotLoaded {
  readonly topic: string
}
export interface KafkaJSStaleTopicMetadataAssignment extends KafkaJSError {
  readonly topic: string
  readonly unknownPartitions: number
}
export interface KafkaJSServerDoesNotSupportApiKey extends KafkaJSNonRetriableError {
  readonly apiKey: number
  readonly apiName: string
}
export interface KafkaJSBrokerNotFound extends KafkaJSError {}
export interface KafkaJSPartialMessageError extends KafkaJSError {}
export interface KafkaJSSASLAuthenticationError extends KafkaJSError {}
export interface KafkaJSGroupCoordinatorNotFound extends KafkaJSError {}
export interface KafkaJSNotImplemented extends KafkaJSError {}
export interface KafkaJSTimeout extends KafkaJSError {}
export interface KafkaJSLockTimeout extends KafkaJSError {}
export interface KafkaJSUnsupportedMagicByteInMessageSet extends KafkaJSError {}
export interface KafkaJSDeleteGroupsError extends KafkaJSError {
  readonly groups: DeleteGroupsResult[]
}
export interface KafkaJSDeleteTopicRecordsError extends KafkaJSError {}

export interface KafkaJSErrorMetadata {
  retriable?: boolean
  topic?: string
  partitionId?: number
  metadata?: PartitionMetadata
}
export interface KafkaJSOffsetOutOfRangeMetadata {
  topic: string
  partition: number
}
export interface KafkaJSNumberOfRetriesExceededMetadata {
  retryCount: number
  retryTime: number
}
export interface KafkaJSConnectionErrorMetadata {
  broker?: string
  code?: string
}
export interface KafkaJSRequestTimeoutErrorMetadata {
  broker: string
  clientId: string
  correlationId: number
  createdAt: number
  sentAt: number
  pendingDuration: number
}
export interface KafkaJSTopicMetadataNotLoadedMetadata {
  topic: string
}
export interface KafkaJSStaleTopicMetadataAssignmentMetadata {
  topic: string
  unknownPartitions: PartitionMetadata[]
}
export interface KafkaJSServerDoesNotSupportApiKeyMetadata {
  apiKey: number
  apiName: string
}
export interface KafkaJSDeleteGroupsErrorGroups {
  groupId: string
  errorCode: number
  error: KafkaJSError
}
export interface KafkaJSDeleteTopicRecordsErrorPartition {
  partition: number
  offset: string
  error: KafkaJSError
}
export interface KafkaJSDeleteTopicRecordsErrorTopic {
  topic: string
  partitions: KafkaJSDeleteTopicRecordsErrorPartition[]
}

type ProducerOn = Producer['on']
type ConsumerOn = Consumer['on']
type AdminOn = Admin['on']

declare module './producer.ts' {
  interface Producer {
    on: ProducerOn
  }
}

declare module './consumer.ts' {
  interface Consumer {
    on: ConsumerOn
  }
}

declare module './admin.ts' {
  interface Admin {
    on: AdminOn
  }
}
