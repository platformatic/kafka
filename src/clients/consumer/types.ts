import { type FetchRequestTopic } from '../../apis/consumer/fetch-v17.ts'
import { type FetchIsolationLevel } from '../../apis/enumerations.ts'
import { type KafkaRecord, type Message } from '../../protocol/records.ts'
import { type BaseOptions, type ClusterMetadata, type TopicWithPartitionAndOffset } from '../base/types.ts'
import { type Deserializers } from '../serde.ts'

export interface GroupProtocolSubscription {
  name: string
  version: number
  metadata?: Buffer | string
}

export interface GroupAssignment {
  topic: string
  partitions: number[]
}

export interface GroupPartitionsAssignments {
  memberId: string
  assignments: Map<string, GroupAssignment>
}

export interface ExtendedGroupProtocolSubscription extends Omit<GroupProtocolSubscription, 'name'> {
  topics?: string[]
  // This is only used in responses
  memberId: string
}

export type Offsets = Map<string, bigint[]>

export type CorruptedMessageHandler = (
  record: KafkaRecord,
  topic: string,
  partition: number,
  firstTimestamp: bigint,
  firstOffset: bigint,
  commit: Message['commit']
) => boolean

export type GroupPartitionsAssigner = (
  current: string,
  members: Map<string, ExtendedGroupProtocolSubscription>,
  topics: Set<string>,
  metadata: ClusterMetadata
) => GroupPartitionsAssignments[]

export const MessagesStreamModes = {
  LATEST: 'latest',
  EARLIEST: 'earliest',
  COMMITTED: 'committed',
  MANUAL: 'manual'
} as const
export type MessagesStreamMode = keyof typeof MessagesStreamModes
export type MessagesStreamModeValue = (typeof MessagesStreamModes)[keyof typeof MessagesStreamModes]

export const MessagesStreamFallbackModes = {
  LATEST: 'latest',
  EARLIEST: 'earliest',
  FAIL: 'fail'
} as const
export type MessagesStreamFallbackMode = keyof typeof MessagesStreamFallbackModes
export type MessagesStreamFallbackModeValue =
  (typeof MessagesStreamFallbackModes)[keyof typeof MessagesStreamFallbackModes]
export interface GroupOptions {
  sessionTimeout?: number
  rebalanceTimeout?: number
  heartbeatInterval?: number
  protocols?: GroupProtocolSubscription[]
  partitionAssigner?: GroupPartitionsAssigner
}

export interface ConsumeBaseOptions<Key, Value, HeaderKey, HeaderValue> {
  autocommit?: boolean | number
  minBytes?: number
  maxBytes?: number
  maxWaitTime?: number
  isolationLevel?: FetchIsolationLevel
  deserializers?: Partial<Deserializers<Key, Value, HeaderKey, HeaderValue>>
  highWaterMark?: number
}

export interface StreamOptions {
  topics: string[]
  mode?: MessagesStreamModeValue
  fallbackMode?: MessagesStreamFallbackModeValue
  offsets?: TopicWithPartitionAndOffset[]
  onCorruptedMessage?: CorruptedMessageHandler
}

export type ConsumeOptions<Key, Value, HeaderKey, HeaderValue> = StreamOptions &
  ConsumeBaseOptions<Key, Value, HeaderKey, HeaderValue> &
  GroupOptions

export type ConsumerOptions<Key, Value, HeaderKey, HeaderValue> = BaseOptions & { groupId: string } & GroupOptions &
  ConsumeBaseOptions<Key, Value, HeaderKey, HeaderValue>

export type FetchOptions<Key, Value, HeaderKey, HeaderValue> = Omit<
  ConsumeBaseOptions<Key, Value, HeaderKey, HeaderValue>,
  'deserializers'
> &
  GroupOptions & {
    node: number
    topics: FetchRequestTopic[]
  }

export interface CommitOptionsPartition extends TopicWithPartitionAndOffset {
  leaderEpoch: number
}

export interface CommitOptions {
  offsets: CommitOptionsPartition[]
}

export interface ListCommitsOptions {
  topics: GroupAssignment[]
}

export interface ListOffsetsOptions {
  topics: string[]
  timestamp?: bigint
  isolationLevel?: FetchIsolationLevel
}
