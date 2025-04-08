import { type Broker, type ConnectionOptions } from '../../network/connection.ts'

export interface TopicWithPartitionAndOffset {
  topic: string
  partition: number
  offset: bigint
}

export interface ClusterPartitionMetadata {
  leader: number
  leaderEpoch: number
  replicas: number[]
}

export interface ClusterTopicMetadata {
  id: string
  partitions: ClusterPartitionMetadata[]
  partitionsCount: number
}

export interface ClusterMetadata {
  id: string
  brokers: Map<number, Broker>
  topics: Map<string, ClusterTopicMetadata>
  lastUpdate: number
}

export interface BaseOptions extends ConnectionOptions {
  clientId: string
  bootstrapBrokers: Broker[] | string[]
  timeout?: number
  retries?: number
  retryDelay?: number
  metadataMaxAge?: number
  autocreateTopics?: boolean
  strict?: boolean
}

export interface MetadataOptions {
  topics: string[]
  autocreateTopics?: boolean
  forceUpdate?: boolean
  metadataMaxAge?: number
}
