import { type Broker, type ConnectionOptions } from '../../network/connection.ts'
import { type Metrics } from '../metrics.ts'

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
  lastUpdate: number
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
  retries?: number | boolean
  retryDelay?: number
  metadataMaxAge?: number
  autocreateTopics?: boolean
  strict?: boolean
  metrics?: Metrics
}

export interface MetadataOptions {
  topics: string[]
  autocreateTopics?: boolean
  forceUpdate?: boolean
  metadataMaxAge?: number
}
