import type { MessagesStreamFallbackModeValue } from '../../clients/consumer/types.ts'

export interface KafkaJSFetchBatchMetadata {
  highWatermark: bigint
  lastOffset: bigint
  seekVersion: number
  membershipVersion: number
}

export interface KafkaJSFetchProgress {
  topic: string
  partition: number
  highWatermark: bigint
  lastOffset: bigint
  seekVersion: number
  membershipVersion: number
}

export const kKafkaJSFallbackModes = Symbol('plt.kafka.compatibility.kafkajs.fallbackModes')
export const kKafkaJSCommitMetadata = Symbol('plt.kafka.compatibility.kafkajs.commitMetadata')
export const kKafkaJSFetchBatch = Symbol('plt.kafka.compatibility.kafkajs.fetchBatch')
export const kKafkaJSProduceTimeout = Symbol('plt.kafka.compatibility.kafkajs.produceTimeout')
export const kKafkaJSFetchProgress = Symbol('plt.kafka.compatibility.kafkajs.fetchProgress')
export const kKafkaJSCreateAssignments = Symbol('plt.kafka.compatibility.kafkajs.createAssignments')

export type KafkaJSFallbackModes = Record<string, MessagesStreamFallbackModeValue>
