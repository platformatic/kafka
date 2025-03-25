import { type CompressionAlgorithms } from '../../protocol/compression.ts'
import { type Message } from '../../protocol/records.ts'
import { type BaseOptions, type TopicWithPartitionAndOffset } from '../base/types.ts'
import { type Serializers } from '../serde.ts'

export interface ProducerInfo {
  producerId: bigint
  producerEpoch: number
}

export interface ProduceResult {
  // This is only defined when ack is not NO_RESPONSE
  offsets?: TopicWithPartitionAndOffset[]
  // This is only defined when ack is NO_RESPONSE
  unwritableNodes?: number[]
}

export type Partitioner<Key, Value, HeaderKey, HeaderValue> = (
  message: Message<Key, Value, HeaderKey, HeaderValue>
) => number

export interface ProduceOptions<Key, Value, HeaderKey, HeaderValue> {
  producerId?: bigint
  producerEpoch?: number
  idempotent?: boolean
  acks?: number
  compression?: CompressionAlgorithms
  partitioner?: Partitioner<Key, Value, HeaderKey, HeaderValue>
  repeatOnStaleMetadata?: boolean
  serializers?: Serializers<Key, Value, HeaderKey, HeaderValue>
}

export type ProducerOptions<Key, Value, HeaderKey, HeaderValue> = BaseOptions &
  ProduceOptions<Key, Value, HeaderKey, HeaderValue>

export type SendOptions<Key, Value, HeaderKey, HeaderValue> = {
  messages: Message<Key, Value, HeaderKey, HeaderValue>[]
} & ProduceOptions<Key, Value, HeaderKey, HeaderValue>
