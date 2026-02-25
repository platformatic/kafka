import { type CompressionAlgorithmValue } from '../../protocol/compression.ts'
import { type MessageToProduce } from '../../protocol/records.ts'
import { type SchemaRegistry } from '../../registries/abstract.ts'
import { type BaseOptions, type TopicWithPartitionAndOffset } from '../base/types.ts'
import { type BeforeSerializationHook, type Serializers } from '../serde.ts'

export interface ProducerInfo {
  producerId: bigint
  producerEpoch: number
  transactionalId?: string
}

export interface ProduceResult {
  // This is only defined when ack is not NO_RESPONSE
  offsets?: TopicWithPartitionAndOffset[]
  // This is only defined when ack is NO_RESPONSE
  unwritableNodes?: number[]
}

export type Partitioner<Key, Value, HeaderKey, HeaderValue> = (
  message: MessageToProduce<Key, Value, HeaderKey, HeaderValue>
) => number

export interface ProducerStreamReport {
  batchId: number
  count: number
  result: ProduceResult
}

export interface ProducerStreamMessageReport<Key, Value, HeaderKey, HeaderValue> {
  batchId: number
  index: number
  message: MessageToProduce<Key, Value, HeaderKey, HeaderValue>
}

export interface ProduceOptions<Key, Value, HeaderKey, HeaderValue> {
  producerId?: bigint
  producerEpoch?: number
  idempotent?: boolean
  acks?: number
  compression?: CompressionAlgorithmValue
  partitioner?: Partitioner<Key, Value, HeaderKey, HeaderValue>
  autocreateTopics?: boolean
  repeatOnStaleMetadata?: boolean
}

export type ProducerOptions<Key, Value, HeaderKey, HeaderValue> = BaseOptions &
  ProduceOptions<Key, Value, HeaderKey, HeaderValue> & {
    transactionalId?: string
    serializers?: Partial<Serializers<Key, Value, HeaderKey, HeaderValue>>
    beforeSerialization?: BeforeSerializationHook<Key, Value, HeaderKey, HeaderValue>
    registry?: SchemaRegistry<unknown, unknown, Key, Value, HeaderKey, HeaderValue>
  }

export type SendOptions<Key, Value, HeaderKey, HeaderValue> = {
  messages: MessageToProduce<Key, Value, HeaderKey, HeaderValue>[]
} & ProduceOptions<Key, Value, HeaderKey, HeaderValue>

export const ProducerStreamReportModes = {
  NONE: 'none',
  BATCH: 'batch',
  MESSAGE: 'message'
} as const
export const allowedProducerStreamReportModes = Object.values(
  ProducerStreamReportModes
) as ProducerStreamReportModeValue[]

export type ProducerStreamReportMode = keyof typeof ProducerStreamReportModes
export type ProducerStreamReportModeValue = (typeof ProducerStreamReportModes)[keyof typeof ProducerStreamReportModes]

export type ProducerStreamOptions<Key, Value, HeaderKey, HeaderValue> = Omit<
  SendOptions<Key, Value, HeaderKey, HeaderValue>,
  'messages'
> & {
  highWaterMark?: number
  batchSize?: number
  batchTime?: number
  reportMode?: ProducerStreamReportModeValue
}
