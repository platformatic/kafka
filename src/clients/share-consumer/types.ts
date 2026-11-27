import { type BaseOptions } from '../base/types.ts'
import { type Deserializers } from '../serde.ts'

export interface ShareFetchOptions<Key = Buffer, Value = Buffer, HeaderKey = Buffer, HeaderValue = Buffer> {
  deserializers?: Partial<Deserializers<Key, Value, HeaderKey, HeaderValue>>
  minBytes?: number
  maxBytes?: number
  maxWaitTime?: number
  maxRecords?: number
  highWaterMark?: number
}

export interface ShareConsumerOptions<Key = Buffer, Value = Buffer, HeaderKey = Buffer, HeaderValue = Buffer>
  extends BaseOptions, ShareFetchOptions<Key, Value, HeaderKey, HeaderValue> {
  groupId: string
  leaveGroupTimeout?: number
}

export interface ShareGroupOptions {
  topics: string[]
}

export interface ShareConsumeOptions<Key = Buffer, Value = Buffer, HeaderKey = Buffer, HeaderValue = Buffer>
  extends ShareGroupOptions, ShareFetchOptions<Key, Value, HeaderKey, HeaderValue> {}

export interface ShareMessageJSON<Key = unknown, Value = unknown, HeaderKey = unknown, HeaderValue = unknown> {
  key: Key
  value: Value
  headers: Array<[HeaderKey, HeaderValue]>
  topic: string
  partition: number
  timestamp: string
  offset: string
  deliveryCount: number
}

export interface ShareMessage<Key = Buffer, Value = Buffer, HeaderKey = Buffer, HeaderValue = Buffer> {
  key: Key
  value: Value
  headers: Map<HeaderKey, HeaderValue>
  headerEntries: Array<[HeaderKey, HeaderValue]>
  topic: string
  partition: number
  timestamp: bigint
  offset: bigint
  deliveryCount: number
  toJSON (): ShareMessageJSON<Key, Value, HeaderKey, HeaderValue>
}

export interface ShareConsumerGroupPayload {
  groupId: string
  memberId: string
  memberEpoch: number
}

export interface ShareConsumerHeartbeatErrorPayload extends ShareConsumerGroupPayload {
  error: Error
}

export interface ShareConsumerAcknowledgementErrorPayload extends ShareConsumerGroupPayload {
  topic: string
  partition: number
  error: Error
}
