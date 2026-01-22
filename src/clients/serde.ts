import { type Callback } from '../apis/definitions.ts'
import { type MessageToConsume, type MessageToProduce } from '../protocol/records.ts'

export type Serializer<InputType = unknown> = (data?: InputType, metadata?: unknown) => Buffer | undefined
export type SerializerWithHeaders<InputType = unknown, HeaderKey = unknown, HeaderValue = unknown> = (
  data?: InputType,
  headers?: Map<HeaderKey, HeaderValue>,
  message?: MessageToProduce<unknown, unknown, unknown, unknown>
) => Buffer | undefined

export type Deserializer<OutputType = unknown> = (data?: Buffer, message?: MessageToConsume) => OutputType | undefined
export type DeserializerWithHeaders<OutputType = unknown, HeaderKey = unknown, HeaderValue = unknown> = (
  data?: Buffer,
  headers?: Map<HeaderKey, HeaderValue>,
  message?: MessageToConsume
) => OutputType | undefined

export interface Serializers<Key, Value, HeaderKey, HeaderValue> {
  key: SerializerWithHeaders<Key, HeaderKey, HeaderValue>
  value: SerializerWithHeaders<Value, HeaderKey, HeaderValue>
  headerKey: Serializer<HeaderKey>
  headerValue: Serializer<HeaderValue>
}

export interface Deserializers<Key, Value, HeaderKey, HeaderValue> {
  key: DeserializerWithHeaders<Key, HeaderKey, HeaderValue>
  value: DeserializerWithHeaders<Value, HeaderKey, HeaderValue>
  headerKey: Deserializer<HeaderKey>
  headerValue: Deserializer<HeaderValue>
}

export type BeforeHookPayloadType = 'key' | 'value' | 'headerKey' | 'headerValue'

export type BeforeDeserializationHook = (
  payload: Buffer,
  type: BeforeHookPayloadType,
  message: MessageToConsume,
  callback: Callback<void>
) => void | Promise<void>

export type BeforeSerializationHook<Key, Value, HeaderKey, HeaderValue> = (
  payload: unknown,
  type: BeforeHookPayloadType,
  message: MessageToProduce<Key, Value, HeaderKey, HeaderValue>,
  callback: Callback<void>
) => void | Promise<void>

export function stringSerializer (data?: string): Buffer | undefined {
  if (typeof data !== 'string') {
    return undefined
  }

  return Buffer.from(data, 'utf-8')
}

export function stringDeserializer (data?: string | Buffer): string | undefined {
  if (!Buffer.isBuffer(data)) {
    return undefined
  }

  return data.toString('utf-8')
}

export function jsonSerializer<T = Record<string, any>> (data?: T): Buffer | undefined {
  return Buffer.from(JSON.stringify(data), 'utf-8')
}

export function jsonDeserializer<T = Record<string, any>> (data?: string | Buffer): T | undefined {
  if (!Buffer.isBuffer(data)) {
    return undefined
  }

  return JSON.parse(data.toString('utf-8')) as T
}

export function serializersFrom<T> (serializer: Serializer<T>): Serializers<T, T, T, T> {
  return {
    key: serializer,
    value: serializer,
    headerKey: serializer,
    headerValue: serializer
  }
}

export function deserializersFrom<T> (deserializer: Deserializer<T>): Deserializers<T, T, T, T> {
  return {
    key: deserializer as DeserializerWithHeaders<T, T, T>,
    value: deserializer as DeserializerWithHeaders<T, T, T>,
    headerKey: deserializer,
    headerValue: deserializer
  }
}

export const serdeProperties = {
  type: 'object',
  properties: {
    key: { function: true },
    value: { function: true },
    headerKey: { function: true },
    headerValue: { function: true }
  },
  additionalProperties: false
}

export const stringSerializers = serializersFrom(stringSerializer)
export const stringDeserializers = deserializersFrom(stringDeserializer)
