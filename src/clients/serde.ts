export type Serializer<InputType = unknown> = (data?: InputType) => Buffer | undefined
export type Deserializer<OutputType = unknown> = (data?: Buffer) => OutputType | undefined

export interface Serializers<Key, Value, HeaderKey, HeaderValue> {
  key: Serializer<Key>
  value: Serializer<Value>
  headerKey: Serializer<HeaderKey>
  headerValue: Serializer<HeaderValue>
}

export interface Deserializers<Key, Value, HeaderKey, HeaderValue> {
  key: Deserializer<Key>
  value: Deserializer<Value>
  headerKey: Deserializer<HeaderKey>
  headerValue: Deserializer<HeaderValue>
}

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

export const stringSerializers: Serializers<string, string, string, string> = {
  key: stringSerializer,
  value: stringSerializer,
  headerKey: stringSerializer,
  headerValue: stringSerializer
}

export const stringDeserializers: Deserializers<string, string, string, string> = {
  key: stringDeserializer,
  value: stringDeserializer,
  headerKey: stringDeserializer,
  headerValue: stringDeserializer
}
