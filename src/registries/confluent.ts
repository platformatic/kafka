import avro, { type Type } from 'avsc'
import { type Callback } from '../apis/definitions.ts'
import {
  type BeforeDeserializationHook,
  type BeforeHookPayloadType,
  type BeforeSerializationHook,
  type Deserializer,
  type Deserializers,
  jsonDeserializer,
  jsonSerializer,
  type Serializer,
  type Serializers,
  stringDeserializer,
  stringSerializer
} from '../clients/serde.ts'
import { EMPTY_BUFFER, UserError } from '../index.ts'
import { type MessageToProduce } from '../protocol/records.ts'
import { AbstractSchemaRegistry } from './abstract.ts'

export interface ConfluentSchemaRegistryMetadata {
  schemas?: Record<BeforeHookPayloadType, number>
}

export interface ConfluentSchemaRegistryOptions {
  url: string
}

// TODO(ShogunPanda): Protobuf support
// TODO(ShogunPanda): JSON Schema support
// TODO(ShogunPanda): Authentication support
export class ConfluentSchemaRegistry extends AbstractSchemaRegistry {
  #url: string
  #schemas: Map<number, Type>

  constructor (options: ConfluentSchemaRegistryOptions) {
    super()
    this.#url = options.url
    this.#schemas = new Map()
  }

  getSchemaId<Key, Value, HeaderKey, HeaderValue> (
    message: Buffer | MessageToProduce<Key, Value, HeaderKey, HeaderValue>,
    type?: BeforeHookPayloadType
  ): number | undefined {
    if (Buffer.isBuffer(message)) {
      if (type !== 'value') {
        return undefined
      }

      return message.readInt32BE(1)
    }

    return (message.metadata as ConfluentSchemaRegistryMetadata)?.schemas?.[type!]
  }

  get (schemaId: number): Type | undefined {
    return this.#schemas.get(schemaId)
  }

  async fetchSchema (schemaId: number, callback: Callback<void>): Promise<void> {
    try {
      const response = await fetch(`${this.#url}/schemas/ids/${schemaId}`)
      const responseBody = await response.json()

      if (!response.ok) {
        throw new UserError(`Failed to fetch schema: [HTTP ${response.status}]`, { cause: responseBody as Error })
      }

      this.#schemas.set(schemaId, avro.Type.forSchema(JSON.parse((responseBody as { schema: string }).schema)))
      process.nextTick(callback)
    } catch (err) {
      process.nextTick(() => callback(err))
    }
  }

  getSerializers<Key, Value, HeaderKey, HeaderValue> (): Serializers<Key, Value, HeaderKey, HeaderValue> {
    return {
      key: this.#schemaSerializer.bind(this, 'key', stringSerializer),
      value: this.#schemaSerializer.bind(this, 'value', jsonSerializer),
      headerKey: this.#schemaSerializer.bind(this, 'headerKey', stringSerializer),
      headerValue: this.#schemaSerializer.bind(this, 'headerValue', jsonSerializer)
    } as Serializers<Key, Value, HeaderKey, HeaderValue>
  }

  getDeserializers<Key, Value, HeaderKey, HeaderValue> (): Deserializers<Key, Value, HeaderKey, HeaderValue> {
    return {
      key: this.#schemaDeserializer.bind(this, 'key', stringDeserializer),
      value: this.#schemaDeserializer.bind(this, 'value', jsonDeserializer),
      headerKey: this.#schemaDeserializer.bind(this, 'headerKey', stringDeserializer),
      headerValue: this.#schemaDeserializer.bind(this, 'headerValue', jsonDeserializer)
    } as Deserializers<Key, Value, HeaderKey, HeaderValue>
  }

  getBeforeSerializationHook<Key, Value, HeaderKey, HeaderValue> (): BeforeSerializationHook<
    Key,
    Value,
    HeaderKey,
    HeaderValue
  > {
    const registry = this

    return function beforeSerialization (
      _: unknown,
      type: BeforeHookPayloadType,
      message: MessageToProduce<Key, Value, HeaderKey, HeaderValue>,
      callback: Callback<void>
    ) {
      // Extract the schema ID from the message metadata
      const schemaId = registry.getSchemaId(message, type)

      // When no schema ID is found, nothing to do
      if (!schemaId) {
        callback(null)
        return
      }

      // The schema is already fetch
      if (registry.get(schemaId)) {
        callback(null)
        return
      }

      registry.fetchSchema(schemaId, callback)
    }
  }

  getBeforeDeserializationHook (): BeforeDeserializationHook {
    const registry = this

    return function beforeDeserialization (payload: Buffer, type: BeforeHookPayloadType, callback: Callback<void>) {
      // Extract the schema ID from the message metadata
      const schemaId = registry.getSchemaId(payload, type)

      // When no schema ID is found, nothing to do
      if (!schemaId) {
        callback(null)
        return
      }

      // The schema is already fetch
      if (registry.get(schemaId)) {
        callback(null)
        return
      }

      registry.fetchSchema(schemaId, callback)
    }
  }

  #schemaSerializer (
    type: BeforeHookPayloadType,
    fallbackSerializer: Serializer<unknown> | Serializer<string>,
    data?: unknown | string,
    headers?: Map<unknown, unknown>,
    metadata?: unknown
  ): Buffer | undefined {
    if (typeof data === 'undefined') {
      return EMPTY_BUFFER
    }

    if (type === 'headerKey' || type === 'headerValue') {
      metadata = headers
    }

    const schemaId = (metadata as ConfluentSchemaRegistryMetadata)?.schemas?.[type]

    if (!schemaId) {
      return fallbackSerializer(data as any)
    }

    const schema = this.#schemas.get(schemaId)
    if (!schema) {
      throw new UserError(`Schema with ID ${schemaId} not found.`, { missingSchema: schemaId })
    }

    const encodedMessage = schema.toBuffer(data)

    const header = Buffer.alloc(5)
    header.writeInt32BE(schemaId, 1)

    return Buffer.concat([header, encodedMessage])
  }

  #schemaDeserializer (
    type: BeforeHookPayloadType,
    fallbackDeserializer: Deserializer<unknown> | Deserializer<string>,
    data?: Buffer
  ): unknown {
    if (typeof data === 'undefined' || data.length === 0) {
      return EMPTY_BUFFER
    }

    const schemaId = this.getSchemaId(data as Buffer, type)

    if (!schemaId) {
      return fallbackDeserializer(data as any)
    }

    const schema = this.#schemas.get(schemaId)
    if (!schema) {
      throw new UserError(`Schema with ID ${schemaId} not found.`, { missingSchema: schemaId })
    }

    return schema.fromBuffer(data.subarray(5))
  }
}
