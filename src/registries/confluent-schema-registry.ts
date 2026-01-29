import { type ValidateFunction } from 'ajv'
import avro, { type Type } from 'avsc'
import { createRequire } from 'node:module'
import { type parse, type Root } from 'protobufjs'
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
import { ajv, type CredentialProvider, EMPTY_BUFFER, UnsupportedFormatError, UserError } from '../index.ts'
import { type MessageToConsume, type MessageToProduce } from '../protocol/records.ts'
import { getCredential } from '../protocol/sasl/utils.ts'
import { AbstractSchemaRegistry } from './abstract.ts'

const require = createRequire(import.meta.url)

type ConfluentSchemaRegistryMessageToProduce = MessageToProduce<unknown, unknown, unknown, unknown>

export interface ConfluentSchemaRegistryMetadata {
  schemas?: Record<BeforeHookPayloadType, number>
}

export type ConfluentSchemaRegistryProtobufTypeMapper = (
  id: number,
  type: BeforeHookPayloadType,
  context: ConfluentSchemaRegistryMessageToProduce | MessageToConsume
) => string

export interface ConfluentSchemaRegistryOptions {
  url: string
  auth?: {
    username?: string | CredentialProvider
    password?: string | CredentialProvider
    token?: string | CredentialProvider
  }
  protobufTypeMapper?: ConfluentSchemaRegistryProtobufTypeMapper
  jsonValidateSend?: boolean
}

export interface Schema {
  id: number
  type: 'avro' | 'protobuf' | 'json'
  schema: Type | Root | ValidateFunction
}

/* c8 ignore next 8 */
export function defaultProtobufTypeMapper (
  _: number,
  type: BeforeHookPayloadType,
  context: ConfluentSchemaRegistryMessageToProduce | MessageToConsume
): string {
  // Confluent Schema Registry convention
  return `${context.topic!}-${type}`
}

// TODO(ShogunPanda): Authentication support
export class ConfluentSchemaRegistry<
  Key = Buffer,
  Value = Buffer,
  HeaderKey = Buffer,
  HeaderValue = Buffer
> extends AbstractSchemaRegistry<number | undefined, Schema, Key, Value, HeaderKey, HeaderValue> {
  #url: string
  #schemas: Map<number, Schema>
  #protobufParse: typeof parse | undefined
  #protobufTypeMapper: ConfluentSchemaRegistryProtobufTypeMapper
  #jsonValidateSend: boolean
  #auth: ConfluentSchemaRegistryOptions['auth'] | undefined

  constructor (options: ConfluentSchemaRegistryOptions) {
    super()
    this.#url = options.url
    this.#schemas = new Map()
    this.#protobufTypeMapper = options.protobufTypeMapper ?? defaultProtobufTypeMapper
    this.#jsonValidateSend = options.jsonValidateSend ?? false
    this.#auth = options.auth
  }

  getSchemaId (
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

  get (id: number): Schema | undefined {
    return this.#schemas.get(id)
  }

  async fetchSchema (id: number, callback: Callback<void>): Promise<void> {
    try {
      const requestInit: RequestInit = {}

      if (this.#auth) {
        if (this.#auth.token) {
          const token = await getCredential('token', this.#auth.token)

          requestInit.headers = {
            Authorization: `Bearer ${token}`
          }
        } else {
          const username = await getCredential('username', this.#auth.username)
          const password = await getCredential('password', this.#auth.password)

          requestInit.headers = {
            Authorization: `Basic ${Buffer.from(`${username}:${password}`).toString('base64')}`
          }
        }
      }

      const response = await fetch(`${this.#url}/schemas/ids/${id}`, requestInit)

      if (!response.ok) {
        throw new UserError(`Failed to fetch a schema: [HTTP ${response.status}]`, { response: await response.text() })
      }

      const responseBody = await response.json()
      const { schema, schemaType } = responseBody as { schemaType: string; schema: string }

      switch (schemaType) {
        case 'AVRO':
          this.#schemas.set(id, { id, type: 'avro', schema: avro.Type.forSchema(JSON.parse(schema)) })
          break
        case 'PROTOBUF':
          this.#protobufParse ??= this.#loadProtobuf()
          this.#schemas.set(id, { id, type: 'protobuf', schema: this.#protobufParse!(schema).root })
          break
        case 'JSON':
          this.#schemas.set(id, { id, type: 'json', schema: ajv.compile(JSON.parse(schema)) })
          break
      }

      process.nextTick(callback)
    } catch (err) {
      process.nextTick(() => callback(err))
    }
  }

  getSerializers (): Serializers<Key, Value, HeaderKey, HeaderValue> {
    return {
      key: this.#schemaSerializer.bind(this, 'key', stringSerializer),
      value: this.#schemaSerializer.bind(this, 'value', jsonSerializer),
      headerKey: this.#schemaSerializer.bind(this, 'headerKey', stringSerializer),
      headerValue: this.#schemaSerializer.bind(this, 'headerValue', jsonSerializer)
    } as Serializers<Key, Value, HeaderKey, HeaderValue>
  }

  getDeserializers (): Deserializers<Key, Value, HeaderKey, HeaderValue> {
    return {
      key: this.#schemaDeserializer.bind(this, 'key', stringDeserializer),
      value: this.#schemaDeserializer.bind(this, 'value', jsonDeserializer),
      headerKey: this.#schemaDeserializer.bind(this, 'headerKey', stringDeserializer),
      headerValue: this.#schemaDeserializer.bind(this, 'headerValue', jsonDeserializer)
    } as Deserializers<Key, Value, HeaderKey, HeaderValue>
  }

  getBeforeSerializationHook (): BeforeSerializationHook<Key, Value, HeaderKey, HeaderValue> {
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

    return function beforeDeserialization (
      payload: Buffer,
      type: BeforeHookPayloadType,
      _message: MessageToConsume,
      callback: Callback<void>
    ) {
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
    headers?: Map<string, string>,
    message?: ConfluentSchemaRegistryMessageToProduce
  ): Buffer | undefined {
    /* c8 ignore next 3 - Hard to test */
    if (typeof data === 'undefined') {
      return EMPTY_BUFFER
    }

    if (type === 'headerKey' || type === 'headerValue') {
      message = headers as unknown as ConfluentSchemaRegistryMessageToProduce
    }

    const schemaId = (message?.metadata as ConfluentSchemaRegistryMetadata)?.schemas?.[type]

    if (!schemaId) {
      return fallbackSerializer(data as any)
    }

    const schema = this.#schemas.get(schemaId)
    if (!schema) {
      throw new UserError(`Schema with ID ${schemaId} not found.`, { missingSchema: schemaId })
    }

    let encodedMessage: Buffer
    switch (schema.type) {
      case 'avro':
        encodedMessage = (schema.schema as Type).toBuffer(data)
        break
      case 'protobuf':
        {
          const typeName = this.#protobufTypeMapper(schemaId, type, message as ConfluentSchemaRegistryMessageToProduce)
          const Type = (schema.schema as Root).lookupType(typeName)
          encodedMessage = Buffer.from(Type.encode(Type.create(data as any)).finish())
        }

        break
      case 'json':
        if (this.#jsonValidateSend) {
          const validate = schema.schema as ValidateFunction
          const valid = validate(data)
          if (!valid) {
            throw new UserError(
              `JSON Schema validation failed before serialization: ${ajv.errorsText(validate.errors)}`,
              { type, data, headers, validationErrors: validate.errors }
            )
          }
        }

        encodedMessage = Buffer.from(JSON.stringify(data))
        break
    }

    const header = Buffer.alloc(5)
    header.writeInt32BE(schemaId, 1)

    return Buffer.concat([header, encodedMessage])
  }

  #schemaDeserializer (
    type: BeforeHookPayloadType,
    fallbackDeserializer: Deserializer<unknown> | Deserializer<string>,
    data?: Buffer,
    headers?: Map<string, string>,
    message?: MessageToConsume
  ): unknown {
    /* c8 ignore next 3 - Hard to test */
    if (typeof data === 'undefined' || data.length === 0) {
      return EMPTY_BUFFER
    }

    if (type === 'headerKey' || type === 'headerValue') {
      message = headers as unknown as MessageToConsume
    }

    const schemaId = this.getSchemaId(data as Buffer, type)

    if (!schemaId) {
      return fallbackDeserializer(data as any)
    }

    const schema = this.#schemas.get(schemaId)
    if (!schema) {
      throw new UserError(`Schema with ID ${schemaId} not found.`, { missingSchema: schemaId })
    }

    switch (schema.type) {
      case 'avro':
        return (schema.schema as Type).fromBuffer(data.subarray(5))
      case 'protobuf': {
        const typeName = this.#protobufTypeMapper(schemaId, type, message as MessageToConsume)
        const Type = (schema.schema as Root).lookupType(typeName)
        return Type.decode(data.subarray(5))
      }

      case 'json': {
        const parsed = JSON.parse(data.subarray(5).toString('utf-8'))
        const validate = schema.schema as ValidateFunction
        const valid = validate(parsed)

        if (!valid) {
          throw new UserError(
            `JSON Schema validation failed before deserialization: ${ajv.errorsText(validate.errors)}`,
            { type, data: parsed, headers, validationErrors: validate.errors }
          )
        }

        return parsed
      }
    }
  }

  #loadProtobuf () {
    try {
      return require('protobufjs').parse
      /* c8 ignore next 5 - In tests protobufjs is always available */
    } catch (e) {
      throw new UnsupportedFormatError(
        'Cannot load protobufjs module, which is an optionalDependency. Please check your local installation.'
      )
    }
  }
}
