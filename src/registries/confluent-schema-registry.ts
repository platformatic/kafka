import { Ajv, type AnySchema, type AnySchemaObject, type ErrorObject, type Options, type ValidateFunction } from 'ajv'
import { Ajv2020 } from 'ajv/dist/2020.js'
import avro, { type Type } from 'avsc'
import { createRequire } from 'node:module'
import { setTimeout as sleep } from 'node:timers/promises'
import { type ConnectionOptions as TLSConnectionOptions } from 'node:tls'
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
import {
  type CredentialProvider,
  EMPTY_BUFFER,
  type GenericError,
  MultipleErrors,
  NetworkError,
  TimeoutError,
  UnsupportedFormatError,
  UserError
} from '../index.ts'
import { type MessageToConsume, type MessageToProduce } from '../protocol/records.ts'
import { getCredential } from '../protocol/sasl/utils.ts'
import { AbstractSchemaRegistry } from './abstract.ts'

const require = createRequire(import.meta.url)
type JsonSchemaValidator = {
  compile: (schema: AnySchema) => ValidateFunction
  errorsText: Ajv['errorsText']
}

const AjvDraft04 = require('ajv-draft-04') as new (options: Options) => JsonSchemaValidator
const draft06MetaSchema = require('ajv/dist/refs/json-schema-draft-06.json') as AnySchemaObject

type ConfluentSchemaRegistryMessageToProduce = MessageToProduce<unknown, unknown, unknown, unknown>

/*
  Node bundles undici but does not export it, so the Agent class cannot be imported. It is taken from
  the constructor of the global dispatcher, which is the very dispatcher fetch() uses by default.
*/
export type UndiciDispatcher = NonNullable<RequestInit['dispatcher']>

export interface UndiciAgentOptions {
  connect: TLSConnectionOptions
}

export type UndiciAgentConstructor = new (options: UndiciAgentOptions) => UndiciDispatcher

const undiciGlobalDispatcherSymbol = /^undici\.globalDispatcher\.(\d+)$/

export function createUndiciAgent (options: UndiciAgentOptions): UndiciDispatcher {
  /*
    The undici globals are installed the first time undici is loaded, which creating a Request is
    enough to do and which performs no I/O.
  */
  new Request('http://localhost') // eslint-disable-line no-new

  /*
    The symbol is versioned and more than one version is registered at the same time, pointing at
    different objects: on Node 26 undici.globalDispatcher.1 is a backwards compatibility wrapper and
    only undici.globalDispatcher.2 is the Agent. The highest version available therefore wins.
  */
  const globals = globalThis as unknown as Record<symbol, { constructor: UndiciAgentConstructor } | undefined>
  let version = -1
  let Agent: UndiciAgentConstructor | undefined

  for (const symbol of Object.getOwnPropertySymbols(globalThis)) {
    const symbolVersion = symbol.description?.match(undiciGlobalDispatcherSymbol)?.[1]

    if (typeof symbolVersion === 'undefined' || Number(symbolVersion) <= version) {
      continue
    }

    version = Number(symbolVersion)
    Agent = globals[symbol]?.constructor
  }

  /* c8 ignore next 6 - Only reachable on a Node.js build which does not bundle undici */
  if (typeof Agent !== 'function') {
    throw new UserError(
      'Cannot access the undici Agent bundled with Node.js, which is required to configure TLS for the Schema Registry.'
    )
  }

  return new Agent(options)
}

export interface ConfluentSchemaRegistryMetadata {
  schemas?: Record<BeforeHookPayloadType, number>
}

export type ConfluentSchemaRegistryProtobufTypeMapper = (
  id: number,
  type: BeforeHookPayloadType,
  context: ConfluentSchemaRegistryMessageToProduce | MessageToConsume
) => string

export type ConfluentSchemaRegistryHeaders = Record<string, string>

export type ConfluentSchemaRegistryHeadersProvider = () =>
  | ConfluentSchemaRegistryHeaders
  | Promise<ConfluentSchemaRegistryHeaders>

export type ConfluentSchemaRegistryRetryDelayGetter = (attempt: number, retries: number, error: Error) => number

export interface ConfluentSchemaRegistryOptions {
  url: string
  auth?: {
    username?: string | CredentialProvider
    password?: string | CredentialProvider
    token?: string | CredentialProvider
  }
  headers?: ConfluentSchemaRegistryHeaders | ConfluentSchemaRegistryHeadersProvider
  timeout?: number
  retries?: number
  retryDelay?: number | ConfluentSchemaRegistryRetryDelayGetter
  tls?: TLSConnectionOptions
  ssl?: TLSConnectionOptions // Alias for tls
  protobufTypeMapper?: ConfluentSchemaRegistryProtobufTypeMapper
  jsonValidateSend?: boolean
  jsonAjvOptions?: Options
}

export const defaultConfluentSchemaRegistryOptions = {
  timeout: 5000,
  retries: 3,
  retryDelay: 1000
}

// Statuses which are worth retrying: the registry is overloaded, restarting or behind a proxy which is
export const retriableSchemaRegistryStatuses = [408, 425, 429, 500, 502, 503, 504]

export type SchemaValidationPhase = 'serialization' | 'deserialization'

export interface SchemaValidationErrorProperties {
  schemaId: number
  schemaType: 'json'
  phase: SchemaValidationPhase
  payloadType: BeforeHookPayloadType
  type: BeforeHookPayloadType
  data: unknown
  headers?: ReadonlyMap<unknown, unknown>
  validationErrors: ErrorObject[] | null | undefined
}

export class SchemaValidationError extends UserError {
  declare readonly schemaId: number
  declare readonly schemaType: 'json'
  declare readonly phase: SchemaValidationPhase
  declare readonly payloadType: BeforeHookPayloadType
  declare readonly type: BeforeHookPayloadType
  declare readonly data: unknown
  declare readonly headers?: ReadonlyMap<unknown, unknown>
  declare readonly validationErrors: ErrorObject[] | null | undefined

  constructor (message: string, properties: SchemaValidationErrorProperties) {
    super(message, properties)
    this.name = 'SchemaValidationError'
  }
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
  #jsonAjv: Ajv2020
  #jsonAjvDraft7: Ajv
  #jsonAjvDraft04: JsonSchemaValidator
  #pendingFetches: Map<number, Promise<void>>
  #auth: ConfluentSchemaRegistryOptions['auth'] | undefined
  #headers: ConfluentSchemaRegistryOptions['headers'] | undefined
  #timeout: number
  #retries: number
  #retryDelay: number | ConfluentSchemaRegistryRetryDelayGetter
  #tls: TLSConnectionOptions | undefined
  #tlsDispatcher: UndiciDispatcher | undefined

  constructor (options: ConfluentSchemaRegistryOptions) {
    super()
    this.#url = options.url
    this.#schemas = new Map()
    this.#protobufTypeMapper = options.protobufTypeMapper ?? defaultProtobufTypeMapper
    this.#jsonValidateSend = options.jsonValidateSend ?? false
    const jsonAjvOptions = { allErrors: true, coerceTypes: false, strict: true, ...options.jsonAjvOptions }
    this.#jsonAjv = new Ajv2020(jsonAjvOptions)
    this.#jsonAjvDraft7 = new Ajv(jsonAjvOptions)
    this.#jsonAjvDraft7.addMetaSchema(draft06MetaSchema)
    this.#jsonAjvDraft04 = new AjvDraft04(jsonAjvOptions)
    this.#auth = options.auth
    this.#headers = options.headers
    this.#timeout = options.timeout ?? defaultConfluentSchemaRegistryOptions.timeout
    this.#retries = options.retries ?? defaultConfluentSchemaRegistryOptions.retries
    this.#retryDelay = options.retryDelay ?? defaultConfluentSchemaRegistryOptions.retryDelay
    this.#tls = options.tls ?? options.ssl
    this.#pendingFetches = new Map()

    if (this.#tls && new URL(this.#url).protocol !== 'https:') {
      throw new UserError('TLS options can only be used with a HTTPS Schema Registry URL.', { url: this.#url })
    }
  }

  getSchemaId (
    message: Buffer | null | MessageToProduce<Key, Value, HeaderKey, HeaderValue>,
    type?: BeforeHookPayloadType
  ): number | undefined {
    if (message === null) {
      return undefined
    }

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
    let fetch = this.#pendingFetches.get(id)

    if (!fetch) {
      fetch = this.#fetchSchema(id).finally(() => {
        this.#pendingFetches.delete(id)
      })
      this.#pendingFetches.set(id, fetch)
    }

    try {
      await fetch
      process.nextTick(callback)
    } catch (err) {
      process.nextTick(() => callback(err as Error))
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
      payload: Buffer | null,
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

  async #requestHeaders (): Promise<ConfluentSchemaRegistryHeaders | undefined> {
    let headers: ConfluentSchemaRegistryHeaders | undefined

    if (this.#headers) {
      headers = { ...(typeof this.#headers === 'function' ? await this.#headers() : this.#headers) }
    }

    // Authentication headers are applied last so that auth options always win over custom headers
    if (this.#auth) {
      headers ??= {}

      if (this.#auth.token) {
        const token = await getCredential('token', this.#auth.token)

        headers.Authorization = `Bearer ${token}`
      } else {
        const username = await getCredential('username', this.#auth.username)
        const password = await getCredential('password', this.#auth.password)

        headers.Authorization = `Basic ${Buffer.from(`${username}:${password}`).toString('base64')}`
      }
    }

    return headers
  }

  async #requestSchema (id: number): Promise<{ schemaType?: string; schema: string }> {
    const requestInit: RequestInit = {}
    const headers = await this.#requestHeaders()

    if (headers) {
      requestInit.headers = headers
    }

    if (this.#tls) {
      // Created once per registry, so that connections are pooled across schema fetches
      this.#tlsDispatcher ??= createUndiciAgent({ connect: this.#tls })
      requestInit.dispatcher = this.#tlsDispatcher
    }

    if (this.#timeout > 0) {
      requestInit.signal = AbortSignal.timeout(this.#timeout)
    }

    let response: Response
    try {
      response = await fetch(`${this.#url}/schemas/ids/${id}`, requestInit)
    } catch (error) {
      if ((error as Error).name === 'TimeoutError') {
        throw new TimeoutError(`Fetching a schema timed out after ${this.#timeout} ms.`, {
          canRetry: true,
          cause: error
        })
      }

      throw new NetworkError('Failed to fetch a schema.', { cause: error })
    }

    if (!response.ok) {
      throw new UserError(`Failed to fetch a schema: [HTTP ${response.status}]`, {
        response: await response.text(),
        canRetry: retriableSchemaRegistryStatuses.includes(response.status)
      })
    }

    return (await response.json()) as { schemaType?: string; schema: string }
  }

  async #requestSchemaWithRetries (id: number): Promise<{ schemaType?: string; schema: string }> {
    const errors: Error[] = []

    for (let attempt = 0; ; attempt++) {
      try {
        return await this.#requestSchema(id)
      } catch (error) {
        errors.push(error as Error)

        if (attempt >= this.#retries || (error as GenericError).canRetry !== true) {
          if (errors.length === 1) {
            throw errors[0]
          }

          throw new MultipleErrors(`Failed to fetch a schema after ${errors.length} attempts.`, errors)
        }

        const delay =
          typeof this.#retryDelay === 'function'
            ? this.#retryDelay(attempt + 1, this.#retries, error as Error)
            : this.#retryDelay

        if (delay > 0) {
          await sleep(delay)
        }
      }
    }
  }

  async #fetchSchema (id: number) {
    const responseBody = await this.#requestSchemaWithRetries(id)
    const { schema } = responseBody
    const schemaType = responseBody.schemaType ?? 'AVRO'

    switch (schemaType) {
      case 'AVRO':
        this.#schemas.set(id, { id, type: 'avro', schema: avro.Type.forSchema(JSON.parse(schema)) })
        break
      case 'PROTOBUF':
        this.#protobufParse ??= this.#loadProtobuf()
        this.#schemas.set(id, { id, type: 'protobuf', schema: this.#protobufParse!(schema).root })
        break
      case 'JSON': {
        const jsonSchema = JSON.parse(schema) as AnySchema
        this.#schemas.set(id, { id, type: 'json', schema: this.#getJsonAjv(jsonSchema).compile(jsonSchema) })
        break
      }
    }
  }

  #getJsonAjv (schema: AnySchema): JsonSchemaValidator {
    if (typeof schema !== 'object' || schema === null || typeof schema.$schema !== 'string') {
      return this.#jsonAjv
    }

    if (schema.$schema.includes('draft-04')) {
      return this.#jsonAjvDraft04
    }

    if (schema.$schema.includes('draft-06') || schema.$schema.includes('draft-07')) {
      return this.#jsonAjvDraft7
    }

    return this.#jsonAjv
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
            throw new SchemaValidationError(
              `JSON Schema validation failed before serialization: ${this.#jsonAjv.errorsText(validate.errors)}`,
              {
                schemaId,
                schemaType: 'json',
                phase: 'serialization',
                payloadType: type,
                type,
                data,
                headers,
                validationErrors: validate.errors
              }
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
          throw new SchemaValidationError(
            `JSON Schema validation failed before deserialization: ${this.#jsonAjv.errorsText(validate.errors)}`,
            {
              schemaId,
              schemaType: 'json',
              phase: 'deserialization',
              payloadType: type,
              type,
              data: parsed,
              headers,
              validationErrors: validate.errors
            }
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
