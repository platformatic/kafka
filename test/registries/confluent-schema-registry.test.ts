import { deepStrictEqual, match, ok, strictEqual, throws } from 'node:assert'
import { randomUUID } from 'node:crypto'
import { once } from 'node:events'
import { readFileSync } from 'node:fs'
import { createServer, type IncomingHttpHeaders, type IncomingMessage, type ServerResponse } from 'node:http'
import { createServer as createHttpsServer, type ServerOptions as HttpsServerOptions } from 'node:https'
import { type AddressInfo } from 'node:net'
import test, { type TestContext } from 'node:test'
import { MultipleErrors, NetworkError, TimeoutError, UserError } from '../../src/errors.ts'
import {
  type DeserializationErrorContext,
  DeserializationErrorActions,
  MessagesStreamModes,
  noopDeserializer,
  SchemaValidationError,
  stringDeserializer,
  stringSerializer
} from '../../src/index.ts'
import { ConfluentSchemaRegistry, createUndiciAgent } from '../../src/registries/confluent-schema-registry.ts'
import {
  confluentSchemaRegistryAuthBasicUrl,
  confluentSchemaRegistryBearerUrl,
  confluentSchemaRegistryUrl,
  createConsumer,
  createProducer,
  createTopic
} from '../helpers.ts'

const originalEmitWarning = process.emitWarning

test.before(() => {
  process.emitWarning = ((..._args: Parameters<typeof process.emitWarning>) => {}) as typeof process.emitWarning
})

test.after(() => {
  process.emitWarning = originalEmitWarning
})

interface Datum {
  id: number
  name: string
}

function createSubject (): string {
  return `S${randomUUID().replaceAll('-', '')}`
}

async function registerSchema (url: string, subject: string, schemaType: string, schema: string) {
  const response = await fetch(`${url}/subjects/${encodeURIComponent(subject)}/versions`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/vnd.schemaregistry.v1+json' },
    body: JSON.stringify({ schemaType, schema })
  })

  if (!response.ok) {
    throw new UserError(`Failed to register schema: [HTTP ${response.status}]`, {
      cause: (await response.json()) as Error
    })
  }

  const schemaData = (await response.json()) as { id: number }
  return schemaData.id
}

test('skips null payloads before deserialization', async () => {
  const registry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url: confluentSchemaRegistryUrl
  })
  let fetches = 0

  registry.fetchSchema = async (_, callback) => {
    fetches++
    callback(null)
  }

  const hook = registry.getBeforeDeserializationHook()
  const message = {
    length: 0,
    attributes: 0,
    timestampDelta: 0n,
    offsetDelta: 0,
    key: null,
    value: Buffer.from('value'),
    headers: [],
    topic: 'topic',
    partition: 0
  }

  strictEqual(registry.getSchemaId(null, 'key'), undefined)

  await new Promise<void>((resolve, reject) => {
    hook(null, 'key', message, error => {
      if (error) {
        reject(error)
        return
      }

      resolve()
    })
  })

  strictEqual(fetches, 0)
})

test('supports producing and consuming messages using Confluent Schema Registry and AVRO', async t => {
  const topic = await createTopic(t, true)

  const producerRegistry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url: confluentSchemaRegistryUrl
  })
  const consumerRegistry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url: confluentSchemaRegistryUrl
  })

  // Register a schema
  const subject = createSubject()
  const schemaId = await registerSchema(
    confluentSchemaRegistryUrl,
    subject,
    'AVRO',
    JSON.stringify({
      type: 'record',
      name: subject,
      fields: [
        { name: 'id', type: 'int' },
        { name: 'name', type: 'string' }
      ]
    })
  )

  const producer = await createProducer(t, { registry: producerRegistry })
  await producer.send({
    messages: [
      {
        topic,
        key: 'key-1',
        value: { id: 1, name: 'Alice' },
        headers: { header1: 'value1' },
        metadata: { schemas: { value: schemaId } }
      },
      { topic, key: 'key-2', value: { id: 2, name: 'Bob' }, metadata: { schemas: { value: schemaId } } }
    ]
  })

  // Create a raw consumer to verify that messages are correctly encoded

  {
    const consumer = createConsumer(t, {
      deserializers: {
        key: stringDeserializer,
        value: noopDeserializer
      }
    })
    const stream = await consumer.consume({ topics: [topic], maxFetches: 1, mode: MessagesStreamModes.EARLIEST })
    const messages = []
    for await (const message of stream) {
      messages.push(message)
    }

    deepStrictEqual(messages[0].key, 'key-1')
    deepStrictEqual(messages[1].key, 'key-2')
    deepStrictEqual(messages[0].value.subarray(0, 5), Buffer.from([0, 0, 0, 0, schemaId]))
    deepStrictEqual(messages[1].value.subarray(0, 5), Buffer.from([0, 0, 0, 0, schemaId]))
  }

  // Consume using the consumer registry
  {
    const consumer = createConsumer(t, {
      registry: consumerRegistry
    })
    const stream = await consumer.consume({ topics: [topic], maxFetches: 1, mode: MessagesStreamModes.EARLIEST })
    const messages = []
    for await (const message of stream) {
      messages.push(message)
    }

    deepStrictEqual(messages[0].key, 'key-1')
    deepStrictEqual(messages[1].key, 'key-2')
    deepStrictEqual(structuredClone(messages[0].value), { id: 1, name: 'Alice' })
    deepStrictEqual(structuredClone(messages[1].value), { id: 2, name: 'Bob' })
  }
})

test('supports producing and consuming messages using Confluent Schema Registry and ProtocolBuffers', async t => {
  const topic = await createTopic(t, true)

  const producerRegistry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url: confluentSchemaRegistryUrl,
    protobufTypeMapper: () => 'Datum'
  })
  const consumerRegistry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url: confluentSchemaRegistryUrl,
    protobufTypeMapper: () => 'Datum'
  })

  // Register a schema
  const subject = createSubject()
  const schemaId = await registerSchema(
    confluentSchemaRegistryUrl,
    subject,
    'PROTOBUF',
    'syntax = "proto3"; message Datum { int32 id = 1;  string name = 2; }'
  )

  const producer = await createProducer(t, { registry: producerRegistry })
  await producer.send({
    messages: [
      { topic, key: 'key-1', value: { id: 1, name: 'Alice' }, metadata: { schemas: { value: schemaId } } },
      { topic, key: 'key-2', value: { id: 2, name: 'Bob' }, metadata: { schemas: { value: schemaId } } }
    ]
  })

  // Create a raw consumer to verify that messages are correctly encoded

  {
    const consumer = createConsumer(t, {
      deserializers: {
        key: stringDeserializer,
        value: noopDeserializer
      }
    })
    const stream = await consumer.consume({ topics: [topic], maxFetches: 1, mode: MessagesStreamModes.EARLIEST })
    const messages = []
    for await (const message of stream) {
      messages.push(message)
    }

    deepStrictEqual(messages[0].key, 'key-1')
    deepStrictEqual(messages[1].key, 'key-2')
    deepStrictEqual(messages[0].value.subarray(0, 5), Buffer.from([0, 0, 0, 0, schemaId]))
    deepStrictEqual(messages[1].value.subarray(0, 5), Buffer.from([0, 0, 0, 0, schemaId]))
  }

  // Consume using the consumer registry
  {
    const consumer = createConsumer(t, {
      registry: consumerRegistry
    })
    const stream = await consumer.consume({ topics: [topic], maxFetches: 1, mode: MessagesStreamModes.EARLIEST })
    const messages = []
    for await (const message of stream) {
      messages.push(message)
    }

    deepStrictEqual(messages[0].key, 'key-1')
    deepStrictEqual(messages[1].key, 'key-2')
    deepStrictEqual(structuredClone(messages[0].value), { id: 1, name: 'Alice' })
    deepStrictEqual(structuredClone(messages[1].value), { id: 2, name: 'Bob' })
  }
})

test('supports producing and consuming messages using Confluent Schema Registry and JSON Schema', async t => {
  const topic = await createTopic(t, true)

  const producerRegistry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url: confluentSchemaRegistryUrl
  })
  const consumerRegistry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url: confluentSchemaRegistryUrl
  })

  // Register a schema
  const subject = createSubject()
  const schemaId = await registerSchema(
    confluentSchemaRegistryUrl,
    subject,
    'JSON',
    JSON.stringify({
      type: 'object',
      properties: {
        id: { type: 'integer' },
        name: { type: 'string' }
      },
      required: ['id', 'name'],
      additionalProperties: false
    })
  )

  const producer = await createProducer(t, { registry: producerRegistry })
  await producer.send({
    messages: [
      { topic, key: 'key-1', value: { id: 1, name: 'Alice' }, metadata: { schemas: { value: schemaId } } },
      { topic, key: 'key-2', value: { id: 2, name: 'Bob' }, metadata: { schemas: { value: schemaId } } }
    ]
  })

  // Create a raw consumer to verify that messages are correctly encoded

  {
    const consumer = createConsumer(t, {
      deserializers: {
        key: stringDeserializer,
        value: noopDeserializer
      }
    })
    const stream = await consumer.consume({ topics: [topic], maxFetches: 1, mode: MessagesStreamModes.EARLIEST })
    const messages = []
    for await (const message of stream) {
      messages.push(message)
    }

    deepStrictEqual(messages[0].key, 'key-1')
    deepStrictEqual(messages[1].key, 'key-2')
    deepStrictEqual(messages[0].value.subarray(0, 5), Buffer.from([0, 0, 0, 0, schemaId]))
    deepStrictEqual(messages[1].value.subarray(0, 5), Buffer.from([0, 0, 0, 0, schemaId]))
  }

  // Consume using the consumer registry
  {
    const consumer = createConsumer(t, {
      registry: consumerRegistry
    })
    const stream = await consumer.consume({ topics: [topic], maxFetches: 1, mode: MessagesStreamModes.EARLIEST })
    const messages = []
    for await (const message of stream) {
      messages.push(message)
    }

    deepStrictEqual(messages[0].key, 'key-1')
    deepStrictEqual(messages[1].key, 'key-2')
    deepStrictEqual(structuredClone(messages[0].value), { id: 1, name: 'Alice' })
    deepStrictEqual(structuredClone(messages[1].value), { id: 2, name: 'Bob' })
  }
})

test('supports JSON Schema drafts 04, 06, 07, and 2020-12', async () => {
  const schemas = [
    {
      schema: {
        $schema: 'http://json-schema.org/draft-04/schema#',
        type: 'number',
        minimum: 2,
        exclusiveMinimum: true
      },
      valid: 3,
      invalid: 2
    },
    {
      schema: {
        $schema: 'http://json-schema.org/draft-06/schema#',
        type: 'number',
        exclusiveMinimum: 2
      },
      valid: 3,
      invalid: 2
    },
    {
      schema: {
        $schema: 'http://json-schema.org/draft-07/schema#',
        type: 'number',
        exclusiveMinimum: 2
      },
      valid: 3,
      invalid: 2
    },
    {
      schema: {
        $schema: 'https://json-schema.org/draft/2020-12/schema',
        type: 'array',
        prefixItems: [{ type: 'string' }],
        items: false,
        minItems: 1
      },
      valid: ['value'],
      invalid: ['value', 'extra']
    }
  ]
  const originalFetch = globalThis.fetch

  try {
    for (const { schema, valid, invalid } of schemas) {
      const registry = new ConfluentSchemaRegistry<string, Datum, string, string>({ url: confluentSchemaRegistryUrl })
      ;(globalThis as { fetch: typeof fetch }).fetch = async () => {
        return new Response(JSON.stringify({ schemaType: 'JSON', schema: JSON.stringify(schema) }))
      }

      await new Promise<void>((resolve, reject) => {
        registry.fetchSchema(1, error => {
          if (error) {
            reject(error)
            return
          }

          resolve()
        })
      })

      const validate = registry.get(1)?.schema as (data: unknown) => boolean
      strictEqual(validate(valid), true)
      strictEqual(validate(invalid), false)
    }
  } finally {
    ;(globalThis as { fetch: typeof fetch }).fetch = originalFetch
  }
})

test('deduplicates concurrent fetches for the same JSON schema ID', async () => {
  const subject = createSubject()
  const schemaId = await registerSchema(
    confluentSchemaRegistryUrl,
    subject,
    'JSON',
    JSON.stringify({
      $id: 'series.json',
      type: 'object',
      properties: {
        id: { type: 'integer' }
      },
      required: ['id']
    })
  )

  const registry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url: confluentSchemaRegistryUrl
  })

  function fetchSchema () {
    return new Promise<void>((resolve, reject) => {
      registry.fetchSchema(schemaId, err => {
        if (err) {
          reject(err)
          return
        }

        resolve()
      })
    })
  }

  await Promise.all([fetchSchema(), fetchSchema()])

  strictEqual(registry.get(schemaId)?.type, 'json')
})

test('uses strict AJV validation for JSON schemas by default', async () => {
  const subject = createSubject()
  const schemaId = await registerSchema(
    confluentSchemaRegistryUrl,
    subject,
    'JSON',
    JSON.stringify({
      type: 'object',
      properties: {
        id: { type: 'integer', 'x-extension': true }
      }
    })
  )

  const registry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url: confluentSchemaRegistryUrl
  })

  const error = await new Promise<Error | undefined>(resolve => {
    registry.fetchSchema(schemaId, err => {
      resolve(err as Error | undefined)
    })
  })

  strictEqual(error instanceof Error, true)
  match(error!.message, /unknown keyword: "x-extension"/)
})

test('supports disabling strict AJV validation for JSON schemas', async () => {
  const subject = createSubject()
  const schemaId = await registerSchema(
    confluentSchemaRegistryUrl,
    subject,
    'JSON',
    JSON.stringify({
      type: 'object',
      properties: {
        id: { type: 'integer', 'x-extension': true }
      },
      required: ['id']
    })
  )

  const registry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url: confluentSchemaRegistryUrl,
    jsonAjvOptions: { strict: false }
  })

  const error = await new Promise<Error | undefined>(resolve => {
    registry.fetchSchema(schemaId, err => {
      resolve(err as Error | undefined)
    })
  })

  strictEqual(error, undefined)

  const schema = registry.get(schemaId)
  strictEqual(schema?.type, 'json')

  const validate = schema!.schema as (data: unknown) => boolean
  strictEqual(validate({ id: 1 }), true)
  strictEqual(validate({}), false)
})

test('fails on JSON schema validation when producing', async t => {
  const topic = await createTopic(t, true)

  const producerRegistry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url: confluentSchemaRegistryUrl,
    jsonValidateSend: true
  })

  // Register a schema
  const subject = createSubject()
  const schemaId = await registerSchema(
    confluentSchemaRegistryUrl,
    subject,
    'JSON',
    JSON.stringify({
      type: 'object',
      properties: {
        id: { type: 'integer' },
        name: { type: 'string' }
      },
      required: ['id', 'name'],
      additionalProperties: false
    })
  )

  try {
    const producer = await createProducer(t, { registry: producerRegistry })
    await producer.send({
      messages: [
        {
          topic,
          key: 'key-1',
          value: { id: 1, name: 'Alice', foo: 'bar' } as Datum,
          metadata: { schemas: { value: schemaId } }
        }
      ]
    })

    throw new Error('Expected error was not thrown')
  } catch (error) {
    strictEqual(error instanceof UserError, true)
    strictEqual(error.message, 'Failed to serialize a message.')
    strictEqual(error.cause instanceof SchemaValidationError, true)
    strictEqual(error.cause.phase, 'serialization')
    strictEqual(
      error.cause.message,
      'JSON Schema validation failed before serialization: data must NOT have additional properties'
    )
  }
})

test('fails on JSON schema validation when consuming', async t => {
  const topic = await createTopic(t, true)

  const consumerRegistry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url: confluentSchemaRegistryUrl
  })

  // Register a schema
  const subject = createSubject()
  const schemaId = await registerSchema(
    confluentSchemaRegistryUrl,
    subject,
    'JSON',
    JSON.stringify({
      type: 'object',
      properties: {
        id: { type: 'integer' },
        name: { type: 'string' }
      },
      required: ['id', 'name'],
      additionalProperties: false
    })
  )

  const producer = await createProducer(t, {
    serializers: {
      key: stringSerializer,
      value (value: object | undefined) {
        return Buffer.concat([Buffer.from([0, 0, 0, 0, schemaId]), Buffer.from(JSON.stringify(value))])
      }
    }
  })

  await producer.send({
    messages: [
      { topic, key: 'key-1', value: { id: 1, name: 'Alice', foo: 'bar' }, metadata: { schemas: { value: schemaId } } }
    ]
  })

  try {
    const consumer = createConsumer(t, {
      registry: consumerRegistry
    })
    const stream = await consumer.consume({ topics: [topic], maxFetches: 1, mode: MessagesStreamModes.EARLIEST })
    const messages = []
    for await (const message of stream) {
      messages.push(message)
    }

    throw new Error('Expected error was not thrown')
  } catch (error) {
    strictEqual(error instanceof UserError, true)
    strictEqual(error.message, 'Failed to deserialize a message.')
    strictEqual(error.cause instanceof SchemaValidationError, true)
    strictEqual(error.cause.phase, 'deserialization')
    strictEqual(
      error.cause.message,
      'JSON Schema validation failed before deserialization: data must NOT have additional properties'
    )
  }
})

test('exposes malformed JSON and JSON schema validation errors to onDeserializationError', async t => {
  const topic = await createTopic(t, true)
  const consumerRegistry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url: confluentSchemaRegistryUrl
  })
  const subject = createSubject()
  const schemaId = await registerSchema(
    confluentSchemaRegistryUrl,
    subject,
    'JSON',
    JSON.stringify({
      type: 'object',
      properties: {
        id: { type: 'integer' },
        name: { type: 'string' }
      },
      required: ['id', 'name'],
      additionalProperties: false
    })
  )
  const schemaHeader = Buffer.alloc(5)
  schemaHeader.writeInt32BE(schemaId, 1)
  const producer = createProducer<string, string>(t, {
    serializers: {
      key: stringSerializer,
      value (value) {
        return Buffer.concat([schemaHeader, Buffer.from(value!)])
      }
    }
  })
  const firstTimestamp = BigInt(Date.now())
  const secondTimestamp = firstTimestamp + 1000n

  await producer.send({
    messages: [
      {
        topic,
        key: 'schema-invalid',
        value: JSON.stringify({ id: 1, name: 'Alice', foo: 'bar' }),
        timestamp: firstTimestamp
      },
      { topic, key: 'malformed', value: '{"id":', timestamp: secondTimestamp }
    ]
  })

  const failures: DeserializationErrorContext[] = []
  const consumer = createConsumer(t, { registry: consumerRegistry })
  const stream = await consumer.consume({
    topics: [topic],
    maxFetches: 1,
    mode: MessagesStreamModes.EARLIEST,
    onDeserializationError (context) {
      failures.push(context)
      return DeserializationErrorActions.SKIP
    }
  })

  const messages = await Array.fromAsync(stream)
  strictEqual(messages.length, 0)
  strictEqual(failures.length, 2)
  deepStrictEqual(
    failures.map(({ offset }) => offset),
    [0n, 1n]
  )
  deepStrictEqual(
    failures.map(({ payloadType }) => payloadType),
    ['value', 'value']
  )
  deepStrictEqual(
    failures.map(({ timestamp }) => timestamp),
    [firstTimestamp, secondTimestamp]
  )
  strictEqual(failures.every(failure => failure.topic === topic), true)
  strictEqual(failures.every(({ partition }) => partition === 0), true)
  strictEqual(failures.every(({ commit }) => typeof commit === 'function'), true)
  strictEqual(failures.every(({ record }) => Buffer.isBuffer(record.value)), true)
  strictEqual(failures.some(({ error }) => error instanceof SyntaxError), true)

  const validationError = failures.map(({ error }) => error).find(error => error instanceof SchemaValidationError)
  strictEqual(validationError instanceof SchemaValidationError, true)
  strictEqual(validationError?.schemaId, schemaId)
  strictEqual(validationError?.schemaType, 'json')
  strictEqual(validationError?.phase, 'deserialization')
  strictEqual(validationError?.payloadType, 'value')
  strictEqual(Array.isArray(validationError?.validationErrors), true)
})

test('fails on missing schema on the registry when producing', async t => {
  const topic = await createTopic(t, true)

  const producerRegistry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url: confluentSchemaRegistryUrl,
    jsonValidateSend: true
  })

  try {
    const producer = await createProducer(t, { registry: producerRegistry })
    await producer.send({
      messages: [
        {
          topic,
          key: 'key-1',
          value: { id: 1, name: 'Alice', foo: 'bar' } as Datum,
          metadata: { schemas: { value: 100 } }
        }
      ]
    })

    throw new Error('Expected error was not thrown')
  } catch (error) {
    strictEqual(error instanceof UserError, true)
    strictEqual(error.message, 'Failed to fetch a schema: [HTTP 404]')
  }
})

test('fails on missing schema on the registry when consuming', async t => {
  const topic = await createTopic(t, true)

  const consumerRegistry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url: confluentSchemaRegistryUrl
  })

  const producer = await createProducer(t, {
    serializers: {
      key: stringSerializer,
      value (value: object | undefined) {
        return Buffer.concat([Buffer.from([0, 0, 0, 0, 100]), Buffer.from(JSON.stringify(value))])
      }
    }
  })

  await producer.send({
    messages: [
      {
        topic,
        key: 'key-1',
        value: { id: 1, name: 'Alice', foo: 'bar' } as Datum,
        metadata: { schemas: { value: 100 } }
      }
    ]
  })

  try {
    const consumer = createConsumer(t, {
      registry: consumerRegistry
    })
    const stream = await consumer.consume({ topics: [topic], maxFetches: 1, mode: MessagesStreamModes.EARLIEST })
    const messages = []
    for await (const message of stream) {
      messages.push(message)
    }

    throw new Error('Expected error was not thrown')
  } catch (error) {
    strictEqual(error instanceof UserError, true)
    strictEqual(error.message, 'Failed to fetch a schema: [HTTP 404]')
  }
})

test('fails on missing schema locally when producing', async t => {
  const topic = await createTopic(t, true)

  const producerRegistry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url: confluentSchemaRegistryUrl,
    jsonValidateSend: true
  })

  producerRegistry.fetchSchema = async (_, cb) => {
    cb(null)
  }

  try {
    const producer = await createProducer(t, { registry: producerRegistry })
    await producer.send({
      messages: [
        {
          topic,
          key: 'key-1',
          value: { id: 1, name: 'Alice', foo: 'bar' } as Datum,
          metadata: { schemas: { value: 100 } }
        }
      ]
    })

    throw new Error('Expected error was not thrown')
  } catch (error) {
    strictEqual(error instanceof UserError, true)
    strictEqual(error.message, 'Failed to serialize a message.')
    strictEqual(error.cause.message, 'Schema with ID 100 not found.')
  }
})

test('fails on missing schema locally when consuming', async t => {
  const topic = await createTopic(t, true)

  const consumerRegistry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url: confluentSchemaRegistryUrl
  })

  consumerRegistry.fetchSchema = async (_, cb) => {
    cb(null)
  }

  const producer = await createProducer(t, {
    serializers: {
      key: stringSerializer,
      value (value: object | undefined) {
        return Buffer.concat([Buffer.from([0, 0, 0, 0, 100]), Buffer.from(JSON.stringify(value))])
      }
    }
  })

  await producer.send({
    messages: [
      {
        topic,
        key: 'key-1',
        value: { id: 1, name: 'Alice', foo: 'bar' } as Datum,
        metadata: { schemas: { value: 100 } }
      }
    ]
  })

  try {
    const consumer = createConsumer(t, {
      registry: consumerRegistry
    })
    const stream = await consumer.consume({ topics: [topic], maxFetches: 1, mode: MessagesStreamModes.EARLIEST })
    const messages = []
    for await (const message of stream) {
      messages.push(message)
    }

    throw new Error('Expected error was not thrown')
  } catch (error) {
    strictEqual(error instanceof UserError, true)
    strictEqual(error.message, 'Failed to deserialize a message.')
    strictEqual(error.cause.message, 'Schema with ID 100 not found.')
  }
})

test('supports Auth-Basic authentication', async t => {
  const topic = await createTopic(t, true)

  const unAuthenticatedRegistry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url: confluentSchemaRegistryAuthBasicUrl
  })
  const authenticatedRegistry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url: confluentSchemaRegistryAuthBasicUrl,
    auth: { username: 'user', password: 'password' }
  })

  // Register a schema
  const subject = createSubject()
  const schemaId = await registerSchema(
    confluentSchemaRegistryUrl,
    subject,
    'AVRO',
    JSON.stringify({
      type: 'record',
      name: subject,
      fields: [
        { name: 'id', type: 'int' },
        { name: 'name', type: 'string' }
      ]
    })
  )

  try {
    const producer = await createProducer(t, { registry: unAuthenticatedRegistry })
    await producer.send({
      messages: [
        {
          topic,
          key: 'key-1',
          value: { id: 1, name: 'Alice' },
          headers: { header1: 'value1' },
          metadata: { schemas: { value: schemaId } }
        },
        { topic, key: 'key-2', value: { id: 2, name: 'Bob' }, metadata: { schemas: { value: schemaId } } }
      ]
    })
  } catch (error) {
    strictEqual(error instanceof UserError, true)
    strictEqual(error.message, 'Failed to fetch a schema: [HTTP 401]')
  }

  const producer = await createProducer(t, { registry: authenticatedRegistry })
  const res = await producer.send({
    messages: [
      {
        topic,
        key: 'key-1',
        value: { id: 1, name: 'Alice' },
        headers: { header1: 'value1' },
        metadata: { schemas: { value: schemaId } }
      },
      { topic, key: 'key-2', value: { id: 2, name: 'Bob' }, metadata: { schemas: { value: schemaId } } }
    ]
  })

  strictEqual(res.offsets![0].topic, topic)
})

test('treats /schemas/ids/{id} responses without schemaType as AVRO', async t => {
  const topic = await createTopic(t, true)

  const subject = createSubject()
  const schemaId = await registerSchema(
    confluentSchemaRegistryUrl,
    subject,
    'AVRO',
    JSON.stringify({
      type: 'record',
      name: subject,
      fields: [
        { name: 'id', type: 'int' },
        { name: 'name', type: 'string' }
      ]
    })
  )

  const originalFetch = fetch
  const patchedFetch: typeof fetch = async (input, init) => {
    const response = await originalFetch(input, init)
    const url = typeof input === 'string' ? input : (input as URL | Request).toString()

    if (!response.ok || !url.endsWith(`/schemas/ids/${schemaId}`)) {
      return response
    }

    const body = (await response.json()) as { schema: string; schemaType?: string }
    delete body.schemaType

    return new Response(JSON.stringify(body), {
      status: response.status,
      statusText: response.statusText,
      headers: response.headers
    })
  }

  ;(globalThis as { fetch: typeof fetch }).fetch = patchedFetch

  t.after(() => {
    ;(globalThis as { fetch: typeof fetch }).fetch = originalFetch
  })

  const registry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url: confluentSchemaRegistryUrl
  })

  const producer = await createProducer(t, { registry })
  await producer.send({
    messages: [{ topic, key: 'key-1', value: { id: 1, name: 'Alice' }, metadata: { schemas: { value: schemaId } } }]
  })

  const consumer = createConsumer(t, { registry })
  const stream = await consumer.consume({ topics: [topic], maxFetches: 1, mode: MessagesStreamModes.EARLIEST })
  const messages = []
  for await (const message of stream) {
    messages.push(message)
  }

  deepStrictEqual(messages[0].key, 'key-1')
  deepStrictEqual(structuredClone(messages[0].value), { id: 1, name: 'Alice' })
})

test('supports Bearer token authentication', async t => {
  const topic = await createTopic(t, true)

  const unAuthenticatedRegistry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url: confluentSchemaRegistryAuthBasicUrl
  })
  const authenticatedRegistry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url: confluentSchemaRegistryBearerUrl,
    auth: { token: 'TOKEN' }
  })

  // Register a schema
  const subject = createSubject()
  const schemaId = await registerSchema(
    confluentSchemaRegistryUrl,
    subject,
    'AVRO',
    JSON.stringify({
      type: 'record',
      name: subject,
      fields: [
        { name: 'id', type: 'int' },
        { name: 'name', type: 'string' }
      ]
    })
  )

  try {
    const producer = await createProducer(t, { registry: unAuthenticatedRegistry })
    await producer.send({
      messages: [
        {
          topic,
          key: 'key-1',
          value: { id: 1, name: 'Alice' },
          headers: { header1: 'value1' },
          metadata: { schemas: { value: schemaId } }
        },
        { topic, key: 'key-2', value: { id: 2, name: 'Bob' }, metadata: { schemas: { value: schemaId } } }
      ]
    })
  } catch (error) {
    strictEqual(error instanceof UserError, true)
    strictEqual(error.message, 'Failed to fetch a schema: [HTTP 401]')
  }

  const producer = await createProducer(t, { registry: authenticatedRegistry })
  const res = await producer.send({
    messages: [
      {
        topic,
        key: 'key-1',
        value: { id: 1, name: 'Alice' },
        headers: { header1: 'value1' },
        metadata: { schemas: { value: schemaId } }
      },
      { topic, key: 'key-2', value: { id: 2, name: 'Bob' }, metadata: { schemas: { value: schemaId } } }
    ]
  })

  strictEqual(res.offsets![0].topic, topic)
})

const avroStringSchema = JSON.stringify({ schemaType: 'AVRO', schema: JSON.stringify({ type: 'string' }) })

function answerWithAvroStringSchema (res: ServerResponse): void {
  res.writeHead(200, { 'Content-Type': 'application/json' })
  res.end(avroStringSchema)
}

async function createFakeRegistry (
  t: TestContext,
  handler: (res: ServerResponse, attempt: number) => void = answerWithAvroStringSchema
): Promise<{ url: string; requests: IncomingHttpHeaders[]; attempts: () => number }> {
  const requests: IncomingHttpHeaders[] = []
  let attempts = 0

  const server = createServer((req, res) => {
    requests.push(req.headers)
    handler(res, ++attempts)
  })

  server.listen(0, '127.0.0.1')
  await once(server, 'listening')

  t.after(() => {
    return new Promise<void>(resolve => {
      server.closeAllConnections()
      server.close(() => resolve())
    })
  })

  return { url: `http://127.0.0.1:${(server.address() as AddressInfo).port}`, requests, attempts: () => attempts }
}

function fetchSchema (registry: ConfluentSchemaRegistry<string, Datum, string, string>, id: number): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    registry.fetchSchema(id, error => {
      if (error) {
        reject(error)
        return
      }

      resolve()
    })
  })
}

test('sends additional custom headers on schema registry requests', async t => {
  const { url, requests } = await createFakeRegistry(t)

  const registry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url,
    headers: {
      'Confluent-Identity-Pool-Id': 'pool-123',
      'target-sr-cluster': 'lsrc-456'
    }
  })

  await fetchSchema(registry, 1)

  strictEqual(requests.length, 1)
  strictEqual(requests[0]['confluent-identity-pool-id'], 'pool-123')
  strictEqual(requests[0]['target-sr-cluster'], 'lsrc-456')
  strictEqual(requests[0].authorization, undefined)
})

test('supports a custom headers provider, invoked on each request', async t => {
  const { url, requests } = await createFakeRegistry(t)

  let invocations = 0
  const registry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url,
    async headers () {
      invocations++
      return { 'Confluent-Identity-Pool-Id': `pool-${invocations}` }
    }
  })

  await fetchSchema(registry, 1)
  await fetchSchema(registry, 2)

  strictEqual(invocations, 2)
  strictEqual(requests[0]['confluent-identity-pool-id'], 'pool-1')
  strictEqual(requests[1]['confluent-identity-pool-id'], 'pool-2')
})

test('merges custom headers with authentication headers', async t => {
  const { url, requests } = await createFakeRegistry(t)

  const registry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url,
    auth: { token: 'TOKEN' },
    headers: { 'Confluent-Identity-Pool-Id': 'pool-123' }
  })

  await fetchSchema(registry, 1)

  strictEqual(requests[0].authorization, 'Bearer TOKEN')
  strictEqual(requests[0]['confluent-identity-pool-id'], 'pool-123')
})

test('gives precedence to authentication options over custom headers', async t => {
  const { url, requests } = await createFakeRegistry(t)

  const registry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url,
    auth: { username: 'user', password: 'password' },
    headers: { Authorization: 'Bearer IGNORED' }
  })

  await fetchSchema(registry, 1)

  strictEqual(requests[0].authorization, `Basic ${Buffer.from('user:password').toString('base64')}`)
})

test('does not mutate the custom headers object', async t => {
  const { url, requests } = await createFakeRegistry(t)

  const headers = { 'Confluent-Identity-Pool-Id': 'pool-123' }
  const registry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url,
    auth: { token: 'TOKEN' },
    headers
  })

  await fetchSchema(registry, 1)

  strictEqual(requests[0].authorization, 'Bearer TOKEN')
  deepStrictEqual(headers, { 'Confluent-Identity-Pool-Id': 'pool-123' })
})

test('times out slow schema registry requests', async t => {
  const { url } = await createFakeRegistry(t, () => {
    // Never answer, so that the request can only end with a timeout
  })

  const registry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url,
    timeout: 200,
    retries: 0
  })

  const error = await fetchSchema(registry, 1).catch((error: Error) => error)

  strictEqual(error instanceof TimeoutError, true)
  strictEqual((error as Error).message, 'Fetching a schema timed out after 200 ms.')
})

test('supports disabling the schema registry request timeout', async t => {
  const { url } = await createFakeRegistry(t, res => {
    setTimeout(() => {
      res.writeHead(200, { 'Content-Type': 'application/json' })
      res.end(avroStringSchema)
    }, 300).unref()
  })

  const registry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url,
    timeout: 0
  })

  await fetchSchema(registry, 1)

  strictEqual(registry.get(1)?.type, 'avro')
})

test('retries schema registry requests which fail with a retriable status', async t => {
  const { url, attempts } = await createFakeRegistry(t, (res, attempt) => {
    if (attempt < 3) {
      res.writeHead(503)
      res.end('Service Unavailable')
      return
    }

    res.writeHead(200, { 'Content-Type': 'application/json' })
    res.end(avroStringSchema)
  })

  const registry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url,
    retries: 3,
    retryDelay: 10
  })

  await fetchSchema(registry, 1)

  strictEqual(attempts(), 3)
  strictEqual(registry.get(1)?.type, 'avro')
})

test('retries schema registry requests which fail with a network error', async t => {
  const { url, attempts } = await createFakeRegistry(t, (res, attempt) => {
    if (attempt < 2) {
      res.destroy()
      return
    }

    res.writeHead(200, { 'Content-Type': 'application/json' })
    res.end(avroStringSchema)
  })

  const registry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url,
    retries: 3,
    retryDelay: 10
  })

  await fetchSchema(registry, 1)

  strictEqual(attempts(), 2)
  strictEqual(registry.get(1)?.type, 'avro')
})

test('gives up after the configured amount of retries', async t => {
  const { url, attempts } = await createFakeRegistry(t, res => {
    res.writeHead(503)
    res.end('Service Unavailable')
  })

  const registry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url,
    retries: 2,
    retryDelay: 10
  })

  const error = (await fetchSchema(registry, 1).catch((error: Error) => error)) as MultipleErrors

  strictEqual(MultipleErrors.isMultipleErrors(error), true)
  strictEqual(error.message, 'Failed to fetch a schema after 3 attempts.')
  strictEqual(error.errors.length, 3)
  strictEqual(attempts(), 3)
  strictEqual(error.errors[0].message, 'Failed to fetch a schema: [HTTP 503]')
})

test('does not retry schema registry requests which fail with a non retriable status', async t => {
  const { url, attempts } = await createFakeRegistry(t, res => {
    res.writeHead(404)
    res.end('Not Found')
  })

  const registry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url,
    retries: 3,
    retryDelay: 10
  })

  const error = (await fetchSchema(registry, 1).catch((error: Error) => error)) as UserError

  strictEqual(error instanceof UserError, true)
  strictEqual(error.message, 'Failed to fetch a schema: [HTTP 404]')
  strictEqual(error.response, 'Not Found')
  strictEqual(attempts(), 1)
})

test('supports a retryDelay function', async t => {
  const { url, attempts } = await createFakeRegistry(t, (res, attempt) => {
    if (attempt < 3) {
      res.writeHead(429)
      res.end('Too Many Requests')
      return
    }

    res.writeHead(200, { 'Content-Type': 'application/json' })
    res.end(avroStringSchema)
  })

  const delays: [number, number, string][] = []
  const registry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url,
    retries: 3,
    retryDelay (attempt, retries, error) {
      delays.push([attempt, retries, error.message])
      return attempt * 5
    }
  })

  await fetchSchema(registry, 1)

  strictEqual(attempts(), 3)
  deepStrictEqual(delays, [
    [1, 3, 'Failed to fetch a schema: [HTTP 429]'],
    [2, 3, 'Failed to fetch a schema: [HTTP 429]']
  ])
})

test('reports network failures as NetworkError', async t => {
  const { url } = await createFakeRegistry(t, res => {
    res.destroy()
  })

  const registry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url,
    retries: 0
  })

  const error = (await fetchSchema(registry, 1).catch((error: Error) => error)) as NetworkError

  strictEqual(error instanceof NetworkError, true)
  strictEqual(error.message, 'Failed to fetch a schema.')
  ok(error.cause)
})

let tlsFolder = '../../test/fixtures/schema-registry-tls/'

/* c8 ignore next 3 - Only relevant when the tests are executed from the compiled sources */
if (import.meta.url.includes('dist')) {
  tlsFolder = '../' + tlsFolder
}

function readTlsFixture (name: string): Buffer {
  return readFileSync(new URL(tlsFolder + name, import.meta.url))
}

const tlsFixtures = {
  ca: readTlsFixture('ca.pem'),
  serverCert: readTlsFixture('server.pem'),
  serverKey: readTlsFixture('server-key.pem'),
  clientCert: readTlsFixture('client.pem'),
  clientKey: readTlsFixture('client-key.pem')
}

async function createTlsRegistry (
  t: TestContext,
  options: HttpsServerOptions = {}
): Promise<{ url: string; requests: IncomingMessage[]; serverNames: (string | undefined)[] }> {
  const requests: IncomingMessage[] = []
  const serverNames: (string | undefined)[] = []

  const server = createHttpsServer(
    {
      cert: tlsFixtures.serverCert,
      key: tlsFixtures.serverKey,
      SNICallback (serverName, callback) {
        serverNames.push(serverName)
        callback(null)
      },
      ...options
    },
    (req, res) => {
      requests.push(req)
      res.writeHead(200, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify({ schemaType: 'AVRO', schema: JSON.stringify({ type: 'string' }) }))
    }
  )

  server.listen(0, '127.0.0.1')
  await once(server, 'listening')

  t.after(() => {
    return new Promise<void>(resolve => {
      server.closeAllConnections()
      server.close(() => resolve())
    })
  })

  return { url: `https://127.0.0.1:${(server.address() as AddressInfo).port}`, requests, serverNames }
}

test('fetches schemas over TLS using a custom CA', async t => {
  const { url, requests } = await createTlsRegistry(t)

  const registry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url,
    tls: { ca: tlsFixtures.ca, servername: 'localhost' }
  })

  await fetchSchema(registry, 7)

  strictEqual(registry.get(7)?.type, 'avro')
  strictEqual(requests[0].url, '/schemas/ids/7')
})

test('supports ssl as an alias for tls', async t => {
  const { url } = await createTlsRegistry(t)

  const registry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url,
    ssl: { ca: tlsFixtures.ca, servername: 'localhost' }
  })

  await fetchSchema(registry, 1)

  strictEqual(registry.get(1)?.type, 'avro')
})

test('rejects Schema Registry certificates which are not trusted', async t => {
  const { url } = await createTlsRegistry(t)

  const registry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url,
    tls: { servername: 'localhost' },
    retries: 0
  })

  const error = (await fetchSchema(registry, 1).catch((error: Error) => error)) as NetworkError

  // fetch reports transport failures as a TypeError carrying the original error as its cause
  strictEqual(error instanceof NetworkError, true)
  strictEqual((error.cause as Error).message, 'fetch failed')
  strictEqual(((error.cause as Error).cause as NodeJS.ErrnoException).code, 'UNABLE_TO_VERIFY_LEAF_SIGNATURE')
})

test('supports disabling Schema Registry certificate validation', async t => {
  const { url } = await createTlsRegistry(t)

  const registry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url,
    tls: { rejectUnauthorized: false }
  })

  await fetchSchema(registry, 1)

  strictEqual(registry.get(1)?.type, 'avro')
})

test('forwards the configured servername to the Schema Registry', async t => {
  const { url, serverNames } = await createTlsRegistry(t)

  const registry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url,
    tls: { ca: tlsFixtures.ca, servername: 'localhost' }
  })

  await fetchSchema(registry, 1)

  deepStrictEqual(serverNames, ['localhost'])
})

test('supports mutual TLS against the Schema Registry', async t => {
  const { url } = await createTlsRegistry(t, {
    ca: tlsFixtures.ca,
    requestCert: true,
    rejectUnauthorized: true
  })

  const registry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url,
    tls: {
      ca: tlsFixtures.ca,
      cert: tlsFixtures.clientCert,
      key: tlsFixtures.clientKey,
      servername: 'localhost'
    }
  })

  await fetchSchema(registry, 1)

  strictEqual(registry.get(1)?.type, 'avro')
})

test('fails mutual TLS when no client certificate is provided', async t => {
  const { url } = await createTlsRegistry(t, {
    ca: tlsFixtures.ca,
    requestCert: true,
    rejectUnauthorized: true
  })

  const registry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url,
    tls: { ca: tlsFixtures.ca, servername: 'localhost' },
    retries: 0
  })

  const error = await fetchSchema(registry, 1).catch((error: Error) => error)

  ok(error instanceof NetworkError)
})

test('sends authentication headers over the TLS transport', async t => {
  const { url, requests } = await createTlsRegistry(t)

  const registry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url,
    auth: { token: 'TOKEN' },
    tls: { ca: tlsFixtures.ca, servername: 'localhost' }
  })

  await fetchSchema(registry, 1)

  strictEqual(requests[0].headers.authorization, 'Bearer TOKEN')
})

test('refuses TLS options on a non HTTPS Schema Registry URL', () => {
  throws(
    () => {
      // eslint-disable-next-line no-new
      new ConfluentSchemaRegistry<string, Datum, string, string>({
        url: confluentSchemaRegistryUrl,
        tls: { rejectUnauthorized: false }
      })
    },
    (error: UserError) => {
      strictEqual(error instanceof UserError, true)
      strictEqual(error.message, 'TLS options can only be used with a HTTPS Schema Registry URL.')
      strictEqual(error.url, confluentSchemaRegistryUrl)
      return true
    }
  )
})

test('resolves the undici Agent bundled with Node.js', () => {
  const agent = createUndiciAgent({ connect: { rejectUnauthorized: false } })

  // Guards the versioned undici.globalDispatcher.<n> symbol lookup across the supported Node versions
  strictEqual(agent.constructor.name, 'Agent')
  strictEqual(typeof (agent as { dispatch?: unknown }).dispatch, 'function')
})

test('reuses a single dispatcher across schema fetches', async t => {
  const { url } = await createTlsRegistry(t)

  const registry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url,
    tls: { ca: tlsFixtures.ca, servername: 'localhost' }
  })

  await fetchSchema(registry, 1)
  await fetchSchema(registry, 2)

  strictEqual(registry.get(1)?.type, 'avro')
  strictEqual(registry.get(2)?.type, 'avro')
})

test('exposes the schema IDs used while consuming on the message metadata', async t => {
  const topic = await createTopic(t, true)

  const registry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url: confluentSchemaRegistryUrl
  })

  const subject = createSubject()
  const schemaId = await registerSchema(
    confluentSchemaRegistryUrl,
    subject,
    'AVRO',
    JSON.stringify({
      type: 'record',
      name: subject,
      fields: [
        { name: 'id', type: 'int' },
        { name: 'name', type: 'string' }
      ]
    })
  )

  const producer = await createProducer(t, { registry })
  await producer.send({
    messages: [
      { topic, key: 'key-1', value: { id: 1, name: 'Alice' }, metadata: { schemas: { value: schemaId } } },
      { topic, key: 'key-2', value: { id: 2, name: 'Bob' }, metadata: { schemas: { value: schemaId } } }
    ]
  })

  const consumer = createConsumer(t, { registry })
  const stream = await consumer.consume({ topics: [topic], maxFetches: 1, mode: MessagesStreamModes.EARLIEST })
  const messages = []
  for await (const message of stream) {
    messages.push(message)
  }

  deepStrictEqual(messages[0].metadata.schemas, { value: schemaId })
  deepStrictEqual(messages[1].metadata.schemas, { value: schemaId })

  // The metadata added by the registry does not replace the one added by the consumer
  strictEqual((messages[0].metadata.consumer as { groupId: string }).groupId, consumer.groupId)

  // Each message gets its own metadata object
  ok(messages[0].metadata !== messages[1].metadata)
})

test('exposes the schema IDs when the registry deserializers are used without the hook', async t => {
  const topic = await createTopic(t, true)

  const registry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url: confluentSchemaRegistryUrl
  })

  const subject = createSubject()
  const schemaId = await registerSchema(
    confluentSchemaRegistryUrl,
    subject,
    'AVRO',
    JSON.stringify({
      type: 'record',
      name: subject,
      fields: [
        { name: 'id', type: 'int' },
        { name: 'name', type: 'string' }
      ]
    })
  )

  const producer = await createProducer(t, { registry })
  await producer.send({
    messages: [{ topic, key: 'key-1', value: { id: 1, name: 'Alice' }, metadata: { schemas: { value: schemaId } } }]
  })

  // Preload the schema, as there is no hook to fetch it before deserializing
  await new Promise<void>((resolve, reject) => {
    registry.fetchSchema(schemaId, error => (error ? reject(error) : resolve()))
  })

  const consumer = createConsumer(t, { deserializers: registry.getDeserializers() })
  const stream = await consumer.consume({ topics: [topic], maxFetches: 1, mode: MessagesStreamModes.EARLIEST })
  const messages = []
  for await (const message of stream) {
    messages.push(message)
  }

  deepStrictEqual(structuredClone(messages[0].value), { id: 1, name: 'Alice' })
  deepStrictEqual(messages[0].metadata.schemas, { value: schemaId })
})

test('exposes the original key and value byte lengths on the message metadata', async t => {
  const topic = await createTopic(t, true)

  const registry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url: confluentSchemaRegistryUrl
  })

  const subject = createSubject()
  const schemaId = await registerSchema(
    confluentSchemaRegistryUrl,
    subject,
    'AVRO',
    JSON.stringify({
      type: 'record',
      name: subject,
      fields: [
        { name: 'id', type: 'int' },
        { name: 'name', type: 'string' }
      ]
    })
  )

  const producer = await createProducer(t, { registry })
  await producer.send({
    messages: [
      { topic, key: 'key-1', value: { id: 1, name: 'Alice' }, metadata: { schemas: { value: schemaId } } },
      { topic, key: 'key-2', value: { id: 2, name: 'Bob' }, metadata: { schemas: { value: schemaId } } }
    ]
  })

  // Consume the raw payloads to know the exact on-wire sizes
  const rawConsumer = createConsumer(t, { deserializers: { key: noopDeserializer, value: noopDeserializer } })
  const rawStream = await rawConsumer.consume({ topics: [topic], maxFetches: 1, mode: MessagesStreamModes.EARLIEST })
  const rawMessages = []
  for await (const message of rawStream) {
    rawMessages.push(message)
  }

  const consumer = createConsumer(t, { registry })
  const stream = await consumer.consume({ topics: [topic], maxFetches: 1, mode: MessagesStreamModes.EARLIEST })
  const messages = []
  for await (const message of stream) {
    messages.push(message)
  }

  deepStrictEqual(structuredClone(messages[0].value), { id: 1, name: 'Alice' })
  deepStrictEqual(messages[0].metadata.lengths, {
    key: rawMessages[0].key.length,
    value: rawMessages[0].value.length
  })
  deepStrictEqual(messages[1].metadata.lengths, {
    key: rawMessages[1].key.length,
    value: rawMessages[1].value.length
  })

  // The decoded value is smaller than the payload it came from, which is the whole point
  ok((messages[0].metadata.lengths as { value: number }).value > 5)

  // The metadata added by the registry does not replace the one added by the consumer
  strictEqual((messages[0].metadata.consumer as { groupId: string }).groupId, consumer.groupId)

  // Each message gets its own metadata object
  ok(messages[0].metadata !== messages[1].metadata)
})

test('continues consuming with the raw payload when a message cannot be decoded', async t => {
  const topic = await createTopic(t, true)
  const consumerRegistry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url: confluentSchemaRegistryUrl
  })
  const subject = createSubject()
  const schemaId = await registerSchema(
    confluentSchemaRegistryUrl,
    subject,
    'JSON',
    JSON.stringify({
      type: 'object',
      properties: {
        id: { type: 'integer' },
        name: { type: 'string' }
      },
      required: ['id', 'name'],
      additionalProperties: false
    })
  )

  const schemaHeader = Buffer.alloc(5)
  schemaHeader.writeInt32BE(schemaId, 1)

  const producer = createProducer<string, string>(t, {
    serializers: {
      key: stringSerializer,
      value (value) {
        return Buffer.concat([schemaHeader, Buffer.from(value!)])
      }
    }
  })

  const invalidPayload = JSON.stringify({ id: 1, name: 'Alice', foo: 'bar' })

  await producer.send({
    messages: [
      { topic, key: 'schema-invalid', value: invalidPayload },
      { topic, key: 'valid', value: JSON.stringify({ id: 2, name: 'Bob' }) }
    ]
  })

  const failures: DeserializationErrorContext[] = []
  const consumer = createConsumer(t, { registry: consumerRegistry })
  const stream = await consumer.consume({
    topics: [topic],
    maxFetches: 1,
    mode: MessagesStreamModes.EARLIEST,
    onDeserializationError (context) {
      failures.push(context)
      return DeserializationErrorActions.CONTINUE
    }
  })

  const messages = await Array.fromAsync(stream)

  // Nothing is lost and the stream is not aborted
  strictEqual(messages.length, 2)
  strictEqual(failures.length, 1)

  // The undecodable message is delivered with its original bytes
  const degraded = messages[0]
  strictEqual(degraded.key, 'schema-invalid')
  deepStrictEqual(degraded.value, Buffer.concat([schemaHeader, Buffer.from(invalidPayload)]))

  const { error, payloadType } = degraded.metadata.deserializationError as {
    error: Error
    payloadType: string
  }
  strictEqual(payloadType, 'value')
  strictEqual(error instanceof SchemaValidationError, true)

  // The messages which could be decoded are unaffected
  deepStrictEqual(structuredClone(messages[1].value), { id: 2, name: 'Bob' })
  strictEqual(messages[1].metadata.deserializationError, undefined)
})

test('continues consuming with the raw payload when the Schema Registry is unreachable', async t => {
  const topic = await createTopic(t, true)

  const producerRegistry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url: confluentSchemaRegistryUrl
  })

  const subject = createSubject()
  const schemaId = await registerSchema(
    confluentSchemaRegistryUrl,
    subject,
    'AVRO',
    JSON.stringify({
      type: 'record',
      name: subject,
      fields: [
        { name: 'id', type: 'int' },
        { name: 'name', type: 'string' }
      ]
    })
  )

  const producer = await createProducer(t, { registry: producerRegistry })
  await producer.send({
    messages: [
      {
        topic,
        key: 'key-1',
        value: { id: 1, name: 'Alice' },
        headers: { header1: 'value1' },
        metadata: { schemas: { value: schemaId } }
      }
    ]
  })

  // A registry which cannot be reached, simulating an outage. Retries are disabled as the failure
  // is the point of the test and each attempt would only add its backoff to the runtime.
  const consumerRegistry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url: 'http://127.0.0.1:1',
    retries: 0
  })

  const failures: DeserializationErrorContext[] = []
  const consumer = createConsumer(t, { registry: consumerRegistry })
  const stream = await consumer.consume({
    topics: [topic],
    maxFetches: 1,
    mode: MessagesStreamModes.EARLIEST,
    onDeserializationError (context) {
      failures.push(context)
      return DeserializationErrorActions.CONTINUE
    }
  })

  const messages = await Array.fromAsync(stream)

  strictEqual(messages.length, 1)
  strictEqual(failures.length, 1)
  strictEqual(failures[0].payloadType, 'value')

  // The message carries the bytes as they were received, headers included
  const rawValue = messages[0].value as unknown as Buffer
  deepStrictEqual(rawValue.subarray(0, 5), Buffer.from([0, 0, 0, 0, schemaId]))
  deepStrictEqual(messages[0].key, Buffer.from('key-1'))
  deepStrictEqual(Array.from(messages[0].headers.entries()), [[Buffer.from('header1'), Buffer.from('"value1"')]])
  ok(messages[0].metadata.deserializationError)

  // The schema ID is still readable from the Confluent wire format header
  strictEqual(consumerRegistry.getSchemaId(rawValue, 'value'), schemaId)
})

test('still destroys the stream on Schema Registry failures without an error handler', async t => {
  const topic = await createTopic(t, true)

  const producerRegistry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url: confluentSchemaRegistryUrl
  })

  const subject = createSubject()
  const schemaId = await registerSchema(
    confluentSchemaRegistryUrl,
    subject,
    'AVRO',
    JSON.stringify({
      type: 'record',
      name: subject,
      fields: [
        { name: 'id', type: 'int' },
        { name: 'name', type: 'string' }
      ]
    })
  )

  const producer = await createProducer(t, { registry: producerRegistry })
  await producer.send({
    messages: [{ topic, key: 'key-1', value: { id: 1, name: 'Alice' }, metadata: { schemas: { value: schemaId } } }]
  })

  const consumerRegistry = new ConfluentSchemaRegistry<string, Datum, string, string>({
    url: 'http://127.0.0.1:1',
    retries: 0
  })

  const consumer = createConsumer(t, { registry: consumerRegistry })
  const stream = await consumer.consume({ topics: [topic], maxFetches: 1, mode: MessagesStreamModes.EARLIEST })

  const error = await Array.fromAsync(stream).catch((error: Error) => error)
  ok(error instanceof Error)
})
