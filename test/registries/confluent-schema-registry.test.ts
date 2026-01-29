import { deepStrictEqual, strictEqual } from 'node:assert'
import { randomUUID } from 'node:crypto'
import { readFile } from 'node:fs/promises'
import { resolve } from 'node:path'
import test from 'node:test'
import { UserError } from '../../src/errors.ts'
import { MessagesStreamModes, noopDeserializer, stringDeserializer, stringSerializer } from '../../src/index.ts'
import { ConfluentSchemaRegistry } from '../../src/registries/confluent-schema-registry.ts'
import {
  confluentSchemaRegistryAuthBasicUrl,
  confluentSchemaRegistryBearerUrl,
  confluentSchemaRegistryUrl,
  createConsumer,
  createProducer,
  createTopic
} from '../helpers.ts'

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
    await readFile(resolve(import.meta.dirname, '../fixtures/confluent-schema-registry.proto'), 'utf-8')
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
    strictEqual(
      error.cause.message,
      'JSON Schema validation failed before deserialization: data must NOT have additional properties'
    )
  }
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
