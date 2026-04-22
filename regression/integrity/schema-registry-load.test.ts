import { deepStrictEqual } from 'node:assert'
import { randomUUID } from 'node:crypto'
import { test } from 'node:test'
import { MessagesStreamModes } from '../../src/index.ts'
import { ConfluentSchemaRegistry } from '../../src/registries/confluent-schema-registry.ts'
import {
  createConsumer,
  createProducer,
  createTopic,
  regressionSingleBootstrapServers
} from '../helpers/index.ts'

const schemaRegistryUrl = 'http://localhost:8004'

interface SchemaDatum {
  id: number
  name: string
}

async function registerSchema (subject: string): Promise<number> {
  // Register a unique schema per run so repeated local executions do not depend on
  // existing registry state or compatibility from previous tests.
  const response = await fetch(`${schemaRegistryUrl}/subjects/${encodeURIComponent(subject)}/versions`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/vnd.schemaregistry.v1+json' },
    body: JSON.stringify({
      schemaType: 'AVRO',
      schema: JSON.stringify({
        type: 'record',
        name: subject,
        fields: [
          { name: 'id', type: 'int' },
          { name: 'name', type: 'string' }
        ]
      })
    })
  })

  if (!response.ok) {
    throw new Error(`failed to register schema: ${response.status}`)
  }

  const body = (await response.json()) as { id: number }
  return body.id
}

test('regression: schema-registry deserialization remains stable under sustained load', { timeout: 90_000 }, async t => {
  // The schema registry service is attached to the single-broker lane, so this test
  // intentionally uses that bootstrap set instead of the 3-broker regression lane.
  const total = 50
  const topic = await createTopic(t, 1, regressionSingleBootstrapServers)
  const subject = `Regression${randomUUID().replaceAll('-', '')}`
  const schemaId = await registerSchema(subject)
  const producerRegistry = new ConfluentSchemaRegistry<string, SchemaDatum, string, string>({ url: schemaRegistryUrl })
  const consumerRegistry = new ConfluentSchemaRegistry<string, SchemaDatum, string, string>({ url: schemaRegistryUrl })
  const producer = createProducer<string, SchemaDatum, string, string>(t, {
    bootstrapBrokers: regressionSingleBootstrapServers,
    registry: producerRegistry
  })

  await producer.send({
    messages: Array.from({ length: total }, (_, id) => ({
      topic,
      key: String(id),
      value: { id, name: `datum-${id}` },
      metadata: { schemas: { value: schemaId } }
    }))
  })

  const consumer = createConsumer<string, SchemaDatum, string, string>(t, {
    bootstrapBrokers: regressionSingleBootstrapServers,
    registry: consumerRegistry
  })
  const stream = await consumer.consume({ topics: [topic], maxFetches: 1, mode: MessagesStreamModes.EARLIEST })
  const values: SchemaDatum[] = []

  // Consume through the registry-backed deserializer and compare decoded objects,
  // not only message counts, to catch schema lookup/cache regressions.
  for await (const message of stream) {
    values.push(structuredClone(message.value))
  }

  deepStrictEqual(
    values,
    Array.from({ length: total }, (_, id) => ({ id, name: `datum-${id}` }))
  )
})
