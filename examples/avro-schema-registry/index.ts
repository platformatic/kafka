import avro, { type Type } from 'avsc'
import { randomUUID } from 'node:crypto'
import {
  Consumer,
  debugDump,
  MessagesStreamFallbackModes,
  MessagesStreamModes,
  noopSerializer,
  Producer,
  sleep,
  stringDeserializer,
  stringSerializer,
  UserError
} from '../../src/index.ts'

interface Datum {
  id: number
  name: string
  room?: number
}

const bootstrapBrokers = ['localhost:9092']
const registryUrl = 'http://localhost:9093'
const topic = `test-avro-schema-registry-${randomUUID()}`

const subjectName = 's' + Date.now().toString()
const originalSchema = {
  type: 'record',
  name: subjectName,
  fields: [
    { name: 'id', type: 'int' },
    { name: 'name', type: 'string' }
  ]
}

const updatedSchema = {
  type: 'record',
  name: subjectName,
  fields: [
    { name: 'id', type: 'int' },
    { name: 'name', type: 'string' },
    { name: 'room', type: 'int', default: 0 } // New field added with a default value
  ]
}

const originalData: Datum[] = [
  { id: 1, name: 'Alice' },
  { id: 2, name: 'Bob' },
  { id: 3, name: 'Charlie' }
]

const updatedData = [
  { id: 1, name: 'Alice', room: 101 },
  { id: 2, name: 'Bob', room: 102 },
  { id: 3, name: 'Charlie', room: 103 }
]

const localSchemas: Record<string, Type> = {}

function debug (label: string, ...args: any[]) {
  debugDump('|', label.padStart(15), '|', ...args)
}

async function registerSchema (schema: object) {
  const response = await fetch(`${registryUrl}/subjects/${encodeURIComponent(subjectName)}/versions`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/vnd.schemaregistry.v1+json' },
    body: JSON.stringify({ schema: JSON.stringify(schema) })
  })

  if (!response.ok) {
    throw new UserError(`Failed to register schema: [HTTP ${response.status}]`, { cause: await response.json() })
  }

  const schemaData = await response.json()
  debug('REGISTER SCHEMA', schemaData.id)
}

async function fetchSchema (schemaId: number): Promise<void> {
  debug('FETCH SCHEMA', schemaId)

  const response = await fetch(`${registryUrl}/schemas/ids/${schemaId}`)

  if (!response.ok) {
    throw new UserError(`Failed to fetch schema: [HTTP ${response.status}]`, { cause: await response.json() })
  }

  const schemaData = await response.json()

  debug('RECEIVED SCHEMA', schemaId)
  localSchemas[schemaId] = avro.Type.forSchema(JSON.parse(schemaData.schema))
}

function registryDeserializer (buffer?: Buffer): object | null {
  if (!buffer) {
    return null
  }

  const schemaId = buffer.readInt32BE(1)

  if (!localSchemas[schemaId]) {
    throw new UserError(`Schema with ID ${schemaId} not found.`, { missingSchema: schemaId })
  }

  // avsc will add the type name as the object name. To avoid this, use structuredClone
  return localSchemas[schemaId].fromBuffer(buffer.subarray(5))
}

async function produceMessages (data: Datum[]) {
  const producer = new Producer({
    clientId: 'id',
    bootstrapBrokers,
    strict: true,
    serializers: {
      key: stringSerializer,
      value: noopSerializer,
      headerKey: stringSerializer,
      headerValue: stringSerializer
    },
    autocreateTopics: true
  })

  try {
    const schemaResponse = await fetch(`${registryUrl}/subjects/${encodeURIComponent(subjectName)}/versions/latest`)

    if (!schemaResponse.ok) {
      throw new UserError(`Failed to fetch schema: [HTTP ${schemaResponse.status}]`, {
        cause: await schemaResponse.json()
      })
    }

    const schemaData = await schemaResponse.json()
    const schema = avro.Type.forSchema(JSON.parse(schemaData.schema))
    const header = Buffer.alloc(5)
    header.writeInt32BE(schemaData.id, 1)

    for (const datum of data) {
      const encodedMessage = schema.toBuffer(datum)

      await producer.send({
        messages: [
          {
            topic,
            key: datum.id.toString(),
            value: Buffer.concat([header, encodedMessage]),
            headers: {
              'content-type': 'application/vnd.confluent.avro'
            }
          }
        ]
      })
    }
  } finally {
    await producer.close()
  }
}

async function consumeMessages (abortSignal: AbortSignal) {
  const consumer = new Consumer({
    groupId: randomUUID(),
    clientId: 'id',
    bootstrapBrokers: ['localhost:9092'],
    strict: true,
    deserializers: {
      key: stringDeserializer,
      value: registryDeserializer,
      headerKey: stringDeserializer,
      headerValue: stringDeserializer
    }
  })

  abortSignal.onabort = function () {
    consumer.close(true)
  }

  while (true) {
    if (abortSignal.aborted) {
      break
    }

    const stream = await consumer.consume({
      topics: [topic],
      maxWaitTime: 500,
      mode: MessagesStreamModes.COMMITTED,
      fallbackMode: MessagesStreamFallbackModes.EARLIEST
    })

    try {
      for await (const message of stream) {
        debug('DATA', `key=${message.key}`, message.value)
      }
    } catch (error) {
      if (typeof error.cause.missingSchema !== 'number') {
        throw error
      }

      await fetchSchema(error.cause.missingSchema)
    }
  }
}

async function main () {
  const abortController = new AbortController()
  // Start the consumer
  consumeMessages(abortController.signal).catch(error => {
    console.error('Error in consumeMessages:', error)
    process.exit(1)
  })

  // Register the schema and produce messages
  await registerSchema(originalSchema)
  await produceMessages(originalData)

  // Update the schema and produce messages again
  await registerSchema(updatedSchema)
  await produceMessages(updatedData)

  await sleep(3000) // Wait for a while to allow messages to be consumed
  abortController.abort() // Stop the consumer
}

await main()
