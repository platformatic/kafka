import { randomUUID } from 'crypto'
import { once } from 'node:events'
import { ConfluentSchemaRegistry, Consumer, debugDump, Producer, UserError } from '../../src/index.ts'
import { kafkaSingleBootstrapServers } from '../../test/helpers.ts'

interface Datum {
  id: number
  name: string
}

const registryUrl = 'http://localhost:8006'
const producerRegistry = new ConfluentSchemaRegistry<string, Datum, string, string>({
  url: registryUrl,
  auth: { token: 'token' }
})
const consumerRegistry = new ConfluentSchemaRegistry<string, Datum, string, string>({
  url: registryUrl,
  auth: { token: 'token' }
})

async function prepareRegistry () {
  const subjectName = 's' + Date.now().toString()
  const schema = {
    type: 'record',
    name: subjectName,
    fields: [
      { name: 'id', type: 'int' },
      { name: 'name', type: 'string' }
    ]
  }

  const response = await fetch(`${registryUrl}/subjects/${encodeURIComponent(subjectName)}/versions`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/vnd.schemaregistry.v1+json',
      Authorization: 'Bearer TOKEN'
    },
    body: JSON.stringify({ schemaType: 'AVRO', schema: JSON.stringify(schema) })
  })

  if (!response.ok) {
    throw new UserError(`Failed to register schema: [HTTP ${response.status}]`, {
      cause: (await response.json()) as Error
    })
  }

  const schemaData = (await response.json()) as { id: number }
  return schemaData.id
}

async function consume () {
  const consumer = new Consumer({
    groupId: randomUUID(),
    clientId: 'id',
    bootstrapBrokers: kafkaSingleBootstrapServers,
    strict: true,
    registry: consumerRegistry
  })

  const stream = await consumer.consume({
    autocommit: false,
    topics: ['temp1'],
    sessionTimeout: 10000,
    rebalanceTimeout: 10000,
    heartbeatInterval: 500,
    maxWaitTime: 500
  })

  once(process, 'SIGINT').then(() => consumer.close(true))

  let i = 0
  stream.on('data', message => {
    console.log('data', message.partition, message.offset, message.key, message.value, message.headers)

    if (++i === 6) {
      consumer.close(true)
    }
  })

  debugDump('consumer started')
}

async function produce (schemaId: number) {
  const producer = new Producer({
    clientId: 'id',
    bootstrapBrokers: kafkaSingleBootstrapServers,
    autocreateTopics: true,
    registry: producerRegistry,
    strict: true
  })

  for (let i = 0; i < 3; i++) {
    await producer.send({
      messages: [
        {
          topic: 'temp1',
          key: `key-${i}-1`,
          value: { id: 1, name: 'Alice' },
          headers: new Map([[`header-key-${i}`, `header-value-${i}`]]),
          partition: i % 2,
          metadata: {
            schemas: {
              value: schemaId
            }
          }
        },
        {
          topic: 'temp1',
          key: `key-${i}-2`,
          value: { id: 2, name: 'Bob' },
          headers: new Map([[`header-key-${i}`, `header-value-${i}`]]),
          partition: i % 2,
          metadata: {
            schemas: {
              value: schemaId
            }
          }
        }
      ]
    })
  }

  await producer.close()
}

const schemaId = await prepareRegistry()
await consume()
await produce(schemaId)
