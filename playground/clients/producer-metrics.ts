import * as client from 'prom-client'
import { Producer, stringSerializers } from '../../src/index.ts'

const registry = new client.Registry()

// Create a producer with string serialisers
const producer1 = new Producer({
  clientId: 'my-producer',
  bootstrapBrokers: ['localhost:9092'],
  serializers: stringSerializers,
  metrics: { client, registry, labels: { a: 1, b: 2 } },
  autocreateTopics: true
})

// Send messages
await producer1.send({
  messages: [
    {
      topic: 'events',
      key: 'user-123',
      value: JSON.stringify({ name: 'John', action: 'login' }),
      headers: { source: 'web-app' }
    }
  ]
})

// Close the producer when done
await producer1.close()

// Create another producer with string serialisers but different labels
const producer2 = new Producer({
  clientId: 'my-producer',
  bootstrapBrokers: ['localhost:9092'],
  serializers: stringSerializers,
  metrics: { client, registry, labels: { b: 3, c: 4 } },
  autocreateTopics: true
})

// Send messages
await producer2.send({
  messages: [
    {
      topic: 'events',
      key: 'user-123',
      value: JSON.stringify({ name: 'John', action: 'login' }),
      headers: { source: 'web-app' }
    }
  ]
})

// Close the producer when done
await producer2.close()

console.log(JSON.stringify(await registry.getMetricsAsJSON(), null, 2))
