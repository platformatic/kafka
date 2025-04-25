# @platformatic/kafka

A modern, high-performance, pure TypeScript/JavaScript type safe client for Apache Kafka.

## Features

- **High Performance**: Optimised for speed.
- **Pure Modern JavaScript**: Built with the latest ECMAScript features, no native addon needed.
- **Type Safety**: Full TypeScript support with strong typing.
- **Flexible API**: You can use promises or callbacks on all APIs.
- **Streaming or Event-based Consumers**: Thanks to Node.js streams, you can choose your preferred consuming method.
- **Flexible Serialisation**: Pluggable serialisers and deserialisers.
- **Connection Management**: Automatic connection pooling and recovery.
- **Low Dependencies**: Minimal external dependencies.

## Installation

```bash
npm install @platformatic/kafka
```

## Getting Started

### Producer

```typescript
import { Producer, stringSerializer } from '@platformatic/kafka'

// Create a producer with string serialisers
const producer = new Producer({
  clientId: 'my-producer',
  bootstrapBrokers: ['localhost:9092'],
  serializers: stringSerializer
})

// Send messages
await producer.send({
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
await producer.close()
```

### Consumer

```typescript
import { Consumer, stringDeserializer } from '@platformatic/kafka'
import { forEach } from 'hwp'

// Create a consumer with string deserialisers
const consumer = new Consumer({
  groupId: 'my-consumer-group',
  clientId: 'my-consumer',
  bootstrapBrokers: ['localhost:9092'],
  deserializers: stringDeserializer
})

// Create a consumer stream
const stream = await consumer.consume({
  autocommit: true,
  topics: ['my-topic'],
  sessionTimeout: 10000,
  heartbeatInterval: 500
})

// Option 1: Event-based consumption
stream.on('data', message => {
  console.log(`Received: ${message.key} -> ${message.value}`)
})

// Option 2: Async iterator consumption
for await (const message of stream) {
  console.log(`Received: ${message.key} -> ${message.value}`)
  // Process message...
}

// Option 3: Concurrent processing
await forEach(
  stream,
  async message => {
    console.log(`Received: ${message.key} -> ${message.value}`)
    // Process message...
  },
  16
) // 16 is the concurrency level

// Close the consumer when done
await consumer.close()
```

### Admin

```typescript
import { Admin } from '@platformatic/kafka'

// Create an admin client
const admin = new Admin({
  clientId: 'my-admin',
  bootstrapBrokers: ['localhost:9092']
})

// Create topics
await admin.createTopics({
  topics: ['my-topic'],
  partitions: 3,
  replicas: 1
})

// Create topics with custom assignments
await admin.createTopics({
  topics: ['my-custom-topic'],
  partitions: -1, // Use assignments instead
  replicas: -1, // Use assignments instead
  assignments: [
    { partition: 0, brokers: [1] },
    { partition: 1, brokers: [2] },
    { partition: 2, brokers: [3] }
  ]
})

// Get metadata
const metadata = await admin.metadata({ topics: ['my-topic'] })
console.log(metadata)

// Delete topics
await admin.deleteTopics({ topics: ['my-topic'] })

// Close the admin client when done
await admin.close()
```

## Serialisation/Deserialisation

`@platformatic/kafka` supports customisation of serialisation out of the box.

You can provide a different serialiser or deserialiser for each part of a message:

- Key
- Value
- Header Key
- Header Value

By default, it will use no-operation serialisers and deserialisers, which means that all the parts above must be `Buffer`s.

To provide a different serialiser, simply pass in the `serializers` option of the producer or the `deserializers` option of the consumer.
Both options accept an object with any of the `key`, `value`, `headerKey` and `headerValue` properties.

```typescript
import {
  Consumer,
  jsonDeserializer,
  jsonSerializer,
  ProduceAcks,
  Producer,
  stringDeserializer,
  stringSerializer
} from '@platformatic/kafka'

type Strings = string[]

const producer = new Producer({
  clientId: 'my-producer',
  bootstrapBrokers: ['localhost:9092'],
  serializers: {
    key: stringSerializer,
    value: jsonSerializer<Strings>
  }
})

const consumer = new Consumer({
  groupId: 'my-consumer-group',
  clientId: 'my-consumer',
  bootstrapBrokers: ['localhost:9092'],
  deserializers: {
    key: stringDeserializer,
    value: jsonDeserializer<Strings>
  },
  maxWaitTime: 1000,
  autocommit: 100
})

// Produce some messages
let i = 0
const timer = setTimeout(() => {
  producer.send({
    messages: [{ topic: 'temp', key: `key-${i++}`, value: ['first', 'second'] }],
    acks: ProduceAcks.LEADER
  })

  if (i < 3) {
    timer.refresh()
  }
}, 1000)

const stream = await consumer.consume({ topics: ['temp'] })

// Notice in your editor that the message below is properly typed as Message<string, Strings, ...>
for await (const message of stream) {
  console.log(message)

  if (message.key === 'key-2') {
    break
  }
}

await stream.close()
await consumer.close()
await producer.close()
```

### Error Handling

`@platformatic/kafka` defines its hierarchy of errors.
All errors inherit from `GenericError` and have a `code` property starting with `PLT_KFK`.

```typescript
try {
  await producer.send({
    messages: [{ topic: 'my-topic', value: 'test' }]
  })
} catch (error) {
  if (error.code === 'PLT_KFK_PRODUCER_ERROR') {
    // Handle producer-specific errors
  } else if (error.code === 'PLT_KFK_CONNECTION_ERROR') {
    // Handle connection errors
  } else {
    // Handle other errors
  }
}
```

## Performance

`@platformatic/kafka` is built with performance in mind, optimising for high throughput and low latency.

Internally, it does not use a single promise to minimise event loop overheads.

It also uses a higher watermark for consumer streams. This improves the throughput but also impacts the memory usage.
By default, it uses a value of `1024` (while Node.js default value is `16`). This means that potentially each stream can put more than a thousand objects in memory.
If each object is 1MB, this means 1GB of RAM per stream. Tune this accordingly.

This value can be changed using the `highWaterMark` option of the `Consumer`.

## API Reference

All the APIs support an optional Node.js style `callback` argument as the last argument.

If the callback is provided, then it will be invoked; otherwise, the method will behave as an `async function` and will resolve or reject when finished.

In all the documentation below, when discussing a function accepting an optional `callback` parameter, the "return value" is considered to be either the resolved value or the result passed to the callback.

Many of the methods accept the same options as the client's constructors. The constructor's options should be considered as defaults for the respective options in the various methods.

- [Producer API](./docs/producer.md)
- [Consumer API](./docs/consumer.md)
- [Admin API](./docs/admin.md)
- [Base Client](./docs/base.md)
- [Other APIs and Types](./docs/other.md)

## Requirements

- Node.js >= 22.14.0

## License

Apache-2.0 - See [LICENSE](LICENSE) for more information.
