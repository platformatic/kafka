# Migrating from KafkaJS to @platformatic/kafka

This guide covers every major API surface to help you migrate from [KafkaJS](https://kafka.js.org/) to `@platformatic/kafka`.

## Overview

`@platformatic/kafka` differs from KafkaJS in several fundamental ways:

| Aspect | KafkaJS | @platformatic/kafka |
|---|---|---|
| **Architecture** | Factory pattern (`new Kafka({}).producer()`) | Direct instantiation (`new Producer({})`) |
| **Connections** | Explicit `connect()` / `disconnect()` | Lazy auto-connect / `close()` |
| **Consumer** | Callback-based (`run({ eachMessage })`) | Stream-based (`consume()` returns `Readable`) |
| **Offsets** | Strings | `bigint` |
| **Timestamps** | Strings | `bigint` |
| **Headers** | `Record<string, Buffer>` | `Map<HeaderKey, HeaderValue>` |
| **Serialization** | Manual (always `Buffer`) | Built-in serializers/deserializers |
| **Compression** | `CompressionTypes.GZIP` enum | Simple string (`'gzip'`) |
| **Diagnostics** | Custom events on client | `node:diagnostics_channel` tracing channels |

## Installation

```bash
npm uninstall kafkajs
npm install @platformatic/kafka
```

## Configuration Mapping

| KafkaJS | @platformatic/kafka | Notes |
|---|---|---|
| `brokers: ['localhost:9092']` | `bootstrapBrokers: ['localhost:9092']` | |
| `clientId: 'my-app'` | `clientId: 'my-app'` | Same |
| `connectionTimeout: 3000` | `connectTimeout: 3000` | Renamed |
| `requestTimeout: 30000` | `requestTimeout: 30000` | Same (on connection level) |
| `retry: { retries: 5 }` | `retries: 5` | Flat option |
| `ssl: true` | `tls: {}` | Object only; `ssl` also accepted as alias |
| `sasl: { mechanism: 'plain', ... }` | `sasl: { mechanism: 'PLAIN', ... }` | Uppercase mechanism values |
| `allowAutoTopicCreation: true` | `autocreateTopics: true` | Renamed |
| — | `timeout: 30000` | General request timeout on client |
| — | `metadataMaxAge: 300000` | Metadata cache TTL |
| — | `strict: true` | Enable runtime options validation |

## Client Creation

### Before (KafkaJS)

```js
const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092']
})

const producer = kafka.producer()
const consumer = kafka.consumer({ groupId: 'my-group' })
const admin = kafka.admin()
```

### After (@platformatic/kafka)

```js
import { Producer, Consumer, Admin } from '@platformatic/kafka'

const producer = new Producer({
  clientId: 'my-app',
  bootstrapBrokers: ['localhost:9092']
})

const consumer = new Consumer({
  clientId: 'my-app',
  bootstrapBrokers: ['localhost:9092'],
  groupId: 'my-group'
})

const admin = new Admin({
  clientId: 'my-app',
  bootstrapBrokers: ['localhost:9092']
})
```

There is no shared `Kafka` factory. Each client is created independently with its own configuration.

## Connection Lifecycle

### Before (KafkaJS)

```js
await producer.connect()
// ... use producer ...
await producer.disconnect()
```

### After (@platformatic/kafka)

```js
// No connect() needed - connections are established lazily on first operation
// ... use producer ...
await producer.close()
```

Connections are established automatically when you perform the first operation (e.g., `send()` or `consume()`). Call `close()` when done to clean up resources.

## Producer Migration

### Basic Send

In KafkaJS, the topic is set at the `send()` call level. In `@platformatic/kafka`, the topic is set per-message.

#### Before (KafkaJS)

```js
await producer.send({
  topic: 'my-topic',
  messages: [
    { key: 'key1', value: 'value1' },
    { key: 'key2', value: 'value2' }
  ]
})
```

#### After (@platformatic/kafka)

```js
import { stringSerializers } from '@platformatic/kafka'

const producer = new Producer({
  clientId: 'my-app',
  bootstrapBrokers: ['localhost:9092'],
  serializers: stringSerializers
})

const result = await producer.send({
  messages: [
    { topic: 'my-topic', key: 'key1', value: 'value1' },
    { topic: 'my-topic', key: 'key2', value: 'value2' }
  ]
})

// result.offsets is TopicWithPartitionAndOffset[]
// Each offset is a bigint, not a string
```

### Multi-Topic Batch

KafkaJS has a separate `sendBatch()` method. In `@platformatic/kafka`, since topic is per-message, a single `send()` handles multiple topics.

#### Before (KafkaJS)

```js
await producer.sendBatch({
  topicMessages: [
    { topic: 'topic-a', messages: [{ value: 'msg1' }] },
    { topic: 'topic-b', messages: [{ value: 'msg2' }] }
  ]
})
```

#### After (@platformatic/kafka)

```js
await producer.send({
  messages: [
    { topic: 'topic-a', value: 'msg1' },
    { topic: 'topic-b', value: 'msg2' }
  ]
})
```

### Compression

#### Before (KafkaJS)

```js
const { CompressionTypes } = require('kafkajs')

await producer.send({
  topic: 'my-topic',
  compression: CompressionTypes.GZIP,
  messages: [{ value: 'compressed' }]
})
```

#### After (@platformatic/kafka)

```js
await producer.send({
  compression: 'gzip', // Simple string: 'none', 'gzip', 'snappy', 'lz4', 'zstd'
  messages: [{ topic: 'my-topic', value: 'compressed' }]
})
```

Available algorithms: `'none'`, `'gzip'`, `'snappy'`, `'lz4'`, `'zstd'`.

### Serialization

KafkaJS always works with `Buffer` values. `@platformatic/kafka` has built-in serializers.

#### Before (KafkaJS)

```js
// Must manually convert to/from Buffer
await producer.send({
  topic: 'my-topic',
  messages: [{
    key: Buffer.from('my-key'),
    value: Buffer.from(JSON.stringify({ hello: 'world' }))
  }]
})
```

#### After (@platformatic/kafka)

```js
import {
  stringSerializers, jsonSerializer, serializersFrom
} from '@platformatic/kafka'

// String serializers - key and value are strings
const producer = new Producer({
  clientId: 'my-app',
  bootstrapBrokers: ['localhost:9092'],
  serializers: stringSerializers
})

await producer.send({
  messages: [{ topic: 'my-topic', key: 'my-key', value: 'hello world' }]
})

// JSON serializer - value can be any serializable object
const jsonProducer = new Producer({
  clientId: 'my-app',
  bootstrapBrokers: ['localhost:9092'],
  serializers: serializersFrom(jsonSerializer)
})
```

### Acknowledgements

#### Before (KafkaJS)

```js
await producer.send({
  topic: 'my-topic',
  acks: -1, // all replicas
  messages: [{ value: 'important' }]
})
```

#### After (@platformatic/kafka)

```js
import { ProduceAcks } from '@platformatic/kafka'

await producer.send({
  acks: ProduceAcks.ALL, // -1
  // Also: ProduceAcks.LEADER (1), ProduceAcks.NO_RESPONSE (0)
  messages: [{ topic: 'my-topic', value: 'important' }]
})
```

Numeric values still work (`-1`, `0`, `1`), but the `ProduceAcks` enum provides better readability.

### Return Types

#### Before (KafkaJS)

```js
const result = await producer.send({ topic: 'my-topic', messages: [...] })
// result: [{ topicName: string, partition: number, errorCode: number,
//            baseOffset: string, logAppendTime: string, logStartOffset: string }]
```

#### After (@platformatic/kafka)

```js
const result = await producer.send({ messages: [...] })
// result: { offsets?: TopicWithPartitionAndOffset[] }
// Where each entry has: { topic: string, partition: number, offset: bigint }
```

## Consumer Migration

### Basic Consumption

This is the biggest architectural change. KafkaJS uses a callback/handler pattern, while `@platformatic/kafka` returns a Node.js `Readable` stream.

#### Before (KafkaJS)

```js
await consumer.subscribe({ topic: 'my-topic', fromBeginning: true })

await consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    console.log({
      key: message.key.toString(),
      value: message.value.toString(),
      offset: message.offset,
      headers: message.headers
    })
  }
})
```

#### After (@platformatic/kafka)

```js
import { Consumer, stringDeserializers, MessagesStreamModes } from '@platformatic/kafka'

const consumer = new Consumer({
  clientId: 'my-app',
  bootstrapBrokers: ['localhost:9092'],
  groupId: 'my-group',
  deserializers: stringDeserializers
})

const stream = await consumer.consume({
  topics: ['my-topic'],
  mode: MessagesStreamModes.EARLIEST
})

for await (const message of stream) {
  console.log({
    key: message.key,
    value: message.value,
    offset: message.offset,  // bigint
    topic: message.topic,
    partition: message.partition,
    headers: message.headers  // Map
  })
}
```

### Offset Modes

#### Before (KafkaJS)

```js
await consumer.subscribe({ topic: 'my-topic', fromBeginning: true })
// or
await consumer.subscribe({ topic: 'my-topic' }) // latest by default
```

#### After (@platformatic/kafka)

```js
import { MessagesStreamModes } from '@platformatic/kafka'

const stream = await consumer.consume({
  topics: ['my-topic'],
  mode: MessagesStreamModes.EARLIEST  // replaces fromBeginning: true
  // or: MessagesStreamModes.LATEST (default)
  // or: MessagesStreamModes.COMMITTED
  // or: MessagesStreamModes.MANUAL (with offsets array)
})
```

### Seeking to Specific Offsets

#### Before (KafkaJS)

```js
consumer.seek({ topic: 'my-topic', partition: 0, offset: '10' })
```

#### After (@platformatic/kafka)

```js
const stream = await consumer.consume({
  topics: ['my-topic'],
  mode: MessagesStreamModes.MANUAL,
  offsets: [{ topic: 'my-topic', partition: 0, offset: 10n }]
})
```

### Autocommit

#### Before (KafkaJS)

```js
await consumer.run({
  autoCommitInterval: 5000,
  eachMessage: async ({ message }) => { /* ... */ }
})
```

#### After (@platformatic/kafka)

```js
const stream = await consumer.consume({
  topics: ['my-topic'],
  autocommit: 5000 // interval in ms; true for default; false to disable
})
```

### Manual Commit

#### Before (KafkaJS)

```js
await consumer.run({
  autoCommit: false,
  eachMessage: async ({ topic, partition, message }) => {
    // process message...
    await consumer.commitOffsets([{
      topic,
      partition,
      offset: (Number(message.offset) + 1).toString()
    }])
  }
})
```

#### After (@platformatic/kafka)

```js
const stream = await consumer.consume({
  topics: ['my-topic'],
  autocommit: false,
  mode: MessagesStreamModes.EARLIEST
})

for await (const message of stream) {
  // process message...
  await message.commit()
}
```

Each message has a `commit()` method that commits its offset directly.

### Isolation Level

#### Before (KafkaJS)

```js
const consumer = kafka.consumer({
  groupId: 'my-group',
  readUncommitted: false  // default is READ_COMMITTED
})
```

#### After (@platformatic/kafka)

```js
import { FetchIsolationLevels } from '@platformatic/kafka'

const stream = await consumer.consume({
  topics: ['my-topic'],
  isolationLevel: FetchIsolationLevels.READ_COMMITTED // 1
  // or: FetchIsolationLevels.READ_UNCOMMITTED (0)
})
```

### Message Shape

| Property | KafkaJS | @platformatic/kafka |
|---|---|---|
| `key` | `Buffer \| null` | `Buffer` (or deserialized type) |
| `value` | `Buffer \| null` | `Buffer` (or deserialized type) |
| `offset` | `string` | `bigint` |
| `timestamp` | `string` | `bigint` |
| `headers` | `Record<string, Buffer>` | `Map<HeaderKey, HeaderValue>` |
| `topic` | provided via callback arg | `message.topic` |
| `partition` | provided via callback arg | `message.partition` |
| `commit()` | N/A | `message.commit()` |

## Transaction Migration

### Before (KafkaJS)

```js
const transaction = await producer.transaction()

try {
  await transaction.send({
    topic: 'my-topic',
    messages: [{ key: 'key', value: 'value' }]
  })
  await transaction.commit()
} catch (e) {
  await transaction.abort()
  throw e
}
```

### After (@platformatic/kafka)

```js
const producer = new Producer({
  clientId: 'my-app',
  bootstrapBrokers: ['localhost:9092'],
  idempotent: true,
  transactionalId: 'my-transaction'
})

const transaction = await producer.beginTransaction()

try {
  await transaction.send({
    messages: [{ topic: 'my-topic', key: Buffer.from('key'), value: Buffer.from('value') }]
  })
  await transaction.commit()
} catch (e) {
  await transaction.abort()
  throw e
}
```

Key differences:
- `producer.transaction()` becomes `producer.beginTransaction()`
- The producer must have `idempotent: true` and a `transactionalId`
- Topic is per-message in the `send()` call

## Admin Migration

### Create Topics

#### Before (KafkaJS)

```js
await admin.createTopics({
  topics: [
    { topic: 'topic-a', numPartitions: 3, replicationFactor: 1 },
    { topic: 'topic-b', numPartitions: 3, replicationFactor: 1 }
  ]
})
```

#### After (@platformatic/kafka)

```js
const created = await admin.createTopics({
  topics: ['topic-a', 'topic-b'],
  partitions: 3,
  replicas: 1
})
// Returns CreatedTopic[] with { id, name, partitions, replicas, configuration }
```

Topic names are simple strings. Configuration like `partitions` and `replicas` is shared across all topics in a single call.

### Delete Topics

#### Before (KafkaJS)

```js
await admin.deleteTopics({ topics: ['topic-a', 'topic-b'] })
```

#### After (@platformatic/kafka)

```js
await admin.deleteTopics({ topics: ['topic-a', 'topic-b'] })
```

Same API.

### List and Describe Groups

#### Before (KafkaJS)

```js
const { groups } = await admin.listGroups()
// groups: [{ groupId: string, protocolType: string }]

const { groups: described } = await admin.describeGroups(['group-id'])
// described: [{ groupId, members, state, ... }]
```

#### After (@platformatic/kafka)

```js
const groups = await admin.listGroups()
// Returns Map<string, GroupBase> with { id, state, groupType, protocolType }

const described = await admin.describeGroups({ groups: ['group-id'] })
// Returns Map<string, Group> with { id, state, protocol, members, ... }
// members is a Map<string, GroupMember>
```

### Topic Offsets

#### Before (KafkaJS)

```js
const offsets = await admin.fetchTopicOffsets('my-topic')
// [{ partition: 0, offset: '100', high: '100', low: '0' }]
```

#### After (@platformatic/kafka)

```js
const offsets = await admin.listOffsets({
  topics: [{
    name: 'my-topic',
    partitions: [{ partitionIndex: 0, timestamp: -1n }]  // -1n = latest
  }]
})
// Returns ListedOffsetsTopic[] with bigint offsets
```

### Consumer Group Offsets

#### Before (KafkaJS)

```js
const offsets = await admin.fetchOffsets({ groupId: 'my-group', topics: ['my-topic'] })
```

#### After (@platformatic/kafka)

```js
const offsets = await admin.listConsumerGroupOffsets({
  groups: ['my-group']
})
// Returns ListConsumerGroupOffsetsGroup[] with bigint offsets
```

### Cluster Info

#### Before (KafkaJS)

```js
const cluster = await admin.describeCluster()
// { brokers: [{ nodeId, host, port }], controller, clusterId }
```

#### After (@platformatic/kafka)

```js
const metadata = await admin.metadata({})
// Returns ClusterMetadata:
// {
//   id: string,
//   brokers: Map<number, { host, port, rack }>,
//   controllerId: number,
//   topics: Map<string, ClusterTopicMetadata>,
//   lastUpdate: number
// }
```

## Error Handling

### Before (KafkaJS)

```js
const { KafkaJSError, KafkaJSProtocolError } = require('kafkajs')

try {
  await producer.send(...)
} catch (error) {
  if (error instanceof KafkaJSError) {
    console.log(error.retriable)
  }
  if (error instanceof KafkaJSProtocolError) {
    console.log(error.code)  // numeric protocol error code
  }
}
```

### After (@platformatic/kafka)

```js
import { GenericError, MultipleErrors, ProtocolError } from '@platformatic/kafka'

try {
  await producer.send(...)
} catch (error) {
  if (GenericError.isGenericError(error)) {
    console.log(error.code)     // 'PLT_KFK_*' string code
    console.log(error.canRetry) // boolean
  }
  if (error instanceof ProtocolError) {
    console.log(error.apiId)    // e.g. 'UNKNOWN_TOPIC_OR_PARTITION'
    console.log(error.apiCode)  // e.g. 'UNKNOWN_TOPIC_OR_PARTITION'
  }
  if (MultipleErrors.isMultipleErrors(error)) {
    // AggregateError - iterate error.errors
    for (const inner of error.errors) {
      console.log(inner.code)
    }
  }
}
```

### Error Code Mapping

| KafkaJS | @platformatic/kafka |
|---|---|
| `KafkaJSError` | `GenericError` |
| `KafkaJSProtocolError` | `ProtocolError` |
| `KafkaJSOffsetOutOfRange` | `OutOfBoundsError` (`PLT_KFK_OUT_OF_BOUNDS`) |
| `KafkaJSNumberOfRetriesExceeded` | `MultipleErrors` (`PLT_KFK_MULTIPLE`) |
| `KafkaJSConnectionError` | `NetworkError` (`PLT_KFK_NETWORK`) |
| `KafkaJSRequestTimeoutError` | `TimeoutError` (`PLT_KFK_TIMEOUT`) |
| `KafkaJSSASLAuthenticationError` | `AuthenticationError` (`PLT_KFK_AUTHENTICATION`) |
| `error.retriable` | `error.canRetry` |

## Event Mapping

KafkaJS emits events on client instances. `@platformatic/kafka` uses Node.js `diagnostics_channel` tracing channels.

| KafkaJS Event | @platformatic/kafka Channel |
|---|---|
| `producer.on('producer.connect', ...)` | `subscribe('plt:kafka:instances', ...)` |
| `producer.on('producer.disconnect', ...)` | Listen for `close` event on the producer |
| `consumer.on('consumer.group_join', ...)` | `plt:kafka:consumer:group` tracing channel |
| `consumer.on('consumer.heartbeat', ...)` | `plt:kafka:consumer:heartbeat` tracing channel |
| `consumer.on('consumer.commit_offsets', ...)` | `plt:kafka:consumer:commits` tracing channel |
| `consumer.on('consumer.fetch', ...)` | `plt:kafka:consumer:fetches` tracing channel |
| Connection events | `plt:kafka:connections:connects` tracing channel |

Example:

```js
import { subscribe } from 'node:diagnostics_channel'
import { consumerGroupChannel } from '@platformatic/kafka'

subscribe(consumerGroupChannel.name, (data) => {
  console.log('Consumer group event:', data)
})
```

## Migration Checklist

1. Replace `kafkajs` with `@platformatic/kafka` in `package.json`
2. Replace `new Kafka({...}).producer()` with `new Producer({...})`
3. Replace `new Kafka({...}).consumer({...})` with `new Consumer({...})`
4. Replace `new Kafka({...}).admin()` with `new Admin({...})`
5. Rename `brokers` to `bootstrapBrokers`
6. Rename `connectionTimeout` to `connectTimeout`
7. Flatten `retry: { retries: N }` to `retries: N`
8. Rename `allowAutoTopicCreation` to `autocreateTopics`
9. Uppercase SASL mechanism values (`'plain'` to `'PLAIN'`)
10. Remove all `connect()` calls - connections are automatic
11. Replace `disconnect()` with `close()`
12. Move `topic` from `send()` level into each message object
13. Replace `sendBatch()` with a single `send()` call
14. Replace `CompressionTypes.GZIP` with `'gzip'` strings
15. Add serializers/deserializers to client options or handle `Buffer` manually
16. Replace `subscribe()` + `run({ eachMessage })` with `consume()` stream
17. Replace `fromBeginning: true` with `mode: 'earliest'`
18. Replace `commitOffsets([...])` with `message.commit()`
19. Replace `producer.transaction()` with `producer.beginTransaction()`
20. Update offset handling from `string` to `bigint`
21. Update header handling from `Record<string, Buffer>` to `Map`
22. Replace `error.retriable` with `error.canRetry`
23. Migrate event listeners to `diagnostics_channel` subscribers
24. Update admin API calls to new method signatures
