# Producer

Client for producing messages to Kafka topics with support for idempotent production and transactions.

The producer inherits from the [`Base`](./base.md) client.

The complete TypeScript type of the `Producer` is determined by the `serializers` option.

The producer supports idempotent production and transactional message production. For detailed information about transactions, see the [Transactions](./transactions.md) guide.

## Constructor

Creates a new producer with type `Producer<Key, Value, HeaderKey, HeaderValue>`.

Options:

| Property                | Type                                                                        | Description                                                                                                                                                                                                                                                                                                                                        |
| ----------------------- | --------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `producerId`            | `bigint`                                                                    | Producer ID.                                                                                                                                                                                                                                                                                                                                       |
| `producerEpoch`         | `number`                                                                    | Producer epoch.                                                                                                                                                                                                                                                                                                                                    |
| `idempotent`            | `boolean`                                                                   | Idempotency of the producer. Required for transactions.                                                                                                                                                                                                                                                                                            |
| `transactionalId`       | `string`                                                                    | Transactional ID for the producer. If not specified, a random UUID is generated. Required when using transactions to ensure the same ID is used across restarts.                                                                                                                                                                                   |
| `acks`                  | `number`                                                                    | Acknowledgement to wait before returning.<br/><br/>Valid values are defined in the `ProduceAcks` enumeration.                                                                                                                                                                                                                                      |
| `compression`           | `string`                                                                    | Compression algorithm to use before sending messages to the broker.<br/><br/>Valid values are: `snappy`, `lz4`, `gzip`, `zstd`                                                                                                                                                                                                                     |
| `partitioner`           | `(message: MessageToProduce<Key, Value, HeaderKey, HeaderValue>) => number` | Partitioner to use to assign a partition to messages that lack it.<br/><br/>It is a function that receives a message and should return the partition number.                                                                                                                                                                                       |
| `repeatOnStaleMetadata` | `boolean`                                                                   | Whether to retry a produce operation when the system detects outdated topic or broker information.<br/><br/>Default is `true`.                                                                                                                                                                                                                     |
| `serializers`           | `Serializers<Key, Value, HeaderKey, HeaderValue>`                           | Object that specifies which serialisers to use.<br/><br/>The object should only contain one or more of the `key`, `value`, `headerKey` and `headerValue` properties.<br/><br/>**Note:** Should not be provided when using a `registry`.                                                                                                            |
| `beforeSerialization`   | `BeforeSerializationHook<Key, Value, HeaderKey, HeaderValue>`               | Hook function called before serialization of each message component (key, value, headers).<br/><br/>**Experimental:** Does not follow semver and may change in minor/patch releases.<br/><br/>**Note:** Should not be provided when using a `registry`.                                                                                            |
| `registry`              | `AbstractSchemaRegistry<Key, Value, HeaderKey, HeaderValue>`                | Schema registry instance for automatic serialization with schema management. See the [Confluent Schema Registry](./confluent-schema-registry.md) guide for details.<br/><br/>**Experimental:** Does not follow semver and may change in minor/patch releases.<br/><br/>**Note:** When provided, do not use `serializers` or `beforeSerialization`. |

It also supports all the constructor options of `Base`.

Notes: `zstd` is not available in node `v20`

## Basic Methods

### `send<Key, Value, HeaderKey, HeaderValue>(options[, callback])`

Sends one or more messages to Kafka.

When `acks` is not `ProduceAcks.NO_RESPONSE`, then the return value is an object with the property `offsets` containing a list of written topic-partition-offset triplets.

When `acks` is `ProduceAcks.NO_RESPONSE`, then the return value is an object with the property `unwritableNodes` containing a list of nodes which are currently busy and should wait for a `client:broker:drain` event before continuing.

Options:

| Property   | Type                                                     | Description           |
| ---------- | -------------------------------------------------------- | --------------------- |
| `messages` | `MessageToProduce<Key, Value, HeaderKey, HeaderValue>[]` | The messages to send. |

It also accepts all options of the constructor except `serializers`.

### `asStream<Key, Value, HeaderKey, HeaderValue>([options])`

Creates a `ProducerStream` writable stream that batches messages and sends them through this producer.

The stream lifecycle is tracked by the producer. If one or more streams are still active, `producer.close()` will fail unless `force` is set to `true`.

### `streamsCount`

Returns the number of currently active `ProducerStream` instances created by this producer.

### `beginTransaction([options, callback])`

Begins a new transaction and returns a `Transaction` object.

The producer must be configured with `idempotent: true` to use transactions. Only one transaction can be active at a time per producer.

Options: accepts all options from the constructor except `serializers`.

The return value is a `Transaction` object. See the [Transactions](./transactions.md) guide for detailed usage.

Example:

```typescript
const producer = new Producer({
  clientId: 'my-producer',
  bootstrapBrokers: ['localhost:9092'],
  idempotent: true,
  transactionalId: 'my-transaction-id'
})

const transaction = await producer.beginTransaction()
await transaction.send({ messages: [{ topic: 'my-topic', value: 'message' }] })
await transaction.commit()
```

### `close([force][, callback])`

Closes the producer and all its connections.

If `force` is not `true`, then the method throws an error if any `ProducerStream` created from this producer is still active.

If `force` is `true`, then the method closes all active `ProducerStream` instances before closing the producer.

The return value is `void`.

## Using Producers as streams.

`producer.asStream()` returns a `ProducerStream`, a writable stream in object mode.

This is useful when:

- you already have a Node.js stream pipeline,
- you want automatic batching (`batchSize` / `batchTime`),
- you want stream backpressure to naturally regulate producers.

### Stream options

`asStream([options])` accepts:

- all `send()` produce options except `messages` (for example `acks`, `compression`, `partitioner`, `autocreateTopics`, `repeatOnStaleMetadata`),
- plus stream-specific options:
  - `batchSize` (max messages per flush),
  - `batchTime` (max wait before timed flush),
  - `reportMode` (`NONE`, `BATCH`, `MESSAGE`),
  - `highWaterMark` (writable stream high-water mark).

### Lifecycle and shutdown

The producer tracks every `ProducerStream` it creates.

You can inspect active stream count through `producer.streamsCount`.

Shutdown rules:

1. `await producer.close()` fails if one or more producer streams are still active.
2. `await producer.close(true)` force-closes active producer streams, then closes the producer.

Recommended patterns:

- Explicit close (preferred): close streams first, then producer.
- Force close: useful during shutdown paths where you cannot reliably coordinate all stream owners.

### Example: piping with `pipeline`

```typescript
import { Readable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { Producer } from '@platformatic/kafka'

const producer = new Producer({
  clientId: 'my-producer',
  bootstrapBrokers: ['localhost:9092']
})

const sink = producer.asStream({
  batchSize: 500,
  batchTime: 25
})

await pipeline(
  Readable.from(
    [
      { topic: 'events', key: Buffer.from('k1'), value: Buffer.from('v1') },
      { topic: 'events', key: Buffer.from('k2'), value: Buffer.from('v2') }
    ],
    { objectMode: true }
  ),
  sink
)

// pipeline() ends the sink; then close producer
await producer.close()
```

### Example: manual writes + delivery reports

```typescript
import { Producer, ProducerStreamReportModes, stringSerializers } from '@platformatic/kafka'

const producer = new Producer<string, string, string, string>({
  clientId: 'my-producer',
  bootstrapBrokers: ['localhost:9092'],
  serializers: stringSerializers
})

const stream = producer.asStream({
  batchSize: 100,
  batchTime: 50,
  reportMode: ProducerStreamReportModes.BATCH
})

stream.on('delivery-report', report => {
  // In BATCH mode: { batchId, count, result }
  console.log(report)
})

stream.on('flush', info => {
  // { batchId, count, duration, result }
  console.log('flushed', info.count)
})

stream.write({ topic: 'events', key: 'user-1', value: 'login' })
stream.write({ topic: 'events', key: 'user-2', value: 'logout' })

await stream.close()
await producer.close()

// Alternative shutdown:
// await producer.close(true)
```

## Using Schema Registries

> ⚠️ **Experimental API**
> Confluent Schema Registry support and the `registry`/`beforeSerialization` integration are experimental.
> They **do not follow semver** and may change in minor/patch releases.

The producer supports automatic serialization through schema registries like [Confluent Schema Registry](./confluent-schema-registry.md). When using a schema registry, messages are automatically serialized according to their schemas and schema IDs are included in the message headers.

Example with Confluent Schema Registry:

```typescript
import { Producer } from '@platformatic/kafka'
import { ConfluentSchemaRegistry } from '@platformatic/kafka/registries'

// Create a schema registry instance
const registry = new ConfluentSchemaRegistry({
  url: 'http://localhost:8081',
  auth: {
    username: 'user',
    password: 'password'
  }
})

// Create a producer with the registry
const producer = new Producer({
  clientId: 'my-producer',
  bootstrapBrokers: ['localhost:9092'],
  registry // Registry handles serialization automatically
})

// Send messages with schema IDs in metadata
await producer.send({
  messages: [
    {
      topic: 'events',
      key: { id: 123 },
      value: { name: 'John', action: 'login' },
      metadata: {
        schemas: {
          key: 1, // Schema ID for key
          value: 2 // Schema ID for value
        }
      }
    }
  ]
})

await producer.close()
```

When using a schema registry:

- **Do not provide** the `serializers` option - the registry provides its own serializers
- **Do not provide** the `beforeSerialization` hook - the registry provides its own hook
- Messages are automatically serialized according to their schemas
- Schema IDs must be provided in the message metadata
- The registry fetches and caches schemas as needed
- Supports AVRO, Protocol Buffers, and JSON Schema formats

For more details, see the [Confluent Schema Registry](./confluent-schema-registry.md) documentation.

## Advanced Methods

The producer manages auxiliary operations automatically. Some of the APIs are exposed to allow for advanced uses.

### `initIdempotentProducer<Key, Value, HeaderKey, HeaderValue>(options[, callback])`

Initialises an idempotent producer. It accepts all options of the constructor except `serializers`.

The return value is an object containing the `producerId` and `producerEpoch` values returned from the broker.
