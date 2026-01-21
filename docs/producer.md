# Producer

Client for producing messages to Kafka topics with support for idempotent production and transactions.

The producer inherits from the [`Base`](./base.md) client.

The complete TypeScript type of the `Producer` is determined by the `serializers` option.

The producer supports idempotent production and transactional message production. For detailed information about transactions, see the [Transactions](./transactions.md) guide.

## Constructor

Creates a new producer with type `Producer<Key, Value, HeaderKey, HeaderValue>`.

Options:

| Property                | Type                                                               | Description                                                                                                                                                          |
| ----------------------- | ------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `producerId`            | `bigint`                                                           | Producer ID.                                                                                                                                                         |
| `producerEpoch`         | `number`                                                           | Producer epoch.                                                                                                                                                      |
| `idempotent`            | `boolean`                                                          | Idempotency of the producer. Required for transactions.                                                                                                              |
| `transactionalId`       | `string`                                                           | Transactional ID for the producer. If not specified, a random UUID is generated. Required when using transactions to ensure the same ID is used across restarts.     |
| `acks`                  | `number`                                                           | Acknowledgement to wait before returning.<br/><br/>Valid values are defined in the `ProduceAcks` enumeration.                                                        |
| `compression`           | `string`                                                           | Compression algorithm to use before sending messages to the broker.<br/><br/>Valid values are: `snappy`, `lz4`, `gzip`, `zstd` |
| `partitioner`           | `(message: MessageToProduce<Key, Value, HeaderKey, HeaderValue>) => number` | Partitioner to use to assign a partition to messages that lack it.<br/><br/>It is a function that receives a message and should return the partition number.         |
| `repeatOnStaleMetadata` | `boolean`                                                          | Whether to retry a produce operation when the system detects outdated topic or broker information.<br/><br/>Default is `true`.                                       |
| `serializers`           | `Serializers<Key, Value, HeaderKey, HeaderValue>`                  | Object that specifies which serialisers to use.<br/><br/>The object should only contain one or more of the `key`, `value`, `headerKey` and `headerValue` properties. |

It also supports all the constructor options of `Base`.

Notes: `zstd` is not available in node `v20`

## Basic Methods

### `send<Key, Value, HeaderKey, HeaderValue>(options[, callback])`

Sends one or more messages to Kafka.

When `acks` is not `ProduceAcks.NO_RESPONSE`, then the return value is an object with the property `offsets` containing a list of written topic-partition-offset triplets.

When `acks` is `ProduceAcks.NO_RESPONSE`, then the return value is an object with the property `unwritableNodes` containing a list of nodes which are currently busy and should wait for a `client:broker:drain` event before continuing.

Options:

| Property   | Type                                            | Description           |
| ---------- | ----------------------------------------------- | --------------------- |
| `messages` | `MessageToProduce<Key, Value, HeaderKey, HeaderValue>[]` | The messages to send. |

It also accepts all options of the constructor except `serializers`.

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

### `close([callback])`

Closes the producer and all its connections.

The return value is `void`.

## Advanced Methods

The producer manages auxiliary operations automatically. Some of the APIs are exposed to allow for advanced uses.

### `initIdempotentProducer<Key, Value, HeaderKey, HeaderValue>(options[, callback])`

Initialises an idempotent producer. It accepts all options of the constructor except `serializers`.

The return value is an object containing the `producerId` and `producerEpoch` values returned from the broker.
