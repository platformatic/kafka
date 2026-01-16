# Producer

Client for producing messages to Kafka topics with support for idempotent production and transactions.

The producer inherits from the [`Base`](./base.md) client.

The complete TypeScript type of the `Producer` is determined by the `serializers` option.

The producer supports idempotent production and transactions.

## Constructor

Creates a new producer with type `Producer<Key, Value, HeaderKey, HeaderValue>`.

Options:

| Property                | Type                                                                        | Description                                                                                                                                                          |
| ----------------------- | --------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `producerId`            | `bigint`                                                                    | Producer ID. This is ignored when using transactions or idempotency.                                                                                                 |
| `producerEpoch`         | `number`                                                                    | Producer epoch. This is ignored when using transactions or idempotency.                                                                                              |
| `idempotent`            | `boolean`                                                                   | Idempotency of the producer.                                                                                                                                         |
| `transactionalId`       | `string`                                                                    | Unique transaction ID to enable transactional production. Requires `idempotent` to be `true`.                                                                        |
| `acks`                  | `number`                                                                    | Acknowledgement to wait before returning.<br/><br/>Valid values are defined in the `ProduceAcks` enumeration. This is ignored when using transactions.               |
| `compression`           | `string`                                                                    | Compression algorithm to use before sending messages to the broker.<br/><br/>Valid values are: `snappy`, `lz4`, `gzip`, `zstd`                                       |
| `partitioner`           | `(message: MessageToProduce<Key, Value, HeaderKey, HeaderValue>) => number` | Partitioner to use to assign a partition to messages that lack it.<br/><br/>It is a function that receives a message and should return the partition number.         |
| `repeatOnStaleMetadata` | `boolean`                                                                   | Whether to retry a produce operation when the system detects outdated topic or broker information.<br/><br/>Default is `true`.                                       |
| `serializers`           | `Serializers<Key, Value, HeaderKey, HeaderValue>`                           | Object that specifies which serialisers to use.<br/><br/>The object should only contain one or more of the `key`, `value`, `headerKey` and `headerValue` properties. |

It also supports all the constructor options of `Base`.

Notes: `zstd` is not available in node `v20`

## Properties

### `transactionalId`

Returns the transaction ID if the producer was created with a `transactionalId` option, otherwise returns `undefined`. This property is only available after calling `beginTransaction()`.

### `coordinatorId`

Returns the coordinator node ID for transactional producers. This property is only available after calling `beginTransaction()`.

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

### `close([callback])`

Closes the producer and all its connections.

The return value is `void`.

## Transaction Methods

Transactions provide exactly-once semantics for producing messages to Kafka. To use transactions, the producer must be created with both `idempotent: true` and a `transactionalId`.

### `beginTransaction([callback])`

Begins a new transaction. This method must be called before sending messages within a transaction.

If a transaction is already active, this method is a no-op.

### `commitTransaction([callback])`

Commits the current transaction, making all messages sent within the transaction visible to consumers.

### `abortTransaction([callback])`

Aborts the current transaction, discarding all messages sent within the transaction.

### `linkMessageStreamToTransaction<Key, Value, HeaderKey, HeaderValue>(stream[, callback])`

Links a consumer message stream to the current transaction. This enables exactly-once semantics (EOS) when consuming messages from one topic and producing to another within the same transaction.

When a message stream is linked to a transaction, you can use the producer `commit` method to commit individual message offsets within the transaction. This ensures that consumed messages are only marked as processed if the entire transaction succeeds.

Note that when using this feature you should disable `autocommit` on the consumer and not use the messages' `commit` method.

Parameters:

| Property | Type                                                 | Description                                             |
| -------- | ---------------------------------------------------- | ------------------------------------------------------- |
| `stream` | `MessagesStream<Key, Value, HeaderKey, HeaderValue>` | The consumer message stream to link to the transaction. |

### `commit<Key, Value, HeaderKey, HeaderValue>(message[, callback])`

Commits a consumer message offset within the current transaction.

The message offset is only committed to the consumer group if the transaction is successfully committed. If the transaction is aborted, the offset is not committed.

Parameters:

| Property  | Type                                          | Description                     |
| --------- | --------------------------------------------- | ------------------------------- |
| `message` | `Message<Key, Value, HeaderKey, HeaderValue>` | The consumer message to commit. |

### Transaction Example

```javascript
import { Producer, stringSerializers } from '@platformatic/kafka'

const producer = new Producer({
  clientId: 'my-client',
  bootstrapBrokers: ['localhost:9092'],
  idempotent: true,
  transactionalId: 'my-unique-transaction-id',
  serializers: stringSerializers
})

// Begin a transaction
await producer.beginTransaction()

try {
  // Send messages within the transaction
  await producer.send({
    messages: [
      { topic: 'my-topic', key: 'key1', value: 'value1' },
      { topic: 'my-topic', key: 'key2', value: 'value2' }
    ]
  })

  // Commit the transaction
  await producer.commitTransaction()
} catch (error) {
  // Abort the transaction on error
  await producer.abortTransaction()
  throw error
}

await producer.close()
```

### Exactly-Once Semantics (EOS) Example

The following example demonstrates how to achieve exactly-once semantics when consuming messages from one topic and producing to another:

```javascript
import { Consumer, Producer, stringSerializers } from '@platformatic/kafka'

const consumer = new Consumer({
  clientId: 'my-client',
  bootstrapBrokers: ['localhost:9092'],
  groupId: 'my-consumer-group',
  serializers: stringSerializers
})

const producer = new Producer({
  clientId: 'my-client',
  bootstrapBrokers: ['localhost:9092'],
  idempotent: true,
  transactionalId: 'my-unique-transaction-id',
  serializers: stringSerializers
})

// Subscribe to a topic
const stream = await consumer.subscribe({
  topic: 'input-topic',
  autocommit: false // Disable autocommit when using transactions
})

// Begin a transaction
await producer.beginTransaction()

// Link the consumer stream to the transaction
await producer.linkMessageStreamToTransaction(stream)

try {
  for await (const message of stream) {
    // Process the message
    const transformedValue = message.value.toUpperCase()

    // Produce the transformed message
    await producer.send({
      messages: [{ topic: 'output-topic', key: message.key, value: transformedValue }]
    })

    // Commit the consumed message offset within the transaction
    await producer.commit(message)
  }

  // Commit the transaction (commits both produced messages and consumed offsets)
  await producer.commitTransaction()
} catch (error) {
  // Abort the transaction on error
  await producer.abortTransaction()
  throw error
}

await producer.close()
await consumer.close()
```

## Advanced Methods

The producer manages auxiliary operations automatically. Some of the APIs are exposed to allow for advanced uses.

### `initIdempotentProducer<Key, Value, HeaderKey, HeaderValue>(options[, callback])`

Initialises an idempotent producer. It accepts all options of the constructor except `serializers`.

The return value is an object containing the `producerId` and `producerEpoch` values returned from the broker.
