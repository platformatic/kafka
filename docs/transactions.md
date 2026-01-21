# Transactions

Kafka transactions provide exactly-once semantics by allowing producers to send messages atomically across multiple partitions and topics. All messages in a transaction are either successfully committed or aborted together.

## Overview

Transactions in @platformatic/kafka enable:

- **Atomic writes**: Send messages to multiple topics and partitions atomically
- **Read-process-write patterns**: Consume messages, process them, produce results, and commit offsets atomically
- **Exactly-once semantics**: Prevent duplicate message processing when combined with idempotent production

## Prerequisites

To use transactions, the producer must be configured with:

- `idempotent: true` - Required for transaction support
- `transactionalId: string` - A unique identifier for the transactional producer.

```typescript
const producer = new Producer({
  clientId: 'my-client',
  bootstrapBrokers: ['localhost:9092'],
  idempotent: true,
  transactionalId: 'my-unique-transaction-id',
  serializers: stringSerializers
})
```

The `transactionalId` should be stable across application restarts to ensure exactly-once semantics. If not specified, a random UUID is generated.

## Basic Usage

### Simple Transaction

```typescript
// Begin a transaction
const transaction = await producer.beginTransaction()

try {
  // Send messages within the transaction
  await transaction.send({
    messages: [
      { topic: 'orders', value: 'order-1' },
      { topic: 'inventory', value: 'reduce-stock-1' }
    ]
  })

  // Commit the transaction
  await transaction.commit()
} catch (error) {
  // Abort the transaction on error
  await transaction.abort()
}
```

### Multiple Sends in a Transaction

```typescript
const transaction = await producer.beginTransaction()

try {
  // First batch of messages
  await transaction.send({
    messages: [{ topic: 'topic1', value: 'message1' }]
  })

  // Second batch of messages
  await transaction.send({
    messages: [{ topic: 'topic2', value: 'message2' }]
  })

  await transaction.commit()
} catch (error) {
  await transaction.abort()
}
```

## Consumer Integration

Transactions can be used with consumers to implement exactly-once read-process-write patterns. This ensures that consumed messages are processed exactly once, even in case of failures.

**Important**: When using a consumer within a transaction, you must manually manage offset commits through the transaction itself. Do not use:

- The consumer's `autocommit` option - set this to `false` or omit it
- The message's `commit()` method - offsets should be committed via `transaction.addOffset()` instead

The transaction will atomically commit all offsets when `transaction.commit()` is called, ensuring exactly-once semantics.

### Reading Committed Messages

Consumers should use `READ_COMMITTED` isolation level to only read messages from committed transactions:

```typescript
const consumer = new Consumer({
  groupId: 'my-group',
  clientId: 'my-client',
  bootstrapBrokers: ['localhost:9092'],
  deserializers: stringDeserializers
})

const stream = await consumer.consume({
  topics: ['input-topic'],
  isolationLevel: FetchIsolationLevels.READ_COMMITTED
})
```

### Adding Consumer Offsets to Transaction

When processing messages from a consumer, you can commit the consumer offsets as part of the transaction:

```typescript
const transaction = await producer.beginTransaction()

try {
  // Add the consumer to the transaction
  await transaction.addConsumer(consumer)

  // Process messages from the consumer
  stream.on('data', async message => {
    // Process the message and produce results
    await transaction.send({
      messages: [{ topic: 'output-topic', value: processedValue }]
    })

    // Add the message offset to the transaction
    await transaction.addOffset(message)
  })

  // Commit both the produced messages and consumer offsets
  await transaction.commit()
} catch (error) {
  await transaction.abort()
}
```

## Transaction API

### Properties

| Property    | Type      | Description                                                         |
| ----------- | --------- | ------------------------------------------------------------------- |
| `id`        | `string`  | The transactional ID of the transaction.                            |
| `completed` | `boolean` | Indicates whether the transaction has been completed (committed or aborted). |

### Methods

#### `send(options[, callback])`

Sends messages within the transaction. Messages are not visible to consumers until the transaction is committed.

Options: accepts the same options as `Producer.send()` except it cannot be used with a different transaction.

The return value is a `Promise<ProduceResult>`.

Example:

```typescript
await transaction.send({
  messages: [
    { topic: 'my-topic', key: 'key1', value: 'value1' },
    { topic: 'my-topic', key: 'key2', value: 'value2' }
  ]
})
```

#### `addConsumer(consumer[, callback])`

Adds a consumer group to the transaction. This is required before calling `addOffset()`.

Options:

| Property   | Type       | Description           |
| ---------- | ---------- | --------------------- |
| `consumer` | `Consumer` | A `Consumer` instance |

The return value is `void`.

Example:

```typescript
await transaction.addConsumer(consumer)
```

#### `addOffset(message[, callback])`

Commits a consumer offset as part of the transaction. The consumer must be added to the transaction first using `addConsumer()`.

Options:

| Property  | Type      | Description                                      |
| --------- | --------- | ------------------------------------------------ |
| `message` | `Message` | A message object from the consumer with metadata |

The return value is `void`.

Example:

```typescript
stream.on('data', async message => {
  // Process the message
  await transaction.send({
    messages: [{ topic: 'output', value: processed }]
  })

  // Commit the offset
  await transaction.addOffset(message)
})
```

#### `commit([callback])`

Commits the transaction, making all sent messages visible to consumers and persisting consumer offsets.

The return value is `void`.

Once committed, the transaction is marked as completed and cannot be used again.

Example:

```typescript
await transaction.commit()
```

#### `abort([callback])`

Aborts the transaction, discarding all sent messages and consumer offsets.

The return value is `void`.

Once aborted, the transaction is marked as completed and cannot be used again.

Example:

```typescript
try {
  await transaction.send({ messages: [...] })
  await transaction.commit()
} catch (error) {
  await transaction.abort()
}
```

#### `cancel([callback])`

Cancels the transaction locally without sending an abort request to the broker. This is useful for cleaning up local state when you want to abandon a transaction without network calls.

The return value is `void`.

Example:

```typescript
await transaction.cancel()
```
