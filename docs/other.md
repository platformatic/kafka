# Other APIs and types

### `Connection`

Represents a single low-level broker connection.

It is returned by [`Base.connectToBrokers()`](./base.md) and exposed by `ConnectionPool` operations.

#### Properties

| Name         | Type                    | Description                                                              |
| ------------ | ----------------------- | ------------------------------------------------------------------------ |
| `host`       | `string \| undefined`   | Current target host while connecting or connected.                       |
| `port`       | `number \| undefined`   | Current target port while connecting or connected.                       |
| `instanceId` | `number`                | Numeric identifier for this connection instance.                         |
| `ownerId`    | `number \| undefined`   | Optional owner instance ID of the component that created the connection. |
| `status`     | `ConnectionStatusValue` | Current connection state.                                                |
| `context`    | `unknown`               | Opaque user context forwarded from the creating component.               |
| `socket`     | `Socket`                | The underlying Node.js socket.                                           |

#### Methods

#### `isConnected()`

Returns `true` when the connection status is `CONNECTED`.

#### `connect(host, port[, callback])`

Opens the socket connection to a broker and performs SASL authentication if configured.

#### `ready([callback])`

Waits until the connection becomes usable or fails while waiting.

#### `close([callback])`

Gracefully closes the socket and resolves once the connection is fully closed.

#### `send(apiKey, apiVersion, createPayload, responseParser, hasRequestHeaderTaggedFields, hasResponseHeaderTaggedFields, callback)`

Sends a low-level Kafka protocol request over the connection.

- `apiKey`: Kafka API key.
- `apiVersion`: Kafka API version.
- `createPayload`: function returning a `Writer` with the request payload.
- `responseParser`: function that parses the broker response.
- `hasRequestHeaderTaggedFields`: whether the request header includes tagged fields.
- `hasResponseHeaderTaggedFields`: whether the response header includes tagged fields.
- `callback`: completion callback receiving the parsed response.

#### `reauthenticate()`

Starts SASL re-authentication when the current connection uses SASL and the broker session requires renewal.

#### Events

| Name                           | Payload Type          | Description                                                                            |
| ------------------------------ | --------------------- | -------------------------------------------------------------------------------------- |
| `connecting`                   | (none)                | Emitted when a socket connection attempt starts.                                       |
| `timeout`                      | `TimeoutError`        | Emitted when the connection attempt or request handling times out.                     |
| `error`                        | `Error`               | Emitted when the connection encounters an error.                                       |
| `connect`                      | (none)                | Emitted when the connection is established. It can fire again after re-authentication. |
| `ready`                        | (none)                | Emitted when the connection is ready to send Kafka requests.                           |
| `close`                        | (none)                | Emitted when the underlying socket is closed.                                          |
| `closing`                      | (none)                | Emitted when the connection starts shutting down.                                      |
| `sasl:handshake`               | `string[]`            | Emitted when SASL handshake completes with the list of supported mechanisms.           |
| `sasl:authentication`          | `Buffer \| undefined` | Emitted when SASL authentication completes.                                            |
| `sasl:authentication:extended` | `Buffer \| undefined` | Emitted when SASL authentication is refreshed or extended.                             |
| `drain`                        | (none)                | Emitted when the socket becomes writable again after backpressure.                     |

### `ConnectionPool`

Tracks and reuses `Connection` instances keyed by broker host and port.

It is exposed as the default pool through the [`Base`](./base.md) client `connections` getter and is also used internally by other components.

#### Properties

| Name         | Type                  | Description                                                        |
| ------------ | --------------------- | ------------------------------------------------------------------ |
| `instanceId` | `number`              | Numeric identifier for this pool instance.                         |
| `ownerId`    | `number \| undefined` | Optional owner instance ID of the component that created the pool. |
| `context`    | `unknown`             | Opaque user context forwarded to created connections.              |

#### Methods

#### `[Symbol.iterator]()`

Returns an iterator of `[brokerKey, connection]` entries for the currently pooled connections.

#### `get(broker[, callback])`

Returns an existing connection for the broker or creates a new one.

#### `getFirstAvailable(brokers[, callback])`

Tries the provided brokers in order and returns the first successful connection.

#### `getEstablishedConnection(broker)`

Returns the currently pooled `Connection` for the broker, if present.

#### `has(broker)`

Returns `true` when the pool currently contains a connection for the broker.

#### `isActive()`

Returns `true` when the pool contains at least one connection.

#### `isConnected()`

Returns `true` when the pool contains at least one connection and all pooled connections are connected.

#### `close([callback])`

Closes and removes all pooled connections.

#### Events

| Name                           | Payload Type                                               | Description                                                                                 |
| ------------------------------ | ---------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| `connecting`                   | `ConnectionPoolEventPayload`                               | Emitted when the pool starts opening a connection to a broker.                              |
| `failed`                       | `ConnectionPoolEventPayload`                               | Emitted when a broker connection attempt fails.                                             |
| `connect`                      | `ConnectionPoolEventPayload`                               | Emitted when a broker connection is established.                                            |
| `sasl:handshake`               | `ConnectionPoolEventPayload & { mechanisms: string[] }`    | Emitted when SASL handshake completes and the broker mechanisms are known.                  |
| `sasl:authentication`          | `ConnectionPoolEventPayload & { authentication?: Buffer }` | Emitted when SASL authentication completes.                                                 |
| `sasl:authentication:extended` | `ConnectionPoolEventPayload & { authentication?: Buffer }` | Emitted when SASL authentication is refreshed or extended after the initial authentication. |
| `disconnect`                   | `ConnectionPoolEventPayload`                               | Emitted when a pooled connection closes and is removed from the pool.                       |
| `drain`                        | `ConnectionPoolEventPayload`                               | Emitted when a pooled connection becomes writable again after backpressure.                 |

`ConnectionPoolEventPayload` contains:

- `broker`: the broker `{ host, port }` associated with the event.
- `connection`: the `Connection` instance associated with the event.

## Messages

### `Message<Key, Value, HeaderKey, HeaderValue>`

Represents a message that been consumed from Kafka by using a [`Consumer`](./consumer.md).

The types of the `key`, `value` and `headers` fields are determined by the current serialisation settings of the `Producer` or the `Consumer`.

| Property    | Type                          | Description                                                                                         |
| ----------- | ----------------------------- | --------------------------------------------------------------------------------------------------- |
| `topic`     | `string`                      | The topic of the message.                                                                           |
| `partition` | `number`                      | The topic's partition of the message.                                                               |
| `key`       | `Key`                         | The key of the message.                                                                             |
| `value`     | `Value`                       | The value of the message.                                                                           |
| `timestamp` | `bigint`                      | The timestamp of the message. When producing, it defaults to the current timestamp.                 |
| `headers`   | `Map<HeaderKey, HeaderValue>` | A map with the message headers.                                                                     |
| `offset`    | `bigint`                      | The message offset                                                                                  |
| `commit`    | () => Promise<void>           | A function to commit the offset. This is a no-op if consumer's `autocommit` option was not `false`. |

`Message` also provides a `.toJSON()` method for debugging and logging.

### `MessageToProduce<Key, Value, HeaderKey, HeaderValue>`

Represents a message that is being produced to Kafka via using a [`Producer`](./producer.md).

All fields, except `topic` and `value`, are optional.

The types of the `key`, `value` and `headers` fields are determined by the current serialisation settings of the `Producer` or the `Consumer`.

| Property    | Type                                                              | Description                                                                         |
| ----------- | ----------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| `topic`     | `string`                                                          | The topic of the message.                                                           |
| `partition` | `number`                                                          | The topic's partition of the message.                                               |
| `key`       | `Key`                                                             | The key of the message.                                                             |
| `value`     | `Value`                                                           | The value of the message.                                                           |
| `timestamp` | `bigint`                                                          | The timestamp of the message. When producing, it defaults to the current timestamp. |
| `headers`   | `Map<HeaderKey, HeaderValue>` \| `Record<HeaderKey, HeaderValue>` | A map or plain Javascript object with the message headers.                          |

### `MessagesStream<Key, Value, HeaderKey, HeaderValue>`

It is a Node.js `Readable` stream returned by the [`Consumer`](./consumer.md) `consume` method.

Do not try to create this manually.

The readonly `context` getter exposes the opaque value supplied through `Consumer.consume({ context })` or the consumer `streamContext` default.

#### Properties

| Name               | Type                                           | Description                                             |
| ------------------ | ---------------------------------------------- | ------------------------------------------------------- |
| `consumer`         | `Consumer<Key, Value, HeaderKey, HeaderValue>` | The parent consumer that created the stream.            |
| `connections`      | `ConnectionPool`                               | The fetch-specific connection pool owned by the stream. |
| `context`          | `unknown`                                      | The opaque stream context value.                        |
| `offsetsToFetch`   | `Map<string, bigint>`                          | Internal next offsets keyed as `$topic:$partition`.     |
| `offsetsToCommit`  | `Map<string, CommitOptionsPartition>`          | Offsets queued for commit.                              |
| `offsetsCommitted` | `Map<string, bigint>`                          | Last committed offsets tracked by the stream.           |
| `committedOffsets` | `Map<string, bigint>`                          | Deprecated alias for `offsetsCommitted`.                |

#### Methods

#### `close([callback])`

Closes the stream, stops autocommit timers, flushes pending autocommit work when needed, and closes the stream-owned connection pool.

#### `isActive()`

Returns `true` when the stream is open and its parent consumer is currently active.

#### `isConnected()`

Returns `true` when the stream is open and its parent consumer is currently connected.

#### `pause()`

Pauses message delivery and marks the stream as explicitly paused.

#### `resume()`

Resumes message delivery and restarts the fetch loop when resuming from an explicit pause.

#### Events

| Name         | Payload Type                                                 | Description                                                                                |
| ------------ | ------------------------------------------------------------ | ------------------------------------------------------------------------------------------ |
| `fetch`      | (none)                                                       | Emitted each time the stream schedules or completes a fetch cycle.                         |
| `autocommit` | `(error: Error \| null, offsets?: CommitOptionsPartition[])` | Emitted after an automatic commit attempt, with either the error or the committed offsets. |
| `offsets`    | (none)                                                       | Emitted after the stream refreshes its internal fetch offsets.                             |

### `ProducerStream<Key, Value, HeaderKey, HeaderValue>`

It is a Node.js `Writable` stream returned by the [`Producer`](./producer.md) `asStream` method.

Do not try to create this manually.

It operates in object mode and batches writes before forwarding them to the parent producer.

#### Properties

| Name       | Type                                           | Description                                  |
| ---------- | ---------------------------------------------- | -------------------------------------------- |
| `producer` | `Producer<Key, Value, HeaderKey, HeaderValue>` | The parent producer that created the stream. |
| `instance` | `number`                                       | Numeric identifier for this producer stream. |

#### Methods

#### `close([callback])`

Ends the writable stream, flushes any buffered messages, and resolves when shutdown completes.

#### Events

| Name              | Payload Type                                                                              | Description                                                                                                                                     |
| ----------------- | ----------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `flush`           | `{ batchId: number, count: number, duration: number, result: ProduceResult }`             | Emitted after a buffered batch has been sent.                                                                                                   |
| `delivery-report` | `ProducerStreamReport \| ProducerStreamMessageReport<Key, Value, HeaderKey, HeaderValue>` | Emitted when delivery reporting is enabled. In `BATCH` mode it reports one event per batch; in `MESSAGE` mode it reports one event per message. |

## `ClusterMetadata`

Metadata about the Kafka cluster. It is returned by the [`Base`](./base.md) client, which is the base class for the `Producer`, `Consumer` and `Admin` clients.

| Property     | Type                                | Description                                                                                            |
| ------------ | ----------------------------------- | ------------------------------------------------------------------------------------------------------ |
| `id`         | `string`                            | Cluster ID                                                                                             |
| `brokers`    | `Map<number, Broker>`               | Map of brokers. The keys are node IDs, while the values are objects with `host` and `port` properties. |
| `topics`     | `Map<string, ClusterTopicMetadata>` | Map of topics. The keys are the topics, while the values contain partition information.                |
| `lastUpdate` | `number`                            | Timestamp of the metadata                                                                              |

## Utilities

### `debugDump(...values)`

Debug/logger utility to inspect any object.

```typescript
import { debugDump } from '@platformatic/kafka'

debugDump('received-message', message)
```

## Serialisation and Deserialisation

### stringSerializer and stringDeserializer

Courtesy string serialisers implementing `Serializer<string>` and `Deserialier<string>`.

### jsonSerializer and jsonDeserializer

Courtesy JSON serialisers implementing `Serializer<T = object>` and `Deserializer<T = object>`.

### stringSerializers and stringDeserializers

Courtesy serializers and deserializers objects using `stringSerializer` or `stringDeserializer` ready to be used in `Producer` or `Consumer`.

### serializersFrom and deserializersFrom

Courtesy methods to create a `Serializers<T, T, T, T>` out of a single `Serializer<T>` or a `Deserializers<T, T, T, T>` out of a single `Deserializer<T>`.

For instance, the following two snippets are equivalent:

```typescript
import { Producer } from '@platformatic/kafka'

function serialize (source: YourType): Buffer {
  return Buffer.from(JSON.stringify(source))
}

const producer = new Producer({
  clientId: 'my-producer',
  bootstrapBrokers: ['localhost:9092'],
  serializers: {
    key: serialize,
    value: serialize,
    headerKey: serialize,
    headerValue: serialize
  }
})
```

```typescript
import { Producer, serializersFrom } from '@platformatic/kafka'

function serialize (source: YourType): Buffer {
  return Buffer.from(JSON.stringify(source))
}

const producer = new Producer({
  clientId: 'my-producer',
  bootstrapBrokers: ['localhost:9092'],
  serializers: serializersFrom(serialize)
})
```
