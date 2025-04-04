# Producer

Client for producing messages to Kafka topics with support for idempotent production.

The producer inherits from the [`Base`](./base.md) client.

The complete TypeScript type of the `Producer` is determined by the `serializers` option.

The producer supports idempotent prdocution.

## Constructor

Creates a new producer with type `Producer<Key, Value, HeaderKey, HeaderValue>`.

Options:

| Property                | Type                                                               | Description                                                                                                                                                          |
| ----------------------- | ------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `producerId`            | `bigint`                                                           | Producer ID.                                                                                                                                                         |
| `producerEpoch`         | `number`                                                           | Producer epoch.                                                                                                                                                      |
| `idempotent`            | `boolean`                                                          | Idempotency of the producer.                                                                                                                                         |
| `acks`                  | `number`                                                           | Acknowledgement to wait before returning.<br/><br/>Valid values are be defined in the `ProduceAcks` enumeration.                                                     |
| `compression`           | `string`                                                           | Compression algorithm to use before sending messages to the broker.<br/><br/>Valid values are exported in the `CompressionAlgorithms` enumeration.                   |
| `partitioner`           | `(message: Message<Key, Value, HeaderKey, HeaderValue>) => number` | Partitioner to use to assign a partition to messages that lack it.<br/><br/>It is a function that receives a message and should return the partition number.         |
| `repeatOnStaleMetadata` | `boolean`                                                          | If to retry a produce operation when the system detects outdated topic or broker informations.<br/><br/>Default is `true`.                                           |
| `serializers`           | `Serializers<Key, Value, HeaderKey, HeaderValue>`                  | Object that specifies which serializers to use.<br/><br/>The object should only contain one or more of the `key`, `value`, `headerKey` and `headerValue` properties. |

It also supports all the constructor options of `Base`.

## Basic Methods

### `send<Key, Value, HeaderKey, HeaderValue>(options[, callback])`

Sends one or more messages to Kafka.

When `acks` is not `ProduceAcks.NO_RESPONSE`, then the return value is an object with the property `offsets` containing a list of written topic-partition-offset triplet.

When `acks` is `ProduceAcks.NO_RESPONSE`, then the return value is an object with the property `unwritableNodes` containing a list nodes which are currently busy and should wait for a `client:broker:drain` event before continuing.

Options:

| Property   | Type                                            | Description           |
| ---------- | ----------------------------------------------- | --------------------- |
| `messages` | `Message<Key, Value, HeaderKey, HeaderValue>[]` | The messages to send. |

It also accepts all options of the constructor except `serializers`.

### `close([callback])`

Closes the producer and all its connections.

The return value is `void`.

## Advanced Methods

The producer manages auxiliary operations automatically. Some of the APIs are exposed to allow for advanced uses.

### `initIdempotentProducer<Key, Value, HeaderKey, HeaderValue>(options[, callback])`

Initializes an idempotent producer. It accepts all options of the constructor except `serializers`.

The return value is an object containing the `producerId` and `producerEpoch` values returned from the broker.
