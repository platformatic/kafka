# Other APIs and types

## Messages

### `Message<Key, Value, HeaderKey, HeaderValue>`

Represents a message that can either:

- Being produced to Kafka by using a [`Producer`](./producer.md). All fields, except `topic` and `value`, are optional.
- Has been consumed from Kafka by using a [`Consumer`](./consumer.md).

The type of the `key`, `value` and `headers` fields are determined by the current serialization settings of the `Producer` or the `Consumer`.

| Property    | Type                          | Description                                                                        |
| ----------- | ----------------------------- | ---------------------------------------------------------------------------------- |
| `topic`     | `string`                      | The topic of the message.                                                          |
| `partition` | `number`                      | The topic's partition of the message.                                              |
| `key`       | `Key`                         | The key of the message.                                                            |
| `value`     | `Value`                       | The value of the message.                                                          |
| `timestamp` | `bigint`                      | The timestamp of the message. When producing, it default to the current timestamp. |
| `headers`   | `Map<HeaderKey, HeaderValue>` | A map with the message headers.                                                    |

### `MessageStream<Key, Value, HeaderKey, HeaderValue>`

It is a Node.js `Readable` stream returned by the [`Consumer`](./consumer.md) `consume` method.

Do not try to create this manually.

## `ClusterMetadata`

Metadata about the Kafka cluster. It is returned by [`Base`](./base.md) client, which is the base class the `Producer`, `Consumer` and `Admin` clients.

| Property     | Type                                | Description                                                                                           |
| ------------ | ----------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `id`         | `string`                            | Cluster ID                                                                                            |
| `brokers`    | `Map<number, Broker>`               | Map of brokers. The keys are nodes ID, while the values are object with `host` and `port` properties. |
| `topics`     | `Map<string, ClusterTopicMetadata>` | Map of topics. The keys are the topics, while the values contains partitions informations.            |
| `lastUpdate` | `number`                            | Timestamp of the metadata                                                                             |

## Serialization and Deserialization

### stringSerializer and stringDeserializer

Courtesy string serializers implementing `Serializer<string>` and `Deserializer<string>`.

### jsonSerializer and jsonDeserializer

Courtesy JSON serializers implementing `Serializer<T = object>` and `Deserializer<T = object>`.

### stringSerializers and stringDeserializers

Courtesy serializers and deserializers object using `stringSerializer` or `destringSerializer` ready to be used in `Producer` or `Consumer`.

### serializersFrom and deserializersFrom

Courtesy methods to create a `Serializers<T, T, T, T>` out of a single `Serializer<T>` or a `Deserializers<T, T, T, T>` out of a single `Deserializer<T>`.

For instance, the following two snippets are equivalent:

```typescript
import { Producer } from '@platformatic/kafka'

function serialize(source: YourType): Buffer {
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

function serialize(source: YourType): Buffer {
  return Buffer.from(JSON.stringify(source))
}

const producer = new Producer({
  clientId: 'my-producer',
  bootstrapBrokers: ['localhost:9092'],
  serializers: serializersFrom(serialize)
})
```
