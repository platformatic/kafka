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

### stringSerializer and destringSerializer

Courtesy string serializers implementing `Serializer<string>` and `Deserializer<string>`.

### stringSerializers and destringSerializers

Courtesy serializers and deserializers object using `stringSerializer` or `destringSerializer` ready to be used in `Producer` or `Consumer`.

                                                                        |
