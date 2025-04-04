# Base

This is the base class for all other clients ([`Producer`](./producer.md), [`Consumer`](./consumer.md) and [`Admin`](./admin.md)).

Unless you only care about cluster metadata, it is unlikely that you ever initialize an instance of this.

## Events

| Name                       | Description                                                 |
| -------------------------- | ----------------------------------------------------------- |
| `client:broker:connect`    | Emitted when connecting to a broker.                        |
| `client:broker:disconnect` | Emitted when disconnecting from a broker.                   |
| `client:broker:failed`     | Emitted when a broker connection fails.                     |
| `client:broker:drain`      | Emitted when a broker is ready to be triggered by requests. |
| `client:metadata`          | Emitted when metadata is retrieved.                         |

## Constructor

Creates a new base client.

| Property           | Type                   | Default   | Description                                                                                                                                |
| ------------------ | ---------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `clientId`         | `string`               |           | Client ID.                                                                                                                                 |
| `bootstrapBrokers` | `(Broker \| string)[]` |           | Bootstrap brokers.<br/><br/>Each broker can be either an object with `host` and `port` properties or a string in the format `$host:$port`. |
| `timeout`          | `number`               | 5 seconds | Timeout in milliseconds for Kafka request that support the parameter.                                                                      |
| `retries`          | `number`               | `3`       | Number of times to retry an operation before failing.                                                                                      |
| `retryDelay`       | `number`               | `250`     | Amount of time in milliseconds to wait between retries.                                                                                    |
| `metadataMaxAge`   | `number`               | 5 minutes | Maximum lifetime of cluster metadata.                                                                                                      |
| `autocreateTopics` | `boolean`              | `false`   | If to autocreate missing topics during metadata retrieval.                                                                                 |
| `strict`           | `boolean`              | `false`   | If to validate all user provided options on each request.<br/><br/>This will impact performance so we recommend to disable in production.  |

## Methods

### `metadata(options[, callback])`

Fetches information about the cluster and the topics.

The return value is a [`ClusterMetadata`](./other.md#clustermetadata) object.

| Property           | Type       | Default   | Description                                                           |
| ------------------ | ---------- | --------- | --------------------------------------------------------------------- |
| `topics`           | `string[]` |           | Topics to get.                                                        |
| `forceUpdate`      | `boolean`  | `false`   | If to retrieve metadata even if the in-memory cached are still valid. |
| `autocreateTopics` | `boolean`  | `false`   | If to autocreate missing topics.                                      |
| `metadataMaxAge`   | `number`   | 5 minutes | Maximum lifetime of cluster metadata.                                 |

### `close([callback])`

Closes the client and all its connections.

The return value is `void`.
