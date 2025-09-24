# Base

This is the base class for all other clients ([`Producer`](./producer.md), [`Consumer`](./consumer.md) and [`Admin`](./admin.md)).

Unless you only care about cluster metadata, it is unlikely that you would ever initialise an instance of this.

## Events

| Name                                         | Description                                                                                  |
| -------------------------------------------- | -------------------------------------------------------------------------------------------- |
| `client:broker:connect`                      | Emitted when connecting to a broker.                                                         |
| `client:broker:disconnect`                   | Emitted when disconnecting from a broker.                                                    |
| `client:broker:failed`                       | Emitted when a broker connection fails.                                                      |
| `client:broker:drain`                        | Emitted when a broker is ready to be triggered by requests.                                  |
| `client:broker:sasl:handshake`               | Emitted when SASL handshake with a broker is completed.                                      |
| `client:broker:sasl:authentication`          | Emitted when SASL authentication to a broker is completed.                                   |
| `client:broker:sasl:authentication:extended` | Emitted when SASL authentication to a broker is extended by performing a new authentication. |
| `client:metadata`                            | Emitted when metadata is retrieved.                                                          |
| `client:close`                               | Emitted when client is closed.                                                               |

## Constructor

Creates a new base client.

| Property           | Type                   | Default   | Description                                                                                                                                      |
| ------------------ | ---------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `clientId`         | `string`               |           | Client ID.                                                                                                                                       |
| `bootstrapBrokers` | `(Broker \| string)[]` |           | Bootstrap brokers.<br/><br/>Each broker can be either an object with `host` and `port` properties or a string in the format `$host:$port`.       |
| `timeout`          | `number`               | 5 seconds | Timeout in milliseconds for Kafka requests that support the parameter.                                                                           |
| `retries`          | `number` \| `boolean`  | `3`       | Number of times to retry an operation before failing. `true` means "infinity", while `false` means 0                                             |
| `retryDelay`       | `number`               | `250`     | Amount of time in milliseconds to wait between retries.                                                                                          |
| `metadataMaxAge`   | `number`               | 5 minutes | Maximum lifetime of cluster metadata.                                                                                                            |
| `autocreateTopics` | `boolean`              | `false`   | Whether to autocreate missing topics during metadata retrieval.                                                                                  |
| `strict`           | `boolean`              | `false`   | Whether to validate all user-provided options on each request.<br/><br/>This will impact performance so we recommend disabling it in production. |
| `metrics`          | object                 |           | A Prometheus configuration. See the [Metrics section](./metrics.md) for more information.                                                        |
| `connectTimeout`   | `number`               | `5000`    | Client connection timeout.                                                                                                                       |
| `maxInflights`     | `number`               | `5`       | Amount of request to send in parallel to Kafka without awaiting for responses, when allowed from the protocol.                                   |
| `tls`              | `TLSConnectionOptions` |           | Configures TLS for broker connections. See section below.                                                                                        |
| `tlsServerName`    | `boolean` \| `string`  |           | A TLS servername to use when connecting. When set to `true` it will use the current target host.                                                 |
| `sasl`             | `SASLOptions`          |           | Configures SASL authentication. See section below.                                                                                               |

## Methods

### `connectToBrokers([nodeIds][, callback])`

Establish a connection to one or more brokers in the cluster.

The return value is a `Map<number, Connection>` object.

| Property | Type               | Default | Description                                                                                               |
| -------- | ------------------ | ------- | --------------------------------------------------------------------------------------------------------- |
| `nodes`  | `number[] \| null` | null    | The nodes to connect to. Valid IDs can be obtained via the `metadata` method and invalid IDs are ignored. |

### `metadata(options[, callback])`

Fetches information about the cluster and the topics.

The return value is a [`ClusterMetadata`](./other.md#clustermetadata) object.

| Property           | Type       | Default   | Description                                                              |
| ------------------ | ---------- | --------- | ------------------------------------------------------------------------ |
| `topics`           | `string[]` |           | Topics to get.                                                           |
| `forceUpdate`      | `boolean`  | `false`   | Whether to retrieve metadata even if the in-memory cache is still valid. |
| `autocreateTopics` | `boolean`  | `false`   | Whether to autocreate missing topics.                                    |
| `metadataMaxAge`   | `number`   | 5 seconds | Maximum lifetime of cluster metadata.                                    |

### `close([callback])`

Closes the client and all its connections.

The return value is `void`.

### `isActive`

Returns `true` if the client is not closed.

### `isConnected`

Returns `true` if all client's connections are currently connected and the client is connected to at least one broker.

## Connecting to Kafka via TLS connection

To connect to a Kafka via TLS connection, simply pass all relevant options in the `tls` options when creating any subclass of `Base`.
Example:

```javascript
import { readFile } from 'node:fs/promises'
import { Producer, stringSerializers } from '@platformatic/kafka'

const producer = new Producer({
  clientId: 'my-producer',
  bootstrapBrokers: ['localhost:9092'],
  serializers: stringSerializers,
  tls: {
    rejectUnauthorized: false,
    cert: await readFile(resolve(import.meta.dirname, './ssl/client.pem')),
    key: await readFile(resolve(import.meta.dirname, './ssl/client.key'))
  }
})
```

## Connecting to Kafka via SASL

To connect to a Kafka via SASL authentication, simply pass all relevant options in the `sasl` options when creating any subclass of `Base`.
Example:

```javascript
import { readFile } from 'node:fs/promises'
import { Producer, stringSerializers } from '@platformatic/kafka'

const producer = new Producer({
  clientId: 'my-producer',
  bootstrapBrokers: ['localhost:9092'],
  serializers: stringSerializers,
  sasl: {
    mechanism: 'PLAIN', // Also SCRAM-SHA-256, SCRAM-SHA-512 and OAUTHBEARER are supported
    // username, password or token can also be (async) functions returning a string
    username: 'username', // This is used from PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512
    password: 'password', // This is used from PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512
    token: 'token', // This is used from OAUTHBEARER
    // This is needed if your Kafka server returns a exitCode 0 when invalid credentials are sent and only stores
    // authentication information in auth bytes.
    //
    // A good example for this is OAuthBearerValidatorCallbackHandler, for which we provide saslOAuthBearer.jwtValidateAuthenticationBytes.
    authBytesValidator: (_authBytes, cb) => cb(null)
  }
})
```
