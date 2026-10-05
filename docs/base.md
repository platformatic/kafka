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

| Property             | Type                   | Default   | Description                                                                                                                                                        |
| -------------------- | ---------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `clientId`           | `string`               |           | Client ID.                                                                                                                                                         |
| `clientRack`         | `string`               |           | Client rack identifier sent as Fetch `rack_id` from v11 and ConsumerGroupHeartbeat `rack_id` from v0.                                                             |
| `bootstrapBrokers`   | `(Broker \| string)[]` |           | Bootstrap brokers.<br/><br/>Each broker can be either an object with `host` and `port` properties or a string in the format `$host:$port`.                         |
| `timeout`            | `number`               | 5 seconds | Timeout in milliseconds for Kafka requests that support the parameter.                                                                                             |
| `retries`            | `number` \| `boolean`  | `3`       | Number of times to retry an operation before failing. `true` means "infinity", while `false` means 0                                                               |
| `retryDelay`         | `number` \| `function` | `1000`    | Amount of time in milliseconds to wait between retries, or a function to calculate custom delays. See section below.                                               |
| `metadataMaxAge`     | `number`               | 5 seconds | Maximum lifetime of cluster metadata.                                                                                                                              |
| `autocreateTopics`   | `boolean`              | `false`   | Whether to autocreate missing topics during metadata retrieval.                                                                                                    |
| `strict`             | `boolean`              | `false`   | Whether to validate all user-provided options on each request.<br/><br/>This will impact performance so we recommend disabling it in production.                   |
| `metrics`            | object                 |           | A Prometheus configuration. See the [Metrics section](./metrics.md) for more information.                                                                          |
| `connectTimeout`     | `number`               | `5000`    | Client connection timeout.                                                                                                                                         |
| `requestTimeout`     | `number`               | `30000`   | Local timeout in milliseconds while waiting for a response to an in-flight request.                                                                                |
| `maxInflights`       | `number`               | `5`       | Amount of request to send in parallel to Kafka without awaiting for responses, when allowed from the protocol.                                                     |
| `handleBackPressure` | `boolean`              | `false`   | If set to `true`, the client will respect the return value of [`socket.write`][node-socket-write] and wait for a `drain` even before resuming sending of requests. |
| `tls`                | `TLSConnectionOptions` |           | Configures TLS for broker connections. See section below.                                                                                                          |
| `ssl`                | `TLSConnectionOptions` |           | Alias for `tls`. Configures TLS for broker connections. See section below. If both are provided, `tls` overrides this.                                             |
| `tlsServerName`      | `boolean` \| `string`  | `true`    | TLS SNI defaults to each target broker's hostname, excluding IP addresses. Set to `false` to disable inference, or a string to use a fixed name.                   |
| `sasl`               | `SASLOptions`          |           | Configures SASL authentication. See section below.                                                                                                                 |
| `context`            | `unknown`              |           | Opaque user data forwarded to internally created `ConnectionPool` and `Connection` instances. Kafka never reads, mutates, or interprets this value.                |

IPv6 bootstrap hosts must be wrapped in brackets, e.g. `[::1]:9092` or `[2001:db8::1]:9092`. Unbracketed IPv6 addresses are treated as host-only and use the default port.

The readonly `context` getter exposes the same opaque value on the client instance.

The readonly `currentMetadata` getter exposes the currently cached [`ClusterMetadata`](./other.md#clustermetadata), or `undefined` if metadata has not been loaded yet. It does not fetch or refresh metadata.

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

### `connections`

Returns the client's default broker connection pool.

### `clearMetadata`

Clear the current metadata.

## Custom Retry Delay Function

The `retryDelay` option can accept either a number (for fixed delay) or a function for dynamic retry delays. This allows you to implement custom retry strategies such as exponential backoff, jitter, or any other retry pattern.

When using a function, it receives the following parameters:

| Parameter     | Type     | Description                                         |
| ------------- | -------- | --------------------------------------------------- |
| `client`      | `object` | The client instance performing the retry            |
| `operationId` | `string` | A unique identifier for the operation being retried |
| `attempt`     | `number` | The current attempt number (starts from 1)          |
| `retries`     | `number` | The maximum number of retries configured            |
| `error`       | `Error`  | The error that caused the retry                     |

The function must return a number representing the delay in milliseconds before the next retry attempt.

## `timeout` vs `requestTimeout`

Both options are valid and control different things:

- `timeout`: request-level Kafka timeout that is sent to broker APIs that expose a timeout field.
- `requestTimeout`: client-side timeout that limits how long this client waits for a response on an in-flight request.

In practice, `requestTimeout` is the guard for `TimeoutError: Request timed out` coming from the connection layer.

## Connecting to Kafka via TLS connection

To connect to a Kafka via TLS connection, simply pass all relevant options in the `tls` options when creating any subclass of `Base`.
SNI is enabled automatically for DNS hostnames, including brokers discovered after bootstrap, but not for IP addresses.
An explicit `tls.servername` takes precedence over automatic inference; a string `tlsServerName` overrides it.
Set `tlsServerName: false` to disable automatic inference without removing an explicit `tls.servername`.
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

### TLS Server Name Indication (SNI)

Setting `tls: {}` enables TLS with Node.js defaults, but does not automatically enable Server Name Indication (SNI).
Node.js `tls.connect()` does not send SNI unless a `servername` is provided. Kafka deployments whose TLS endpoints
require SNI, for example for routing through a TLS proxy, can close connections when it is missing. This behavior
is not specific to a Kafka provider.

Set `tlsServerName: true` alongside your TLS options to send the current target broker's hostname:

```javascript
tls: {},
tlsServerName: true
```

This applies both to bootstrap connections and to connections to brokers discovered through Kafka metadata.
Prefer `true` when SNI should follow the hostname of each broker, rather than
always using the bootstrap hostname. A string sets a fixed SNI name for all broker connections; alternatively,
`tls.servername` can provide a fixed name directly. A truthy `tlsServerName` overrides `tls.servername`.

Keep any existing TLS options when adding `tlsServerName`. Do not disable certificate verification to fix missing SNI.
See [Troubleshooting](./troubleshooting.md#connection-errors-during-tls-or-authentication-setup) for the associated error symptoms and a complete configuration example.

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
    // username, password, token and oauthBearerExtensions can also be (async) functions returning a value
    username: 'username', // This is used from PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512
    password: 'password', // This is used from PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512
    token: 'token', // This is used from OAUTHBEARER
    oauthBearerExtensions: {}, // This is used from OAUTHBEARER to add extension according to RFC 7628
    // This is needed if your Kafka server returns a exitCode 0 when invalid credentials are sent and only stores
    // authentication information in auth bytes.
    //
    // A good example for this is OAuthBearerValidatorCallbackHandler, for which we provide saslOAuthBearer.jwtValidateAuthenticationBytes.
    authBytesValidator: (_authBytes, cb) => cb(null)
  }
})
```

### `SASLOptions`

| Property                | Type                                                                   | Default | Description                                                                                                   |
| ----------------------- | ---------------------------------------------------------------------- | ------- | ------------------------------------------------------------------------------------------------------------- |
| `mechanism`             | `SASLMechanismValue`                                                   |         | SASL mechanism: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER`, or `GSSAPI`.                        |
| `username`              | `string \| CredentialProvider`                                         |         | Username or a function that provides one for PLAIN or SCRAM authentication.                                   |
| `password`              | `string \| CredentialProvider`                                         |         | Password or a function that provides one for PLAIN or SCRAM authentication.                                   |
| `token`                 | `string \| CredentialProvider`                                         |         | Token or a function that provides one for OAUTHBEARER authentication.                                         |
| `oauthBearerExtensions` | `Record<string, string> \| CredentialProvider<Record<string, string>>` |         | Extensions or a function that provides them for OAUTHBEARER authentication.                                   |
| `authenticate`          | `SASLCustomAuthenticator`                                              |         | Custom authentication function; required for GSSAPI. See the section below.                                   |
| `authBytesValidator`    | `(authBytes: Buffer, callback: CallbackWithPromise<Buffer>) => void`   |         | Validates the authentication bytes returned by the broker.                                                    |
| `reauthFraction`        | `number`                                                               | `0.8`   | Fraction of the broker session lifetime at which reauthentication becomes due; greater than `0`, at most `1`. |
| `reauthLeadTime`        | `number`                                                               | `0`     | Milliseconds before session expiration at which reauthentication becomes due, if earlier.                     |
| `lazyReauthentication`  | `boolean`                                                              | `false` | If enabled, reauthenticate on the next request after the timer expires instead of while idle.                 |

SASL reauthentication runs automatically at 80% of the session lifetime reported by the broker. Set
`sasl.reauthFraction` to change that fraction (greater than `0` and at most `1`). Set `sasl.reauthLeadTime`
to move the timer earlier when fewer than that many milliseconds would remain before expiration.
The timer uses the earlier of the fractional time and the lifetime minus the lead time. The lead time
defaults to `0`, so existing reauthentication timing is unchanged. If subtracting the lead time from
the session lifetime gives zero or a negative value, the lead time is ignored and the fractional time
is used instead to avoid an immediate reauthentication loop. A
broker-reported session lifetime of `0` disables automatic reauthentication.
Set `sasl.lazyReauthentication: true` to wait until the next request after the timer expires
before reauthenticating. Concurrent requests wait for the same reauthentication. By default,
the timer continues to reauthenticate idle connections as before.

## Connecting to Kafka via SASL using a custom authenticator

For advanced use cases where you need full control over the SASL authentication process, you can provide a custom `authenticate` function in the `sasl` options. This allows you to implement custom authentication flows, handle complex credential management, or integrate with external authentication systems.

Example:

```javascript
import { Producer, stringSerializers } from '@platformatic/kafka'

const producer = new Producer({
  clientId: 'my-producer',
  bootstrapBrokers: ['localhost:9092'],
  serializers: stringSerializers,
  sasl: {
    mechanism: 'PLAIN',
    authenticate: async (
      mechanism,
      connection,
      authenticate,
      usernameProvider,
      passwordProvider,
      tokenProvider,
      callback
    ) => {
      try {
        // Custom logic to retrieve or generate credentials
        const username = typeof usernameProvider === 'function' ? await usernameProvider() : usernameProvider
        const password = typeof passwordProvider === 'function' ? await passwordProvider() : passwordProvider

        // Perform the SASL authentication
        const authData = Buffer.from(`\u0000${username}\u0000${password}`)
        const response = await authenticate({
          authBytes: authData
        })

        callback(null, response)
      } catch (err) {
        callback(err)
      }
    }
  }
})
```

The `authenticate` function receives the following parameters:

- `mechanism`: The SASL mechanism being used (e.g., 'PLAIN', 'SCRAM-SHA-256')
- `connection`: The broker connection being authenticated
- `authenticate`: The SASL authentication API function to send auth bytes to the server
- `usernameProvider`: The username (string or async function) from the sasl options
- `passwordProvider`: The password (string or async function) from the sasl options
- `tokenProvider`: The token (string or async function) from the sasl options
- `callback`: A callback function to call with the authentication result

**Important**: The `authenticate` function should never throw exceptions, especially when using async functions. The function is not awaited and exceptions are not handled, which can lead to memory leaks, resource leaks, and unexpected behavior. Always wrap your code in a try-catch block and pass errors to the callback instead.

[node-socket-write]: https://nodejs.org/dist/latest/docs/api/stream.html#writablewritechunk-encoding-callback
