# Base

This is the base class for all other clients ([`Producer`](./producer.md), [`Consumer`](./consumer.md) and [`Admin`](./admin.md)).

Unless you only care about cluster metadata, it is unlikely that you would ever initialise an instance of this.

## Events

| Name                                         | Description                                                                                  |
| -------------------------------------------- | -------------------------------------------------------------------------------------------- |
| `client:broker:connect`                      | Emitted when connecting to a broker.                                                         |
| `client:broker:disconnect`                   | Emitted when disconnecting from a broker. `reason` is `'idle'` for an idle close.            |
| `client:broker:failed`                       | Emitted when a broker connection fails.                                                      |
| `client:broker:drain`                        | Emitted when a broker is ready to be triggered by requests.                                  |
| `client:broker:sasl:handshake`               | Emitted when SASL handshake with a broker is completed.                                      |
| `client:broker:sasl:authentication`          | Emitted when SASL authentication to a broker is completed.                                   |
| `client:broker:sasl:authentication:extended` | Emitted when SASL authentication to a broker is extended by performing a new authentication. |
| `client:metadata`                            | Emitted when metadata is retrieved.                                                          |
| `client:performWithRetry:retry`              | Emitted before a retry with `(operationId, attempt, retries, delay, broker?)`.                |
| `client:close`                               | Emitted when client is closed.                                                               |

The retry event includes `broker` only when the direct error identifies a broker. It does not look inside aggregates
of failures from multiple brokers. The address belongs to the failed attempt, not the next one.

## Connection errors

`NetworkError` and `TimeoutError` from a connection include `broker: { host, port }` when the endpoint is known.
Request timeouts also include the request's `correlationId`. When several errors are grouped in `MultipleErrors`,
look for `broker` on the individual errors, not on the aggregate.

A `ResponseError` decoded from a Kafka response includes the address of the broker that sent the response, even if
the response contains errors for several partitions.

## Constructor

Creates a new base client.

| Property                | Type                   | Default   | Description                                                                                                                                                        |
| ----------------------- | ---------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `clientId`              | `string`               |           | Client ID.                                                                                                                                                         |
| `clientRack`            | `string`               |           | Client rack identifier sent as Fetch `rack_id` from v11 and ConsumerGroupHeartbeat `rack_id` from v0.                                                              |
| `bootstrapBrokers`      | `(Broker \| string)[]` |           | Bootstrap brokers.<br/><br/>Each broker can be either an object with `host` and `port` properties or a string in the format `$host:$port`.                         |
| `timeout`               | `number`               | 5 seconds | Timeout in milliseconds for Kafka requests that support the parameter.                                                                                             |
| `retries`               | `number` \| `boolean`  | `3`       | Number of times to retry an operation before failing. `true` means "infinity", while `false` means 0                                                               |
| `retryDelay`            | `number` \| `function` | `1000`    | Amount of time in milliseconds to wait between retries, or a function to calculate custom delays. See section below.                                               |
| `apiVersions`           | `Record`               |           | Fixed versions or synchronous version selectors, keyed by numeric Kafka API ID. See below.                                                                        |
| `maxRetryErrors`        | `number`               | `10`      | Maximum number of errors kept while retrying an operation. The first error and the most recent ones are kept.                                                      |
| `metadataMaxAge`        | `number`               | 5 seconds | Maximum lifetime of cluster metadata.                                                                                                                              |
| `knownBrokersMaxAge`    | `number`               | 1 hour    | How long a broker missing from metadata responses is kept as a connection fallback. `0` drops it at the next metadata update.                                      |
| `maxAbsentKnownBrokers` | `number`               | `10`      | Maximum number of brokers missing from the latest metadata response kept as connection fallbacks, keeping the most recently seen. `0` disables the fallbacks.      |
| `autocreateTopics`      | `boolean`              | `false`   | Whether to autocreate missing topics during metadata retrieval.                                                                                                    |
| `strict`                | `boolean`              | `false`   | Whether to validate all user-provided options on each request.<br/><br/>This will impact performance so we recommend disabling it in production.                   |
| `metrics`               | object                 |           | A Prometheus configuration. See the [Metrics section](./metrics.md) for more information.                                                                          |
| `connectTimeout`        | `number`               | `5000`    | Client connection timeout.                                                                                                                                         |
| `requestTimeout`        | `number`               | `30000`   | Local timeout in milliseconds while waiting for a response to an in-flight request.                                                                                |
| `maxInflights`          | `number`               | `5`       | Amount of request to send in parallel to Kafka without awaiting for responses, when allowed from the protocol.                                                     |
| `keepAlive`             | `KeepAliveOptions`     |           | Enables TCP keep-alive on broker connections. See section below.                                                                                                   |
| `connectionsMaxIdle`    | `number`               |           | Close a broker connection that has had no traffic and no pending requests for this many milliseconds. The next request reconnects. `0` or unset disables it.       |
| `handleBackPressure`    | `boolean`              | `false`   | If set to `true`, the client will respect the return value of [`socket.write`][node-socket-write] and wait for a `drain` even before resuming sending of requests. |
| `tls`                   | `TLSConnectionOptions` |           | Configures TLS for broker connections. See section below.                                                                                                          |
| `ssl`                   | `TLSConnectionOptions` |           | Alias for `tls`. Configures TLS for broker connections. See section below. If both are provided, `tls` overrides this.                                             |
| `tlsServerName`         | `boolean` \| `string`  | `true`    | TLS SNI defaults to each target broker's hostname, excluding IP addresses. Set to `false` to disable inference, or a string to use a fixed name.                   |
| `socketFactory`         | `function`             |           | Opens the socket to each broker instead of the client. See section below.                                                                                          |
| `sasl`                  | `SASLOptions`          |           | Configures SASL authentication. See section below.                                                                                                                 |
| `context`               | `unknown`              |           | Opaque user data forwarded to internally created `ConnectionPool` and `Connection` instances. Kafka never reads, mutates, or interprets this value.                |

IPv6 bootstrap hosts must be wrapped in brackets, e.g. `[::1]:9092` or `[2001:db8::1]:9092`. Unbracketed IPv6 addresses are treated as host-only and use the default port.

The readonly `context` getter exposes the same opaque value on the client instance.

The readonly `currentMetadata` getter exposes the currently cached [`ClusterMetadata`](./other.md#clustermetadata), or `undefined` if metadata has not been loaded yet. It does not fetch or refresh metadata.

### API version overrides

`apiVersions` is available on all clients. Use an exported API ID constant as a computed key:

```ts
import { FetchId } from '@platformatic/kafka'

const apiVersions = { [FetchId]: 11 }
```

A number pins the API to that exact version. A synchronous selector can instead choose a version each time the client resolves the API. It receives:

- `name` and `id`: the Kafka API name and numeric ID.
- `client` and `broker`: the respective version ranges, each with `minVersion` and `maxVersion`.
- `candidateVersion`: the lower of the client and broker maximum versions. It is the highest common version when the ranges overlap.
- `metadata`: the cached cluster metadata, or `undefined` if none is cached. Selection does not trigger a metadata refresh.

Return `undefined` to keep the default. For example, select Fetch v11 when the broker reports a zero UUID and the default Fetch version requires IDs:

```ts
import { FetchId, type BaseOptions } from '@platformatic/kafka'

const apiVersions: BaseOptions['apiVersions'] = {
  [FetchId]: ({ candidateVersion, client, broker, metadata }) => {
    if (candidateVersion > 12 && Math.max(client.minVersion, broker.minVersion) <= 11 && metadata) {
      for (const { id } of metadata.topics.values()) {
        if (id === '00000000-0000-0000-0000-000000000000') {
          return 11
        }
      }
    }

    return undefined
  }
}
```

The selected version must be an integer within both ranges; otherwise the client returns `PLT_KFK_UNSUPPORTED_API`. The selector must return a number or `undefined` synchronously. If it throws, the client returns `PLT_KFK_USER` with the original error as its cause. Overrides apply to negotiated requests and do not change the initial `ApiVersions` handshake.

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
| `maxRetryErrors`   | `number`   | `10`      | Overrides the client `maxRetryErrors` option for this call.              |

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

Clear the current metadata. Brokers seen in earlier metadata responses are still used as connection fallbacks.

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

## TCP keep-alive

Setting `keepAlive` enables TCP keep-alive on every broker connection, plaintext or TLS. Each key maps to the matching argument of Node.js [`socket.setKeepAlive()`][node-socket-set-keep-alive]:

| Property       | Type     | Description                                                                                                 |
| -------------- | -------- | ----------------------------------------------------------------------------------------------------------- |
| `initialDelay` | `number` | Milliseconds of inactivity before the first probe. `0` uses the operating system default.                   |
| `interval`     | `number` | Milliseconds between probes. Requires Node.js 24.19.0 or 26.4.0 and later.                                  |
| `count`        | `number` | Number of unanswered probes before the connection is dropped. Requires Node.js 24.19.0 or 26.4.0 and later. |

Node.js truncates `initialDelay` and `interval` to whole seconds, so values below `1000` become `0`; with `strict` enabled they are rejected instead. Setting `interval` or `count` on an older Node.js version throws a `UserError`.

`keepAlive` is applied after the socket is created, so it takes precedence over `keepAlive` and `keepAliveInitialDelay` passed in `tls`.

## Custom socket factory

By default the client opens each broker connection itself with Node.js `net.connect()` or `tls.connect()`. Setting `socketFactory` lets the application open the socket instead: to check broker addresses before connecting, to go through a proxy, or to reach brokers through a tunnel. It is called for every broker connection, the bootstrap brokers and the brokers the cluster advertises in its metadata alike.

The factory receives an object with these properties:

| Property  | Type          | Description                                                                                              |
| --------- | ------------- | -------------------------------------------------------------------------------------------------------- |
| `host`    | `string`      | Broker host, as configured or as advertised by the cluster.                                              |
| `port`    | `number`      | Broker port.                                                                                             |
| `timeout` | `number`      | The `connectTimeout` of the connection, in milliseconds.                                                 |
| `signal`  | `AbortSignal` | Aborted when the connection attempt times out or the connection is closed before the socket is returned. |

It returns a plain `net.Socket` to the broker, or a promise of one, either connected or still connecting. The client then uses it as it uses its own sockets: when `tls` is set, TLS is negotiated on it with the same options and SNI rules (`tlsServerName`), the certificate is verified against `host`, and `keepAlive`, SASL and `connectionsMaxIdle` apply as usual. `connectTimeout` bounds the whole attempt: the factory, the TCP connection of a socket it returns still connecting, and the TLS handshake. A socket returned after the attempt timed out or was closed is destroyed, and so is a socket that is destroyed or was never connected, a `tls.TLSSocket` while `tls` is set, or a value that is not a `net.Socket`; the last three fail the connection with a `UserError`.

The `signal` is aborted only when an attempt is given up, never once the connection is up, so it can be passed to `net.connect()`.

Only a `net.Socket` is accepted, since the client calls `setNoDelay()`, `setKeepAlive()` and `destroySoon()` on it. A tunnel that gives a plain stream, such as an SSH channel, has to be exposed as a socket, for example through a local forwarding server.

An error thrown by the factory, or a promise it rejects, fails the connection with a `NetworkError` whose `cause` is that error. Like any connection error it is retried, unless the error, or one of its causes, has `canRetry` set to `false`.

This example refuses brokers on loopback, private, link-local and shared addresses, and connects to the address it checked, so a second DNS answer cannot change it. Adjust the list to your network: it is a starting point, not a complete policy.

```javascript
import { lookup } from 'node:dns/promises'
import { BlockList, createConnection } from 'node:net'
import { Admin } from '@platformatic/kafka'

const blocked = new BlockList()

// IPv4-mapped IPv6 addresses (::ffff:10.0.0.1) are checked against these too
for (const [network, prefix] of [
  ['0.0.0.0', 8],
  ['10.0.0.0', 8],
  ['100.64.0.0', 10],
  ['127.0.0.0', 8],
  ['169.254.0.0', 16],
  ['172.16.0.0', 12],
  ['192.168.0.0', 16]
]) {
  blocked.addSubnet(network, prefix, 'ipv4')
}

for (const [network, prefix] of [
  ['::', 128],
  ['::1', 128],
  ['fc00::', 7],
  ['fe80::', 10]
]) {
  blocked.addSubnet(network, prefix, 'ipv6')
}

const admin = new Admin({
  clientId: 'my-admin',
  bootstrapBrokers: ['broker.example.com:9092'],
  async socketFactory ({ host, port, signal }) {
    const { address, family } = await lookup(host)

    if (blocked.check(address, family === 6 ? 'ipv6' : 'ipv4')) {
      throw Object.assign(new Error(`Broker ${host}:${port} is not allowed.`), { canRetry: false })
    }

    return createConnection({ host: address, port, signal })
  }
})
```

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
[node-socket-set-keep-alive]: https://nodejs.org/dist/latest/docs/api/net.html#socketsetkeepalive
