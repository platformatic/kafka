# Troubleshooting

## Connection errors during TLS or authentication setup

Inspect the nested errors when an operation such as `joinGroup` or `listApis` fails. For example:

```text
joinGroup failed 4 times.
  findGroupCoordinator failed 4 times.
    listApis failed 4 times.
      PLT_KFK_NETWORK: Connection closed
```

The outer errors reflect retries of operations that depend on the failed connection. A `joinGroup` failure does not
necessarily indicate a consumer group problem. The retry count depends on your configuration.

A nested `listApis` failure is a useful indicator that the connection is failing during the initial setup or API
negotiation, before the requested higher-level operation can complete. Check TLS configuration (including SNI) and
authentication first. These errors alone do not establish the cause; DNS, network access, or a server closing the
connection can also produce connection failures.

### Check TLS Server Name Indication (SNI)

Kafka deployments whose TLS endpoints require SNI can close connections when the client does not send a server name.
This is not specific to a Kafka provider. Enabling TLS alone with `tls: {}` does not send SNI automatically. Set
`tlsServerName: true` to use the hostname of each target broker, including brokers discovered after bootstrap:

```javascript
import { Consumer } from '@platformatic/kafka'

const consumer = new Consumer({
  clientId: 'my-consumer',
  groupId: 'my-consumer-group',
  bootstrapBrokers: ['kafka.example.com:9092'],
  tls: {},
  tlsServerName: true,
  sasl: {
    mechanism: 'PLAIN',
    username: process.env.KAFKA_USERNAME,
    password: process.env.KAFKA_PASSWORD
  }
})
```

Replace the example hostname with your cluster's bootstrap endpoint. The example uses SASL/PLAIN; keep your cluster's
existing SASL configuration, or omit `sasl` if it is not required. `bootstrapBrokers` entries use `hostname:port`, not
any protocol prefix. Preserve any existing TLS options and keep certificate verification enabled.

### What if enabling SNI does not resolve the error?

- Verify that the bootstrap endpoint and port match the cluster's TLS and authentication requirements.
- Verify DNS resolution and network access to both the bootstrap endpoint and the broker hostnames returned in metadata.
- Check TLS options, certificate trust, SASL mechanism, and credentials. Successful metadata access does not establish consumer group authorization.
- Include the library and Node.js versions, sanitized client configuration, and nested errors when reporting the issue. Never include API secrets.

See [TLS configuration](./base.md#tls-server-name-indication-sni) and [Diagnostic and Instrumentation](./diagnostic.md) for more details.
