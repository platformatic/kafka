# Confluent Schema Registry

> ⚠️ **Experimental API**
> `ConfluentSchemaRegistry` and the related `registry`, `beforeSerialization`, and `beforeDeserialization` hooks are experimental.
> They **do not follow semver** and may change in minor/patch releases.

The `ConfluentSchemaRegistry` class provides integration with [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html) for automatic message serialization and deserialization with schema management.

## Features

- **Multi-format support**: AVRO, Protocol Buffers, and JSON Schema
- **Automatic serialization/deserialization**: Seamlessly integrates with Producer and Consumer
- **Schema caching**: Fetched schemas are cached for performance
- **Authentication support**: Basic and Bearer token authentication
- **Type safety**: Full TypeScript generics support
- **Validation**: Optional JSON schema validation on send, automatic on receive

## Installation

The Confluent Schema Registry support is included in the main package:

```bash
npm install @platformatic/kafka
```

For Protocol Buffers support, also install:

```bash
npm install protobufjs
```

## Constructor

Creates a new schema registry instance with type `ConfluentSchemaRegistry<Key, Value, HeaderKey, HeaderValue>`.

Options:

| Property             | Type                           | Required | Description                                                                  |
| -------------------- | ------------------------------ | -------- | ---------------------------------------------------------------------------- |
| `url`                | `string`                       | Yes      | URL of the Confluent Schema Registry                                         |
| `auth`               | `object`                       | No       | Authentication configuration                                                 |
| `auth.username`      | `string \| CredentialProvider` | No       | Username for Basic authentication                                            |
| `auth.password`      | `string \| CredentialProvider` | No       | Password for Basic authentication                                            |
| `auth.token`         | `string \| CredentialProvider` | No       | Token for Bearer authentication                                              |
| `tls`                | `object`                       | No       | TLS options for HTTPS registries, as accepted by `tls.connect`               |
| `ssl`                | `object`                       | No       | Alias for `tls`                                                              |
| `headers`            | `object \| function`           | No       | Additional HTTP headers sent with every registry request                     |
| `timeout`            | `number`                       | No       | Request timeout in milliseconds, `0` disables it (default: `5000`)           |
| `retries`            | `number`                       | No       | Amount of retries for failed requests (default: `3`)                         |
| `retryDelay`         | `number \| function`           | No       | Delay between retries in milliseconds (default: `1000`)                      |
| `protobufTypeMapper` | `function`                     | No       | Custom type mapper for Protocol Buffers                                      |
| `jsonValidateSend`   | `boolean`                      | No       | Enable JSON schema validation on send (default: `false`)                     |
| `jsonAjvOptions`     | `object`                       | No       | AJV options for JSON schemas. Defaults to `{ allErrors: true, coerceTypes: false, strict: true }` |

## Basic Usage

### AVRO Schema

```typescript
import { Producer, Consumer } from '@platformatic/kafka'
import { ConfluentSchemaRegistry } from '@platformatic/kafka/registries'

// Create registry instance
const registry = new ConfluentSchemaRegistry({
  url: 'http://localhost:8081'
})

// Producer
const producer = new Producer({
  clientId: 'avro-producer',
  bootstrapBrokers: ['localhost:9092'],
  registry
})

await producer.send({
  messages: [
    {
      topic: 'users',
      key: { id: 123 },
      value: { name: 'John Doe', age: 30 },
      metadata: {
        schemas: {
          key: 1, // AVRO schema ID for key
          value: 2 // AVRO schema ID for value
        }
      }
    }
  ]
})

// Consumer
const consumer = new Consumer({
  groupId: 'avro-consumers',
  clientId: 'avro-consumer',
  bootstrapBrokers: ['localhost:9092'],
  registry
})

const stream = await consumer.consume({
  topics: ['users']
})

for await (const message of stream) {
  // Automatically deserialized from AVRO
  console.log('User:', message.value)
}
```

### JSON Schema

```typescript
const registry = new ConfluentSchemaRegistry({
  url: 'http://localhost:8081',
  jsonValidateSend: true // Enable validation on send
})

const producer = new Producer({
  clientId: 'json-producer',
  bootstrapBrokers: ['localhost:9092'],
  registry
})

// Will validate against schema before sending
await producer.send({
  messages: [
    {
      topic: 'events',
      value: {
        eventType: 'user_login',
        timestamp: Date.now(),
        userId: 'user-123'
      },
      metadata: {
        schemas: {
          value: 3 // JSON schema ID
        }
      }
    }
  ]
})
```

JSON Schema drafts 04, 06, 07, and 2020-12 are supported. The draft is selected from the schema's `$schema` property; schemas without one use the default 2020-12 validator.

Set `jsonAjvOptions.strict` to `false` if the registry contains JSON schemas with non-standard keywords:

```typescript
const registry = new ConfluentSchemaRegistry({
  url: 'http://localhost:8081',
  jsonAjvOptions: {
    strict: false
  }
})
```

### Protocol Buffers

```typescript
// Custom type mapper for complex protobuf schemas
function customTypeMapper (id, type, context) {
  // Map schema IDs to protobuf message types
  const typeMap = {
    4: 'com.example.UserKey',
    5: 'com.example.UserValue'
  }
  return typeMap[id] || `${context.topic}-${type}`
}

const registry = new ConfluentSchemaRegistry({
  url: 'http://localhost:8081',
  protobufTypeMapper: customTypeMapper
})

const producer = new Producer({
  clientId: 'protobuf-producer',
  bootstrapBrokers: ['localhost:9092'],
  registry
})

await producer.send({
  messages: [
    {
      topic: 'users',
      key: { id: 123 },
      value: { name: 'John', email: 'john@example.com' },
      metadata: {
        schemas: {
          key: 4, // Protobuf schema ID for key
          value: 5 // Protobuf schema ID for value
        }
      }
    }
  ]
})
```

## Authentication

### Basic Authentication

```typescript
const registry = new ConfluentSchemaRegistry({
  url: 'http://localhost:8081',
  auth: {
    username: 'user',
    password: 'password'
  }
})
```

### Bearer Token Authentication

```typescript
const registry = new ConfluentSchemaRegistry({
  url: 'http://localhost:8081',
  auth: {
    token: 'your-api-token'
  }
})
```

### Dynamic Credentials

```typescript
// Using CredentialProvider for dynamic credentials
const registry = new ConfluentSchemaRegistry({
  url: 'http://localhost:8081',
  auth: {
    username: async () => getUsername(),
    password: async () => getPassword()
  }
})
```

### Custom Headers

Some deployments require additional headers alongside the `Authorization` one. For example,
[Confluent Cloud OAuth](https://docs.confluent.io/cloud/current/sr/sr-rest-apis.html#oauth-for-ccloud-sr-rest-api)
requires the identity pool and the target cluster to be sent on every request:

```typescript
const registry = new ConfluentSchemaRegistry({
  url: 'https://psrc-xxxxx.europe-west3.gcp.confluent.cloud',
  auth: {
    token: () => getOAuthToken()
  },
  headers: {
    'Confluent-Identity-Pool-Id': 'pool-abc123',
    'target-sr-cluster': 'lsrc-abc123'
  }
})
```

`headers` also accepts a function, which is invoked before each request and can be asynchronous:

```typescript
const registry = new ConfluentSchemaRegistry({
  url: 'http://localhost:8081',
  async headers () {
    return { 'X-Request-Id': randomUUID() }
  }
})
```

Custom headers are merged with the authentication ones. The headers generated by `auth` take precedence,
so `Authorization` cannot be overridden via `headers` when `auth` is also configured.

## Timeouts and Retries

Schema fetches are bounded by `timeout` and retried up to `retries` times, mirroring the
`timeout`, `retries` and `retryDelay` options of the Kafka clients:

```typescript
const registry = new ConfluentSchemaRegistry({
  url: 'http://localhost:8081',
  timeout: 2000,
  retries: 5,
  retryDelay: 500
})
```

Set `timeout: 0` to disable the timeout and `retries: 0` to disable retries.

Only failures which can plausibly succeed later are retried: network errors, timeouts and the
`408`, `425`, `429`, `500`, `502`, `503` and `504` responses. Every other status, such as the `404`
returned for an unknown schema ID or the `401` returned for invalid credentials, fails immediately.

When all the attempts fail, the resulting error is a `MultipleErrors` whose `errors` array contains
every attempt, in order. A failure which is not retried is thrown as is: a `UserError` for HTTP
errors, a `TimeoutError` when the deadline expires and a `NetworkError` when the connection fails.

`retryDelay` also accepts a function, which receives the attempt number (1-based), the configured
amount of retries and the error which caused the retry, and returns the delay in milliseconds:

```typescript
const registry = new ConfluentSchemaRegistry({
  url: 'http://localhost:8081',
  retryDelay: attempt => 2 ** attempt * 100
})
```

## TLS

Enterprise registries are rarely served by a certificate signed by a public CA. The `tls` option
(aliased as `ssl`, like in the client options) accepts everything
[`tls.connect`](https://nodejs.org/api/tls.html#tlsconnectoptions-callback) does and applies it to
every Schema Registry request:

```typescript
const registry = new ConfluentSchemaRegistry({
  url: 'https://schema-registry.internal:8081',
  tls: {
    ca: await readFile('/etc/ssl/internal-ca.pem')
  }
})
```

Mutual TLS, SNI and TLS version bounds are configured the same way:

```typescript
const registry = new ConfluentSchemaRegistry({
  url: 'https://schema-registry.internal:8081',
  tls: {
    ca: await readFile('/etc/ssl/internal-ca.pem'),
    cert: await readFile('/etc/ssl/client.pem'),
    key: await readFile('/etc/ssl/client.key'),
    servername: 'schema-registry.internal',
    minVersion: 'TLSv1.3'
  }
})
```

Certificate validation can be disabled with `rejectUnauthorized: false`. Do not do this outside
development: it accepts any certificate, which defeats the point of using TLS.

Under the hood the options are handed to an [undici](https://undici.nodejs.org) `Agent`, which is
passed to `fetch()` as its dispatcher. The agent is created once per registry, so schema fetches keep
reusing the same pooled connection. Node bundles undici but does not export it, so the `Agent` class
is read from the constructor of the global dispatcher `fetch()` itself uses; nothing needs to be
installed.

Since TLS options are meaningless on a plaintext connection, providing them together with a non
`https:` URL throws a `UserError` from the constructor rather than silently ignoring them.

## Message Metadata (Producer)

Schema IDs are passed through message metadata when producing:

```typescript
// Producer message with schema metadata
const message = {
  topic: 'my-topic',
  key: { id: 123 },
  value: { data: 'example' },
  headers: { source: 'api' },
  metadata: {
    schemas: {
      key: 1, // Schema ID for key
      value: 2, // Schema ID for value
      headerKey: 3, // Schema ID for header keys (optional)
      headerValue: 4 // Schema ID for header values (optional)
    }
  }
}
```

## Schema Types

### AVRO Schema

AVRO schemas are parsed using the `avsc` library:

```typescript
// Registry response for AVRO schema
{
  "schemaType": "AVRO",
  "schema": "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}]}"
}
```

### Protocol Buffers Schema

Protocol Buffers schemas require the `protobufjs` library:

```typescript
// Registry response for Protobuf schema
{
  "schemaType": "PROTOBUF",
  "schema": "syntax = \"proto3\";\n\nmessage User {\n  int32 id = 1;\n  string name = 2;\n}"
}
```

### JSON Schema

JSON schemas are validated using AJV:

```typescript
// Registry response for JSON schema
{
  "schemaType": "JSON",
  "schema": "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"number\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"]}"
}
```

JSON validation failures produce a `SchemaValidationError`, which extends `UserError`. The error is passed directly to `onDeserializationError`. When serialization or deserialization fails the operation instead, its outer `UserError` exposes the `SchemaValidationError` as `cause`. The validation error includes `schemaId`, `schemaType`, `phase`, `payloadType`, the decoded `data`, and AJV `validationErrors` so applications can distinguish schema-invalid data from malformed JSON.
