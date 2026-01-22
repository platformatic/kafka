# Confluent Schema Registry

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

| Property             | Type                           | Required | Description                                              |
| -------------------- | ------------------------------ | -------- | -------------------------------------------------------- |
| `url`                | `string`                       | Yes      | URL of the Confluent Schema Registry                     |
| `auth`               | `object`                       | No       | Authentication configuration                             |
| `auth.username`      | `string \| CredentialProvider` | No       | Username for Basic authentication                        |
| `auth.password`      | `string \| CredentialProvider` | No       | Password for Basic authentication                        |
| `auth.token`         | `string \| CredentialProvider` | No       | Token for Bearer authentication                          |
| `protobufTypeMapper` | `function`                     | No       | Custom type mapper for Protocol Buffers                  |
| `jsonValidateSend`   | `boolean`                      | No       | Enable JSON schema validation on send (default: `false`) |

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
