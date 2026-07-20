# Compatibility Layers

`@platformatic/kafka` provides compatibility entry points for selected APIs from KafkaJS and node-rdkafka. These entry points use `@platformatic/kafka` internally while exposing the documented calling conventions of the corresponding client.

## Available Layers

| Client       | Import path                                      | Reference version | Documentation                     |
| ------------ | ------------------------------------------------ | ----------------- | --------------------------------- |
| KafkaJS      | `@platformatic/kafka/compatibility/kafkajs`      | `2.2.4`           | [KafkaJS](./kafkajs.md)           |
| node-rdkafka | `@platformatic/kafka/compatibility/node-rdkafka` | `3.6.1`           | [node-rdkafka](./node-rdkafka.md) |

## Compatibility Contract

Compatibility applies to the APIs explicitly listed in each client document. It does not imply coverage of undocumented APIs, package internals, deep imports, or other versions of the source packages.

Covered APIs retain their documented input, output, callback, promise, and event conventions where listed. Broker interaction, scheduling, errors, diagnostics, and resource management are provided by `@platformatic/kafka` and can differ from the reference implementation where documented.

Changes to the compatibility layers follow the `@platformatic/kafka` versioning policy.

## Module Format

The compatibility entry points are ECMAScript modules. They expose named exports and can be loaded with `import`. On supported Node.js versions, `require()` can load the synchronous ESM namespace; deep imports and replacement of the original package name are not provided.

## INTERNAL

Status: shallow public API converted for both. Existing original test suite coverage to be done.
