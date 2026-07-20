# KafkaJS Compatibility

The KafkaJS compatibility layer implements the listed KafkaJS `2.2.4` APIs using `@platformatic/kafka`.

## Import

```typescript
import { CompressionTypes, Kafka, logLevel } from '@platformatic/kafka/compatibility/kafkajs'
```

The entry point exports KafkaJS-shaped runtime values and self-contained TypeScript declarations. It does not load KafkaJS at runtime.

## Client Configuration

| API                                   | Status        | Notes                                                                                                   |
| ------------------------------------- | ------------- | ------------------------------------------------------------------------------------------------------- |
| `new Kafka()`                         | Available     | Static or lazy broker lists, client ID, connection/authentication/request timeouts, retry count, TLS, socket factories, and supported SASL options are translated. |
| `producer()`, `consumer()`, `admin()` | Available     | Return compatibility clients backed by the corresponding `@platformatic/kafka` client.                  |
| `logger()` and `logCreator`           | Available     | Uses KafkaJS log levels and entry shapes.                                                               |
| Broker resolver function              | Available     | Resolved lazily for bootstrap attempts and reconnects.                                                  |
| Custom socket factory                 | Available     | Receives KafkaJS-shaped host, port, TLS, and `onConnect` arguments.                                     |
| Authentication and reauthentication timeouts | Available | Separate authentication deadline and configurable reauthentication threshold are applied.               |
| Disabled request timeout enforcement  | Available     | `enforceRequestTimeout: false` disables the local response timer.                                       |
| PLAIN, SCRAM, OAuth bearer            | Available     | KafkaJS mechanism names are translated to Platformatic mechanism names.                                 |
| KafkaJS `AWS` and custom SASL mechanisms | Available | Custom providers use KafkaJS framing; `AWS` follows KafkaJS 2.2.4's historical AWS payload, not `AWS_MSK_IAM`. |

## Producer

| API                                    | Status        | Notes                                                                                                                 |
| -------------------------------------- | ------------- | --------------------------------------------------------------------------------------------------------------------- |
| `connect()`, `disconnect()`            | Available     | `connect()` performs metadata discovery; reconnecting preserves producer and transaction state while replacing broker connections. |
| `send()`                               | Available     | KafkaJS messages, acknowledgements, compression, and Produce request timeouts are converted.                           |
| `sendBatch()`                          | Available     | Topic batches are combined into a Platformatic send; per-batch Produce request timeouts are preserved.                 |
| Message and acknowledgement validation | Available     | Topics, message values, disconnected state, idempotent acknowledgements, and custom partition results are validated.  |
| Retry configuration                    | Available     | Client and producer retry settings are merged and translated, including exponential delay, jitter, and maximum delay. |
| Producer errors                        | Available     | Protocol, request timeout, authentication, network, and structured retry-exhaustion details are converted to KafkaJS error shapes. |
| Compression and `CompressionCodecs`    | Available     | GZIP, Snappy, LZ4, and ZSTD constants and codecs use Platformatic's compression implementations.                      |
| Default and legacy partitioners        | Available     | Partition selection follows KafkaJS `2.2.4`.                                                                          |
| Custom partitioner                     | Available     | Invoked with KafkaJS message metadata, including explicit partitions.                                                  |
| Idempotent producer                    | Available     | Uses Platformatic idempotent production and enforces KafkaJS retry and acknowledgement constraints.                   |
| `transaction()`                        | Available | `send()`, `sendBatch()`, `sendOffsets()`, `commit()`, `abort()`, and `isActive()` are available. |
| Repeated header names                  | Available | Ordered repeated values are preserved when producing and consuming through the compatibility layer. |

KafkaJS's default `acks: -1` and `allowAutoTopicCreation: true` values are retained.
An unspecified `maxInFlightRequests` uses KafkaJS's effectively unlimited behavior.

## Consumer

| API                                       | Status        | Notes                                                                                                                                                                                                                                         |
| ----------------------------------------- | ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `connect()`, `disconnect()`, `stop()`     | Available     | Stops delivery, closes the active stream, waits for handlers, and leaves the group before returning.                                                                                                                                          |
| `subscribe()`                             | Available     | String subscriptions are retained per topic; regular expressions are resolved against metadata when subscribed.                                                                                                                               |
| `run({ eachMessage })`                    | Available     | Waits for initial group startup; handler completion precedes offset resolution and configured automatic commits.                                                                                                                              |
| `run({ eachBatch })`                      | Available     | Broker fetch-partition boundaries, control-record progress, and high watermarks are preserved, including KafkaJS lag calculations.                                                                                                        |
| Automatic commits                         | Available     | `autoCommit`, `autoCommitInterval`, `autoCommitThreshold`, `eachBatchAutoResolve`, and `commitOffsetsIfNecessary()` are applied to resolved offsets.                                                                                          |
| Committed offset restart                  | Available     | `fromBeginning` selects each topic's initial and out-of-range fallback while valid committed offsets take precedence.                                                                                                                          |
| `commitOffsets()` and uncommitted offsets | Available     | Explicit string offsets and commit metadata are preserved; `eachBatch` reports resolved offsets that have not been committed.                                                                                                                 |
| `seek()`                                  | Available     | Applies per topic-partition without restarting unrelated delivery; stale fetches, rebalances, and superseded earliest/latest resolution are ignored.                                                                                           |
| `pause()`, `paused()`, `resume()`         | Available     | Delivery is paused and resumed selectively by topic-partition while retaining the next record.                                                                                                                                                |
| `partitionsConsumedConcurrently`          | Available     | Different partitions run concurrently up to the configured limit while order is preserved within each partition.                                                                                                                              |
| Heartbeat callback                        | Available     | Handler heartbeats share the native throttled group heartbeat operation and propagate request failures.                                                                                                                                         |
| `restartOnFailure`                        | Available     | Retriable pump and terminal group failures emit `CRASH` and apply the configured restart decision.                                                                                                                                            |
| Custom partition assigners                | Supported     | Async `assign()` and custom protocol metadata are supported. The assigner receives a metadata-oriented `Cluster` facade; broker-oriented methods report `KafkaJSNotImplemented`.                                                               |

The adapter disables the underlying stream's automatic commits and commits only after successful compatibility handlers.
It resets transient pause, seek, and local offset state when a run stops or restarts after a crash. Rebalances preserve pauses for retained assignments while stale handlers cannot resolve or commit offsets.

## Admin

| API                                                     | Status    | Notes                                                                                                                        |
| ------------------------------------------------------- | --------- | ---------------------------------------------------------------------------------------------------------------------------- |
| Topic listing, creation, deletion, and partition growth | Available | Low-level requests preserve timeouts, replica assignments, validation flags, leader waiting, and per-topic errors.           |
| Topic metadata and offsets                              | Available | Numeric Platformatic offsets are returned as strings; earliest, latest, and timestamp lookups are supported.                 |
| Consumer-group list, describe, and delete               | Available | Coordinator requests preserve group state, protocol data, member metadata, member assignments, and per-group errors.         |
| Consumer-group fetch, set, and reset offsets            | Available | Offset metadata and optional special-offset resolution are converted; offset mutation requires an inactive group.            |
| Cluster description                                     | Available | Broker, controller, and cluster identifiers are converted from Platformatic metadata.                                        |
| Configuration description and alteration                | Available | Topic, broker, and broker-logger requests preserve validation flags, synonyms, sources, and per-resource results.            |
| ACL creation, description, and deletion                 | Available | Controller requests preserve filters, resource details, matching ACLs, and per-filter errors.                                |
| Topic record deletion                                   | Available | String offsets are converted to `bigint`, with partition failures exposed through KafkaJS error shapes.                      |
| Partition reassignment                                  | Available | Alter and list operations use controller requests and preserve replica, adding-replica, removing-replica, and error details. |

## Errors And Events

KafkaJS error classes, constants, assignment protocol codecs, partitioners, logger types, and instrumentation event names are exported. Platformatic offset-range and member-ID protocol failures use their specialized KafkaJS classes, and retry exhaustion uses `KafkaJSNumberOfRetriesExceeded` with retry count and delay metadata. Compatibility clients emit KafkaJS lifecycle, commit, crash, fetch, request, request-timeout, and request-queue events. Request events include the wire API identity, broker, client ID, correlation ID, queue and response timings, and response frame size.

## Version Contract

Compatibility is defined against the public API of KafkaJS `2.2.4`. The version identifies the reference API; it is not a compatibility range for earlier or later KafkaJS releases.
