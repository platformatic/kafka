# node-rdkafka Compatibility

The node-rdkafka compatibility layer implements the listed node-rdkafka `3.6.1` APIs using `@platformatic/kafka`. It does not load librdkafka or a native addon.

## Import

```typescript
import { AdminClient, HighLevelProducer, KafkaConsumer, Producer } from '@platformatic/kafka/compatibility/node-rdkafka'
```

The entry point also exports the deprecated `Consumer` constructor wrapper, stream factories, `CODES`, `Topic`, `features`, and `librdkafkaVersion` compatibility values. Using `Consumer` emits node-rdkafka's deprecation warning; use `KafkaConsumer` instead.

## Configuration

Common librdkafka properties are translated, including bootstrap brokers, client and group IDs, request and metadata timeouts, producer retry controls, acknowledgements, idempotence, transactions, compression, fetch limits, offset reset, isolation level, TLS, and PLAIN/SCRAM/OAuth bearer credentials.

Only the documented translated properties affect the Platformatic client. Unsupported SASL mechanisms report `ERR__NOT_IMPLEMENTED`; other librdkafka-only properties are not passed to a native client.

## Producer

| API                                          | Status        | Notes                                                                                                          |
| -------------------------------------------- | ------------- | -------------------------------------------------------------------------------------------------------------- |
| `connect()`, `disconnect()`, `isConnected()` | Available     | Uses callback and EventEmitter lifecycle conventions and supports reconnecting an existing facade.              |
| `produce()`                                  | Available     | Queues an asynchronous Platformatic send and returns synchronously.                                            |
| Delivery reports                             | Available     | Reports preserve topic, partition, key, timestamp, value mode, and opaque correlation data.                    |
| `poll()`, `setPollInterval()`                | Available     | Drains compatibility delivery reports; an interval enables automatic draining.                                |
| `flush()`                                    | Available     | Waits for queued sends, then services pending delivery reports.                                                |
| `offsetsForTimes()`                          | Available     | Uses a short-lived Platformatic offset requester for producer instances.                                       |
| `HighLevelProducer` serializers              | Available     | Synchronous, promise, and callback serializers are supported.                                                  |
| Transactions                                 | Partial       | Initialization, begin, commit, and abort use Platformatic transactions.                                        |
| `sendOffsetsToTransaction()`                 | Available     | Uses the supplied consumer's active group membership metadata.                                                 |
| Repeated header names                        | Partial       | Platformatic retains one value per header name.                                                                |

## Consumer

| API                                                         | Status        | Notes                                                                                                                                              |
| ----------------------------------------------------------- | ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `connect()`, `disconnect()`, `subscribe()`, `unsubscribe()` | Available     | String and regular-expression subscriptions are accepted and existing facades can reconnect.                                                     |
| `consume(number, callback)`                                 | Available     | Returns a bounded message array after the requested count or consume timeout.                                                                      |
| `consume(callback)` and `consume()`                         | Available     | Starts flowing consumption with callback and/or `data` events.                                                                                     |
| Committed offset restart                                    | Available     | `auto.offset.reset` controls fallback for uncommitted partitions.                                                                                  |
| `commit()`, `commitMessage()`                               | Available     | Commits delivered or explicitly supplied offsets asynchronously.                                                                                   |
| `commitSync()`, `commitMessageSync()`                       | Not available | Pure JavaScript cannot synchronously wait for broker acknowledgement; these report `ERR__NOT_IMPLEMENTED`.                                         |
| `committed()`, `offsetsStore()`, `offsetsForTimes()`        | Available     | Results are converted between number and `bigint` representations.                                                                                 |
| Assignment and incremental assignment                       | Partial       | Beginning, end, stored, omitted, and concrete offsets are resolved before fetching. Manual assignments do not join a consumer group.               |
| `position()` and `seek()`                                   | Partial       | Uses delivered positions and seek-version protection. Seek supports concrete, beginning, and end offsets; stored-offset seek remains unavailable. |
| `pause()` and `resume()`                                    | Partial       | Paused partitions are excluded from fetches while unpaused partitions continue.                                                                   |
| Rebalance events and callback                               | Partial       | Function callbacks own assignment; native cooperative state is not reproduced.                                                                    |
| Partition EOF and native warnings                           | Partial       | `partition.eof` is emitted for empty fetches when `enable.partition.eof` is enabled; native warnings remain unavailable.                           |

## Admin

| API                                                    | Status        | Notes                                                                                       |
| ------------------------------------------------------ | ------------- | ------------------------------------------------------------------------------------------- |
| `AdminClient.create()`                                 | Available     | Creates a Platformatic admin-backed facade. `AdminClient` itself is a non-constructible factory object. |
| `createTopic()`, `deleteTopic()`, `createPartitions()` | Available     | Callback arguments and per-call deadlines are propagated to the broker operations.          |
| `disconnect()`                                         | Partial       | Starts asynchronous Platformatic shutdown behind the synchronous node-rdkafka method shape. |
| OAuth token refresh                                    | Available     | Updates the mutable OAuth bearer credential used by new authentications.                   |

## Streams

| API                                                         | Status    | Notes                                                                       |
| ----------------------------------------------------------- | --------- | --------------------------------------------------------------------------- |
| `createReadStream()` and `KafkaConsumer.createReadStream()` | Available | Uses bounded pulls, `fetchSize`, batching, and Node.js backpressure.        |
| `createWriteStream()` and `Producer.createWriteStream()`    | Available | Supports buffer and object modes.                                           |
| `autoClose` and native queue retry timing                   | Partial   | Shutdown is idempotent; queue scheduling uses Node.js rather than librdkafka timing. |

## Native-Only APIs

`getClient()` returns the underlying Platformatic client rather than a native handle. Native threads, queue internals, statistics and log callbacks, arbitrary librdkafka plugins, exact local error generation, synchronous broker commits, and native event timing are not reproduced. Repeated message headers retain one value per name because Platformatic message headers use a map. Numeric error and topic constants are provided for the compatibility facade; `librdkafkaVersion` identifies the reference ABI and does not indicate a loaded native library.

## Version Contract

Compatibility is defined against the public API of node-rdkafka `3.6.1` and its librdkafka `2.12.0` reference. These versions identify the reference API; they are not compatibility ranges for earlier or later releases.
