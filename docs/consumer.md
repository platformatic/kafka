# Consumer

Client for consuming messages from Kafka topics with support for consumer groups.

The consumer inherits from the [`Base`](./base.md) client.

The complete TypeScript type of the `Consumer` is determined by the `deserializers` option.

## Events

| Name                        | Payload Type                     | Description                                                                                |
| --------------------------- | -------------------------------- | ------------------------------------------------------------------------------------------ |
| `consumer:group:join`       | `ConsumerGroupJoinPayload`       | Emitted when joining a group.                                                              |
| `consumer:group:leave`      | `ConsumerGroupLeavePayload`      | Emitted when leaving a group.                                                              |
| `consumer:group:rejoin`     | (none)                           | Emitted when re-joining a group after a rebalance.                                         |
| `consumer:group:rebalance`  | `ConsumerGroupRebalancePayload`  | Emitted when group rebalancing occurs.                                                     |
| `consumer:heartbeat:start`  | `ConsumerHeartbeatPayload?`      | Emitted when starting new heartbeats.                                                      |
| `consumer:heartbeat:cancel` | `ConsumerHeartbeatPayload`       | Emitted if a scheduled heartbeat has been canceled.                                        |
| `consumer:heartbeat:end`    | `ConsumerHeartbeatPayload?`      | Emitted during successful heartbeats.                                                      |
| `consumer:heartbeat:error`  | `ConsumerHeartbeatErrorPayload`  | Emitted during failed heartbeats.                                                          |
| `consumer:lag`              | `Offsets`                        | Emitted during periodic lag monitoring with calculated lag data for topics and partitions. |
| `consumer:lag:error`        | `Error`                          | Emitted when lag calculation fails during monitoring operations.                           |

### Event Payload Types

```typescript
interface ConsumerGroupJoinPayload {
  groupId: string
  memberId: string
  generationId?: number
  isLeader?: boolean
  assignments?: GroupAssignment[]
}

interface ConsumerGroupLeavePayload {
  groupId: string
  memberId: string | null
  generationId?: number
}

interface ConsumerGroupRebalancePayload {
  groupId: string
}

interface ConsumerHeartbeatPayload {
  groupId?: string
  memberId?: string | null
  generationId?: number
}

interface ConsumerHeartbeatErrorPayload extends ConsumerHeartbeatPayload {
  error: Error
}

// Offsets is a Map<string, bigint[]> where keys are topic names
// and values are arrays of offsets (position represents the partition)
type Offsets = Map<string, bigint[]>
```

## Constructor

Creates a new consumer with type `Consumer<Key, Value, HeaderKey, HeaderValue>`.

Options:

| Property            | Type                                                | Default                   | Description                                                                                                                                                                                                                                                                                                                                                                                          |
| ------------------- | --------------------------------------------------- | ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| groupId             | `string`                                            |                           | Consumer group ID.                                                                                                                                                                                                                                                                                                                                                                                   |
| autocommit          | `boolean \| number`                                 | `true`                    | Whether to autocommit consumed messages.<br/><br/> If it is `true`, then messages are committed immediately.<br/><br/> If it is a number, it specifies how often offsets will be committed. Only the last offset for a topic-partition is committed.<br/><br/>If set to `false`, then each message read from the stream will have a `commit` method which should be used to manually commit offsets. |
| minBytes            | `number`                                            | `1`                       | Minimum amount of data the brokers should return. The value might not be respected by Kafka.                                                                                                                                                                                                                                                                                                         |
| maxBytes            | `number`                                            | 10MB                      | Maximum amount of data the brokers should return. The value might not be respected by Kafka.                                                                                                                                                                                                                                                                                                         |
| maxWaitTime         | `number`                                            | 5 seconds                 | Maximum amount of time in milliseconds the broker will wait before sending a response to a fetch request.                                                                                                                                                                                                                                                                                            |
| isolationLevel      | string                                              | `READ_COMMITTED`          | Kind of isolation applied to fetch requests. It can be used to only read producers-committed messages.<br/><br/> The valid values are defined in the `FetchIsolationLevels` enumeration.                                                                                                                                                                                                             |
| deserializers       | `Deserializers<Key, Value, HeaderKey, HeaderValue>` |                           | Object that specifies which deserialisers to use.<br/><br/>The object should only contain one or more of the `key`, `value`, `headerKey` and `headerValue` properties.                                                                                                                                                                                                                               |
| highWaterMark       | `number`                                            | `1024`                    | The maximum amount of messages to store in memory before delaying fetch requests. Note that this severely impacts both performance at the cost of memory use.                                                                                                                                                                                                                                        |
| sessionTimeout      | `number`                                            | 1 minute                  | Amount of time in milliseconds to wait for a consumer to send the heartbeat before considering it down.<br/><br/> This is only relevant when Kafka creates a new group.<br/><br/> Not supported for `groupProtocol=consumer`, instead it is set with broker configuration property `group.consumer.session.timeout.ms`.                                                                              |
| rebalanceTimeout    | `number`                                            | 2 minutes                 | Amount of time in milliseconds to wait for a consumer to confirm the rebalancing before considering it down.<br/><br/> This is only relevant when Kafka creates a new group.                                                                                                                                                                                                                         |
| heartbeatInterval   | `number`                                            | 3 seconds                 | Interval in milliseconds between heartbeats.<br/><br/> Not supported for `groupProtocol=consumer`, instead it is set with the broker configuration property `group.consumer.heartbeat.interval`.                                                                                                                                                                                                     |
| groupProtocol       | `'classic' \| 'consumer'`                           | `'classic'`               | Group protocol to use. Use `'classic'` for the original consumer group protocol and `'consumer'` for the new protocol introduced in [KIP-848](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol).<br/><br/> The `'consumer'` protocol provides server-side partition assignment and incremental rebalancing behavior.              |
| groupRemoteAssignor | `string`                                            | `null`                    | Server-side assignor to use for `groupProtocol=consumer`. Keep it unset to let the server select a suitable assignor for the group. Available assignors: `'uniform'` or `'range'`.                                                                                                                                                                                                                   |
| protocols           | `GroupProtocolSubscription[]`                       | `roundrobin`, version `1` | Protocols used by this consumer group.<br/><br/> Each protocol must be an object specifying the `name`, `version` and optionally `metadata` properties. <br/><br/> Not supported for `groupProtocol=consumer`.                                                                                                                                                                                       |
| partitionAssigner   | `GroupPartitionsAssigner`                           |                           | Client-side partition assignment strategy.<br/><br/> Not supported for `groupProtocol=consumer`, use `groupRemoteAssignor` instead.                                                                                                                                                                                                                                                                  |

It also supports all the constructor options of `Base`.

## Basic Methods

### `isActive`

Returns `true` if the consumer is not closed and it is currently an active member of a consumer group.

This method will return `false` during consumer group rebalacing.

### `consume<Key, Value, HeaderKey, HeaderValue>(options[, callback])`

Creates a stream to consume messages from topics.

The return value is a stream of type `MessagesStreamKey, Value, HeaderKey, HeaderValue>`.

Options:

| Property             | Type                            | Default  | Description                                                                                                                                                                                                                                                                                                                                    |
| -------------------- | ------------------------------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `topics`             | `string[]`                      |          | Topics to consume.                                                                                                                                                                                                                                                                                                                             |
| `mode`               | `string`                        | `LATEST` | Where to start fetching new messages.<br/><br/> The valid values are defined in the `MessagesStreamModes` enumeration (see below).                                                                                                                                                                                                             |
| `fallbackMode`       | `string`                        | `LATEST` | Where to start fetching new messages when offset information is lacking for the consumer group.<br/><br/> The valid values are defined in the `MessagesStreamFallbackModes` enumeration (see below).                                                                                                                                           |
| `maxFetches`         | `number`                        | `0`      | The maximum number of fetches to perform on each partition before automatically closing the stream. Setting to zero will disable automating closing. <br/><br/>Note that [`Array.fromAsync`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array/fromAsync) can be used to create an array out of a stream. |
| `offsets`            | `TopicWithPartitionAndOffset[]` |          | When manual offset mode is specified, a list of topic-partition-offset triplets.                                                                                                                                                                                                                                                               |
| `onCorruptedMessage` | `CorruptedMessageHandler`       |          | A callback that will be invoked if a deserialiser throws an error. If the function returns `true`, then the strem will throw an error and thus it will be destroyed. Note that the stream will not wait for the `commit` function to finish so make sure you handle callbacks and promises accordingly.                                        |

It also accepts all options of the constructor except `deserializers` and `groupId`.

`MessagesStreamModes` modes:

| Name        | Description                                                                |
| ----------- | -------------------------------------------------------------------------- |
| `LATEST`    | Consume messages received after the started streaming.                     |
| `EARLIEST`  | Consume messages from the earliest available on the broker.                |
| `COMMITTED` | Resume the consumer group from the offset after the last committed offset. |
| `MANUAL`    | Consume messages after the offsets provided in the `offsets` option.       |

`MessagesStreamFallbackModes` modes:

| Name       | Description                                                 |
| ---------- | ----------------------------------------------------------- |
| `LATEST`   | Consume messages received after the started streaming.      |
| `EARLIEST` | Consume messages from the earliest available on the broker. |
| `FAIL`     | Throw an exception.                                         |

### `close([force][, callback])`

Closes the consumer and all its connections.

If `force` is not `true`, then the method will throw an error if any `MessagesStream` created from this consumer is still active.

If `force` is `true`, then the method will close all `MessagesStream` created from this consumer.

The return value is `void`.

## Advanced Methods

The consumer manages group participation automatically. Some of the APIs are exposed to allow for advanced uses.

### `fetch<Key, Value, HeaderKey, HeaderValue>(options[, callback])`

Fetches messages directly from a specific node without allocating a stream.

The return value is the raw response from Kafka.

Options:

| Property       | Type                  | Default          | Description                                                                                                                                                                              |
| -------------- | --------------------- | ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| node           | number                |                  | Node to consume data from.                                                                                                                                                               |
| topics         | `FetchRequestTopic[]` |                  | Topic and partitions to consume.                                                                                                                                                         |
| minBytes       | `number`              | `1`              | Minimum amount of data the brokers should return. The value might not be respected by Kafka.                                                                                             |
| maxBytes       | `number`              | 10MB             | Maximum amount of data the brokers should return. The value might not be respected by Kafka.                                                                                             |
| maxWaitTime    | `number`              | 5 seconds        | Maximum amount of time in milliseconds the broker will wait before sending a response to a fetch request.                                                                                |
| isolationLevel | `string`              | `READ_COMMITTED` | Kind of isolation applied to fetch requests. It can be used to only read producers-committed messages.<br/><br/> The valid values are defined in the `FetchIsolationLevels` enumeration. |

Note that all the partitions must be hosted on the node otherwise the operation will fail.

### `commit(options[, callback])`

Commits offsets for consumed messages.

The return value is `void`.

Options:

| Property | Type                       | Description                                                                                                             |
| -------- | -------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| offsets  | `CommitOptionsPartition[]` | Offsets to commit. Each offset is an object containing the `topic`, `partition`, `offset` and `leaderEpoch` properties. |

### `listOffsets(options[, callback])`

Lists available offsets for topics.

If the topics list is empty, then it will fetch the offsets of all topics currently consumed by the consumer.

The return value is a map where keys are in the form `$topic:$partition` and values are arrays of offsets (where the position represents the partition).

Options:

| Property       | Type                       | Description                                                                                                                                                                                                                                            |
| -------------- | -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| topics         | `string[]`                 | Topics to check.                                                                                                                                                                                                                                       |
| partition      | `Record<string, number[]>` | Partitions to get for each topic. By default it fetches all the partitions of the topic.                                                                                                                                                               |
| timestamp      | `bigint`                   | Timestamp of the offsets to retrieve.<br/><br/> If it is `-1`, it will return the last available offset; if it is `-2`, it will return the first available offset.<br/><br/> These special values are also defined in the `ListOffsetTimestamps` enum. |
| isolationLevel | `string`                   | Kind of isolation applied to fetch requests. It can be used to only read producers-committed messages.<br/><br/> The valid values are defined in the `FetchIsolationLevels` enumeration. Default is `READ_COMMITTED`.                                  |

### `listCommittedOffsets(options[, callback])`

Lists offsets committed by the consumer group.

The return value is a map where keys are in the form `$topic:$partition` and values are arrays of offsets (where the position represents the partition).

Options:

| Property | Type       | Description      |
| -------- | ---------- | ---------------- |
| topics   | `string[]` | Topics to check. |

### `getLag(options[, callback])`

Calculates the consumer lag for specified topics.

If the topics list is empty, then it will calculate the lag of all topics currently consumed by the consumer.

The return value is a map where keys are topic names and values are arrays of lag values (where the position represents the partition). A value of `-1n` indicates that the consumer is not assigned to that partition.

If a partition is filtered out via the `partitions` option, then a `-2n` will be returned for that partition.

Options:

| Property   | Type                       | Description                                                                              |
| ---------- | -------------------------- | ---------------------------------------------------------------------------------------- |
| topics     | `string[]`                 | Topics to check lag for.                                                                 |
| partitions | `Record<string, number[]>` | Partitions to get for each topic. By default it fetches all the partitions of the topic. |

### `startLagMonitoring(options, interval)`

Initiates periodic consumer lag monitoring at the specified `interval` in milliseconds.

Consumer lag data is automatically emitted through `consumer:lag` events at each monitoring cycle.

Monitoring continues until explicitly stopped with `stopLagMonitoring` or automatically terminates when the consumer closes.

The `options` parameter accepts the same configuration as `getLag`.

### `stopLagMonitoring()`

Terminates the periodic consumer lag monitoring that was previously activated with `startLagMonitoring`.

### `findGroupCoordinator([callback])`

Finds the coordinator for the consumer group.

The return value is the number of the node acting as a coordinator for the group.

### `joinGroup(options[, callback])`

Joins (and creates if necessary) a consumer group.

It returns the group member ID for this consumer.

This method is no-op for `groupProtocol=consumer`.

Options:

| Property          | Type                          | Default                   | Description                                                                                                                                                                  |
| ----------------- | ----------------------------- | ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| sessionTimeout    | `number`                      | 1 minute                  | Amount of time in milliseconds to wait for a consumer to send the heartbeat before considering it down.<br/><br/> This is only relevant when Kafka creates a new group.      |
| rebalanceTimeout  | `number`                      | 2 minutes                 | Amount of time in milliseconds to wait for a consumer to confirm the rebalancing before considering it down.<br/><br/> This is only relevant when Kafka creates a new group. |
| heartbeatInterval | `number`                      | 3 seconds                 | Interval in milliseconds between heartbeats.                                                                                                                                 |
| protocols         | `GroupProtocolSubscription[]` | `roundrobin`, version `1` | Protocols used by this consumer group.<br/><br/> Each protocol must be an object specifying the `name`, `version` and optionally `metadata` properties.                      |

### `leaveGroup([force])`

Leaves a consumer group.

If `force` is not `true`, then the method will throw an error if any `MessagesStream` created from this consumer is still active.

The return value is `void`.

This method is no-op for `groupProtocol=consumer`.

## FAQs

### My consumer is not receiving any message when the application restarts

If you use a fixed consumer group ID (or one that is persisted between restarts), you might encounter a rebalance issue if the application is closed without properly shutting down the consumer or issuing a leaveGroup request.

This commonly happens during development (for example, when you terminate the process with Ctrl+C or use a file watcher), or if the application crashes in production.

When the application exits without leaving the group, Kafka still considers the consumer as potentially alive. Upon restarting with the same group ID but a different member ID, Kafka triggers a group rebalance. Since the previous member did not formally leave, Kafka waits for the full session timeout (1 minute by default) to determine whether the old member is still active.

Only after this timeout expires and the member is considered dead will Kafka complete the rebalance and assign partitions to the new instance. During this period, the restarted consumer cannot consume any messages, even if it is the only member in the group.

If you encounter this issue, there are two ways to easily avoid it:

1. If don't really use consumer groups, just use a random consumer group (like [crypto.randomUUID](https://nodejs.org/dist/latest/docs/api/crypto.html#cryptorandomuuidoptions)) as your group ID.
2. If you use consumer groups, manually call `joinGroup`, rather than relying on `consume` to handle it automatically.

### How do blocking operations impact consuming of messages?

Kafka requires consumers to regularly report their status using a dedicated heartbeat API. If a consumer fails to send heartbeats within the configured session timeout, Kafka considers it dead. When this happens, the consumer must rejoin the group, during which time it cannot consume messages. Moreover, all other group members must also rejoin due to the triggered rebalance, resulting in a temporary halt in message consumption across the group.

We've identified three common scenarios that can lead to missed heartbeats while consuming messages:

- Long fetch durations – If the consumer fetches messages with a maxWaitTime that exceeds the sessionTimeout, heartbeats may not be processed—even if they are sent—because the fetch API is blocking. @platformatic/kafka solves this by using a separate connection dedicated exclusively to fetch operations, ensuring that other APIs (such as heartbeats) remain unaffected.

- Complex asynchronous message processing – When user code performs long or delayed async tasks while handling messages, it may block the driver from sending timely heartbeats. If the client does not decouple fetching from processing, heartbeats can be lost. Solutions like KafkaJS require users to manually send heartbeats during processing, which is error-prone. In contrast, @platformatic/kafka fully decouples message fetching from processing to prevent this issue entirely.

- CPU-intensive operations – In Node.js, blocking the event loop with heavy computations prevents any I/O operations, including heartbeats. If such operations are unavoidable, the recommended solution is to offload them to a separate thread using Node.js Worker Threads.

### How do I handle reconnections when the connections fail or the brokers crash?

By default, `@platformatic/kafka` retries an operation three times with a waiting time of 250 milliseconds (these are the default of the `retries` and `retryDelay` options). This is to ensure that connection error are properly reported to the user. If the operation fails, the `error` event is emitted on the stream. After that, the stream is no longer usable, but the consumer still is (thanks to the internal decoupled architecture) and therefore you just have to invoke `consume` again to obtain a new stream and start processing messages again.

If you instead want it to handle reconnections automatically, you just have to change the values of `retries` to a higher value or `true` to set it to "infinite retries". Also, increase `retryDelay` to at least a second to avoid opening too many TCP connections in short period of time if the broker is down.

You can change those values in the `Consumer` constructor (and, similarly, to any other `@platformatic/kafka` client).

### Why does my server rejects my request even if SASL authentication succeeded?

Some Kafka servers (such as those using JWT tokens) may return a successful exit code (0) even when invalid credentials are provided, storing authentication information only in auth bytes rather than the exit code.

In such cases, you can provide the `sasl.authBytesValidator` option to validate authentication information from the auth bytes response rather than relying on the exit code alone.

For JWT tokens we already provide `saslOAuthBearer.jwtValidateAuthenticationBytes`.
