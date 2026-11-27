# Share Consumer

Client for share groups ([KIP-932](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka)), which need Apache Kafka 4.2 or later. On brokers which do not support share groups, or have them disabled, `joinGroup` fails with an `UnsupportedApiError` or with an `UNSUPPORTED_VERSION` error.

The members of a share group read the same partitions together: the coordinator can assign a partition to more than one member, and each record is acknowledged on its own instead of committing an offset. A share group gives no ordering guarantees.

The records are acknowledged implicitly: a record is accepted once the stream hands it to the application. Explicit acknowledgements will be added later.

The share consumer inherits from the [`Base`](./base.md) client.

## Events

| Name                                   | Payload Type                               | Description                                                                                                                                                             |
| -------------------------------------- | ------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `share-consumer:group:join`            | `ShareConsumerGroupPayload`                | Emitted when joining a group.                                                                                                                                           |
| `share-consumer:group:leave`           | `ShareConsumerGroupPayload`                | Emitted when leaving a group.                                                                                                                                           |
| `share-consumer:group:rejoin`          | `ShareConsumerHeartbeatErrorPayload`       | Emitted when the coordinator does not know the member anymore. The next heartbeat joins again with the same member ID.                                                  |
| `share-consumer:heartbeat:start`       | `ShareConsumerGroupPayload`                | Emitted when starting a heartbeat.                                                                                                                                      |
| `share-consumer:heartbeat:end`         | `ShareConsumerGroupPayload`                | Emitted during successful heartbeats.                                                                                                                                   |
| `share-consumer:heartbeat:error`       | `ShareConsumerHeartbeatErrorPayload`       | Emitted during failed heartbeats. The membership is kept and the next heartbeat is sent after the usual interval.                                                       |
| `share-consumer:acknowledgement:error` | `ShareConsumerAcknowledgementErrorPayload` | Emitted when the broker refuses the acknowledgements of a partition, for instance because the acquisition lock of the records expired. The records are delivered again. |

The payloads contain the `groupId`, the `memberId` and the `memberEpoch`. The error payloads also contain the `error`, and the acknowledgement error payloads the `topic` and the `partition`.

The first heartbeat of `joinGroup` emits the heartbeat events too, so a failed join also emits `share-consumer:heartbeat:error`.

## Constructor

Creates a new share consumer.

Options:

| Property          | Type     | Default | Description                                                                                                                                                        |
| ----------------- | -------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| groupId           | `string` |         | Share group ID.                                                                                                                                                    |
| leaveGroupTimeout | `number` | Unset   | Maximum time in milliseconds for the group leave step during `close()`. Set to `0` to skip the leave request. When omitted, the existing `requestTimeout` applies. |

The share consumer also supports these options, which `consume` can override:

| Property      | Type            | Default            | Description                                                                                                                       |
| ------------- | --------------- | ------------------ | --------------------------------------------------------------------------------------------------------------------------------- |
| deserializers | `Deserializers` |                    | Deserializers for the keys, values and headers, as in the [`Consumer`](./consumer.md).                                            |
| minBytes      | `number`        | `1`                | Minimum amount of data the broker should return for a fetch.                                                                      |
| maxBytes      | `number`        | `52428800` (50 MB) | Maximum amount of data the broker should return for a fetch.                                                                      |
| maxWaitTime   | `number`        | `5000`             | Maximum time in milliseconds the broker waits for `minBytes` of records.                                                          |
| maxRecords    | `number`        | `500`              | Maximum number of records the broker should acquire for a fetch. The broker can return more, as it acquires whole record batches. |
| highWaterMark | `number`        | `maxRecords`       | Number of records the stream buffers before it stops fetching.                                                                    |

It also supports all the constructor options of `Base`. The `clientRack` option is sent to the coordinator as the rack of the member.

The session timeout and the heartbeat interval are set on the broker, with the `group.share.session.timeout.ms` and `group.share.heartbeat.interval.ms` properties. The partitions are always assigned by the coordinator.

The member ID is generated when the share consumer is created and does not change while it exists.

## Properties

| Name            | Type                        | Description                                                                                                                                                 |
| --------------- | --------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `groupId`       | `string`                    | Share group ID.                                                                                                                                             |
| `memberId`      | `string`                    | Member ID of this share consumer.                                                                                                                           |
| `memberEpoch`   | `number`                    | Member epoch sent by the coordinator, or `0` when the share consumer is not a member of the group.                                                          |
| `assignments`   | `GroupAssignment[] \| null` | Partitions assigned to this member, or `null` when it is not a member of the group or the assignment is not known yet. No event is emitted when it changes. |
| `coordinatorId` | `number \| null`            | Node ID of the group coordinator, once found.                                                                                                               |

## Basic Methods

### `consume(options[, callback])`

Starts consuming records from the given topics and returns a `ShareMessagesStream`, a `Readable` stream of messages.

It joins the share group with the topics if needed. If the share consumer has already joined the group, the topics must be the same.

A member of a share group can only consume with one stream: it fails if another stream is still open. Once that stream is closed, `consume` can be called again.

Options:

| Property | Type       | Description             |
| -------- | ---------- | ----------------------- |
| topics   | `string[]` | Topics to consume from. |

It also supports the `deserializers`, `minBytes`, `maxBytes`, `maxWaitTime`, `maxRecords` and `highWaterMark` options of the constructor.

The messages have the `key`, `value`, `headers`, `headerEntries`, `topic`, `partition`, `timestamp` and `offset` properties of the messages of the [`Consumer`](./consumer.md), and a `deliveryCount` property with the number of times the record has been delivered to the group. They cannot be committed.

```typescript
import { ShareConsumer, stringDeserializers } from '@platformatic/kafka'

const consumer = new ShareConsumer({
  clientId: 'my-worker',
  groupId: 'my-share-group',
  bootstrapBrokers: ['localhost:9092'],
  deserializers: stringDeserializers
})

const stream = await consumer.consume({ topics: ['jobs'] })

for await (const message of stream) {
  await handle(message.value)
}
```

#### Acknowledgements

The records are acknowledged implicitly. A record is accepted once the stream hands it to the application, whether the stream is read with `for await`, a `data` listener or a pipe. The acknowledgement is sent with the next fetch to the same broker, or when the stream is closed. A fetch can wait up to `maxWaitTime` on the broker, so an acknowledgement can wait as long, and so can `close()`.

An accepted record is never delivered again to any member of the group. So if the processing of a record fails, or the process crashes while handling it, the record is lost.

The records which are still buffered in the stream when it is closed have not reached the application: they are released, so the group delivers them again. Their delivery count increases. Once it reaches the `group.share.delivery.count.limit` broker setting (5 by default), the record is archived and not delivered anymore.

The broker locks the records it hands to a member for the time set by the `group.share.record.lock.duration.ms` broker setting (30 seconds by default). The stream still delivers a record which waited in its buffer for longer than that, but by then the broker may already have released the record and delivered it to another member. Accepting it then fails with a `share-consumer:acknowledgement:error` event. Share groups deliver records at least once, so applications must be ready to handle the same record more than once. Use a lower `highWaterMark` or `maxRecords` when processing records takes long, and keep `maxWaitTime` well below the lock duration.

#### `ShareMessagesStream`

The stream fetches from the leader of each assigned partition. It follows the assignment the share consumer receives with its heartbeats, so it waits until the coordinator sends one.

Errors which can be retried are retried according to the `retries` and `retryDelay` options of the share consumer, including the errors of a single partition. Other errors, or a record which cannot be deserialized, destroy the stream with an error.

##### `close([callback])`

Closes the stream: the records still buffered are released and the share sessions on the brokers are closed, sending the pending acknowledgements. A `for await` loop reading the stream ends without errors.

The return value is `void`.

### `joinGroup(options[, callback])`

Joins (and creates if necessary) a share group, subscribing to the given topics.

Once joined, the share consumer sends a heartbeat at the interval set by the coordinator until it leaves the group or it is closed. The coordinator might send the assignment with a later heartbeat.

It returns the member ID for this share consumer.

It fails if the share consumer is joining the group, has already joined it, or is leaving it.

The topics do not need to exist: the coordinator assigns their partitions once they are created, and removes the partitions of a deleted topic from the assignment.

If the topics of the assignment cannot be resolved because the metadata cannot be loaded, the join still succeeds: the error is emitted with `share-consumer:heartbeat:error` and the assignment is resolved again after the next heartbeat.

Options:

| Property | Type       | Description             |
| -------- | ---------- | ----------------------- |
| topics   | `string[]` | Topics to subscribe to. |

### `leaveGroup([force][, callback])`

Leaves the share group.

If a stream is consuming and `force` is not `true`, it fails with an error. With `force`, the stream is closed first: its buffered records are released and its pending acknowledgements are sent, since the broker refuses the acknowledgements of a member which left the group.

If leaving fails the error is returned, but the share consumer stops sending heartbeats anyway and the coordinator removes the member when its session times out.

Concurrent calls wait for the same request. Calling `joinGroup` while leaving fails.

The return value is `void`.

### `isActive()`

Returns `true` if the share consumer is a member of the group.

### `close([callback])`

Closes the stream, if any, leaves the share group, if needed, and closes all the connections.

The `leaveGroupTimeout` constructor option bounds only the group leave step. On timeout, the share consumer closes its connections without waiting for the broker, which removes the member when its session times out.

The return value is `void`.
