# Share Consumer

Client for share groups ([KIP-932](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka)), which need Apache Kafka 4.2 or later.

The members of a share group read the same partitions together: the coordinator can assign a partition to more than one member, and each record is acknowledged on its own instead of committing an offset. A share group gives no ordering guarantees.

The share consumer currently manages the membership of the group. Fetching and acknowledging records will be added later.

The share consumer inherits from the [`Base`](./base.md) client.

## Events

| Name                             | Payload Type                         | Description                                                                                                            |
| -------------------------------- | ------------------------------------ | ---------------------------------------------------------------------------------------------------------------------- |
| `share-consumer:group:join`      | `ShareConsumerGroupPayload`          | Emitted when joining a group.                                                                                          |
| `share-consumer:group:leave`     | `ShareConsumerGroupPayload`          | Emitted when leaving a group.                                                                                          |
| `share-consumer:group:rejoin`    | `ShareConsumerHeartbeatErrorPayload` | Emitted when the coordinator does not know the member anymore. The next heartbeat joins again with the same member ID. |
| `share-consumer:heartbeat:start` | `ShareConsumerGroupPayload`          | Emitted when starting a heartbeat.                                                                                     |
| `share-consumer:heartbeat:end`   | `ShareConsumerGroupPayload`          | Emitted during successful heartbeats.                                                                                  |
| `share-consumer:heartbeat:error` | `ShareConsumerHeartbeatErrorPayload` | Emitted during failed heartbeats. The membership is kept and the next heartbeat is sent after the usual interval.      |

The payloads contain the `groupId`, the `memberId` and the `memberEpoch`. The error payloads also contain the `error`.

The first heartbeat of `joinGroup` emits the heartbeat events too, so a failed join also emits `share-consumer:heartbeat:error`.

## Constructor

Creates a new share consumer.

Options:

| Property          | Type     | Default | Description                                                                                                                                                        |
| ----------------- | -------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| groupId           | `string` |         | Share group ID.                                                                                                                                                    |
| leaveGroupTimeout | `number` | Unset   | Maximum time in milliseconds for the group leave step during `close()`. Set to `0` to skip the leave request. When omitted, the existing `requestTimeout` applies. |

It also supports all the constructor options of `Base`. The `clientRack` option is sent to the coordinator as the rack of the member.

The session timeout and the heartbeat interval are set on the broker, with the `group.share.session.timeout.ms` and `group.share.heartbeat.interval.ms` properties. The partitions are always assigned by the coordinator.

The member ID is generated when the share consumer is created and does not change while it exists.

## Properties

| Name            | Type                        | Description                                                                                        |
| --------------- | --------------------------- | -------------------------------------------------------------------------------------------------- |
| `groupId`       | `string`                    | Share group ID.                                                                                    |
| `memberId`      | `string`                    | Member ID of this share consumer.                                                                  |
| `memberEpoch`   | `number`                    | Member epoch sent by the coordinator, or `0` when the share consumer is not a member of the group. |
| `assignments`   | `GroupAssignment[] \| null` | Partitions assigned to this member, or `null` when it is not a member of the group or the assignment is not known yet. No event is emitted when it changes. |
| `coordinatorId` | `number \| null`            | Node ID of the group coordinator, once found.                                                      |

## Basic Methods

### `joinGroup(options[, callback])`

Joins (and creates if necessary) a share group, subscribing to the given topics.

Once joined, the share consumer sends a heartbeat at the interval set by the coordinator until it leaves the group or it is closed. The coordinator might send the assignment with a later heartbeat.

It returns the member ID for this share consumer.

It fails if the share consumer has already joined the group or is leaving it.

If the topics of the assignment cannot be resolved because the metadata cannot be loaded, the join still succeeds: the error is emitted with `share-consumer:heartbeat:error` and the assignment is resolved again after the next heartbeat.

Options:

| Property | Type       | Description             |
| -------- | ---------- | ----------------------- |
| topics   | `string[]` | Topics to subscribe to. |

### `leaveGroup([callback])`

Leaves the share group. If leaving fails, the share consumer is still a member of the group and keeps sending heartbeats.

Concurrent calls wait for the same request. Calling `joinGroup` while leaving fails.

The return value is `void`.

### `isActive()`

Returns `true` if the share consumer is a member of the group.

### `close([callback])`

Leaves the share group, if needed, and closes all the connections.

The `leaveGroupTimeout` constructor option bounds only the group leave step. On timeout, the share consumer closes its connections without waiting for the broker, which removes the member when its session times out.

The return value is `void`.
