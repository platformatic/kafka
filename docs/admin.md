# Admin

Client for administrative operations like creating/deleting topics and managing consumer groups.

The admin inherits from the [`Base`](./base.md) client.

## Constructor

Creates a new admin.

It supports all the constructor options of `Base`.

## Methods

### `listTopics([options[, callback]])`

List all topics available on the cluster.

The return value is a list of available topics.

Options:

| Property           | Type      | Description                                                    |
| ------------------ | --------- | -------------------------------------------------------------- |
| `includeInternals` | `boolean` | Whether to include internal Kafka topics in the returned list. |

### `createTopics(options[, callback])`

Creates one or more topics.

The return value is a list of created topics, each containing `id`, `name`, `partitions`, `replicas` and `configuration` properties.

Options:

| Property      | Type                               | Description                                                                                                  |
| ------------- | ---------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| `topics`      | `string[]`                         | Topics to create.                                                                                            |
| `partitions`  | `number`                           | Number of partitions for each topic.                                                                         |
| `replicas`    | `number`                           | Number of replicas for each topic.                                                                           |
| `assignments` | `BrokerAssignment[]`               | Assignments of partitions.<br/><br/> Each assignment is an object with `partition` and `brokers` properties. |
| `configs`     | `CreateTopicsRequestTopicConfig[]` | Topic configurations.<br/><br/> Each configuration is an object with `name` and `value` properties.          |

### `deleteTopics(options[, callback])`

Deletes one or more topics.

The return value is `void`.

Options:

| Property | Type       | Description       |
| -------- | ---------- | ----------------- |
| `topics` | `string[]` | Topics to delete. |

### `listGroups(options[, callback])`

Lists consumer groups.

The return value is a list of groups, each containing the `id`, `state`, `groupType` and `protocolType` properties.

Options:

| Property | Type                   | Description                                                                                                     |
| -------- | ---------------------- | --------------------------------------------------------------------------------------------------------------- |
| states   | `ConsumerGroupState[]` | States of the groups to return.<br/><br/>The valid values are defined in the `ConsumerGroupStates` enumeration. |
| types    | `string[]`             | Types of the groups to return.<br/><br/>Default is `['consumer']`.                                              |

### `describeGroups(options[, callback])`

Gets detailed information about consumer groups.

The return value is a map where keys are group names and values are the detailed group information.

Options:

| Property | Type                          | Description                                                                                 |
| -------- | ----------------------------- | ------------------------------------------------------------------------------------------- |
| groups   | `string[]`                    | Groups to describe.                                                                         |
| types    | `includeAuthorizedOperations` | Whether to include authorisation information in the response.<br/><br/> Default is `false`. |

### `deleteGroups(options[, callback])`

Deletes one or more consumer groups.

The return value is `void`.

Options:

| Property | Type       | Description       |
| -------- | ---------- | ----------------- |
| groups   | `string[]` | Groups to delete. |

### `describeClientQuotas(options[, callback])`

Gets detailed information about client quotas.

The return value is an object specifying quotas for the requested user/client combination.

Options:

| Property   | Type                                          | Description                                                                                              |
| ---------- | --------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| components | `DescribeClientQuotasRequestComponent[]`      | Array of components specifying the entity types and match criteria for which to describe client quotas.  |
| strict     | `boolean`                                     | Whether to use strict matching for components. Defaults to `false`.                                      |

### `alterClientQuotas(options[, callback])`

Alters client quotas for specified entities.

The return value is a list of entities for which quotas have been changed.

Options:

| Property     | Type                              | Description                                                                               |
| ------------ | --------------------------------- | ----------------------------------------------------------------------------------------- |
| entries      | `AlterClientQuotasRequestEntry[]` | Array of entries specifying the entities and quotas to change.                            |
| validateOnly | `boolean`                         | Whether to only validate the request without applying changes. Defaults to `false`.       |

### `describeLogDirs(options[, callback])`

Describes log directories for specified topics across all brokers.

The return value is an array of broker log directory descriptions, each containing information one broker's log directories.

Options:

| Property | Type                            | Description                                                                      |
| -------- | ------------------------------- | -------------------------------------------------------------------------------- |
| topics   | `DescribeLogDirsRequestTopic[]` | Array of topics specifying the topics and partitions for which to describe logs. |

### `listOffsets(options[, callback])`

Lists offsets for specified topic partitions.

The return value is an array of topics, each containing partition offset information including the offset, timestamp, partition index, and leader epoch.

Options:

| Property       | Type                       | Description                                                                                                                                                           |
| -------------- | -------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| topics         | `TopicOffsetRequest[]`     | Array of topics with their partitions to query offsets for. Each topic has `name` and `partitions` (array of `{ partitionIndex, timestamp }`).                        |
| isolationLevel | `IsolationLevel` \| `null` | Isolation level for reading offsets. Valid values are `IsolationLevels.READ_UNCOMMITTED` (0) or `IsolationLevels.READ_COMMITTED` (1). Defaults to `READ_UNCOMMITTED`. |

Example:

```typescript
const offsets = await admin.listOffsets({
  topics: [
    {
      name: 'my-topic',
      partitions: [
        { partitionIndex: 0, timestamp: -1n }, // Latest offset
        { partitionIndex: 1, timestamp: -2n }  // Earliest offset
      ]
    }
  ],
  isolationLevel: IsolationLevels.READ_COMMITTED
})

// offsets is an array of AdminListedOffsetsTopic:
// [
//   {
//     name: 'my-topic',
//     partitions: [
//       { partitionIndex: 0, offset: 12345n, timestamp: 1234567890n, leaderEpoch: 1 },
//       { partitionIndex: 1, offset: 100n, timestamp: 1234567800n, leaderEpoch: 1 }
//     ]
//   }
// ]
```

Special timestamp values:
- `-1n` - Get the latest offset (end of the log)
- `-2n` - Get the earliest offset (beginning of the log)
- Any other positive value - Get the offset at or after the specified timestamp

### `close([callback])`

Closes the admin and all its connections.

The return value is `void`.
