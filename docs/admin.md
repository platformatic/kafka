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

### `createPartitions(options[, callback])`

Creates additional partitions for existing topics.

The return value is `void`.

Options:

| Property      | Type                             | Description                                                                                            |
| ------------- | -------------------------------- | ------------------------------------------------------------------------------------------------------ |
| `topics`      | `CreatePartitionsRequestTopic[]` | Topics to create partitions for with information about partition count and assignments for each topic. |
| `validateOnly`| `boolean`                        | Whether to only validate the request without applying changes. Defaults to `false`.                    |

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

### `removeMembersFromConsumerGroup(options[, callback])`

Removes members from a consumer group.

The return value is `void`.

Options:

| Property | Type                                  | Description                                                                                                                                                               |
| -------- | ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| groupId  | `string`                              | The consumer group ID.                                                                                                                                                    |
| members  | `(string \| MemberRemoval)[] \| null` | Array of member IDs to remove (as strings), or an array of `MemberRemoval` objects with `memberId` and optional `reason`, or `null` to remove all members from the group. |

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

### `listConsumerGroupOffsets(options[, callback])`

Lists committed offsets for consumer groups.

The return value is an array of groups with information about committed offsets per partition per topic.

Options:

| Property      | Type                        | Description                                                                 |
| ------------- | --------------------------- | --------------------------------------------------------------------------- |
| groups        | `OffsetFetchRequestGroup[]` | Array of groups specifying the groups and topics for which to list offsets. |
| requireStable | `boolean`                   | Whether to require stable offsets.                                          |

### `alterConsumerGroupOffsets(options[, callback])`

Alters committed offsets for a consumer group. Note, that the consumer group must be empty to succeed.

The return value is `void`.

Options:

| Property | Type                               | Description                                                  |
| -------- | ---------------------------------- | ------------------------------------------------------------ |
| groupId  | `string`                           | The consumer group ID for which to alter offsets.            |
| topics   | `AlterConsumerGroupOffsetsTopic[]` | Array of topics with partitions and their new offset values. |

### `deleteConsumerGroupOffsets(options[, callback])`

Deletes committed offsets for specific topics of a consumer group.

The return value is an array of topic partitions indicating which offsets were deleted.

Options:

| Property | Type                                             | Description                                                 |
| -------- | ------------------------------------------------ | ----------------------------------------------------------- |
| groupId  | `string`                                         | The consumer group ID from which to delete offsets.         |
| topics   | `{ name: string, partitionIndexes: number[] }[]` | Array of topics and partitions for which to delete offsets. |

### `close([callback])`

Closes the admin and all its connections.

The return value is `void`.
