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

| Property      | Type                 | Description                                                                                                  |
| ------------- | -------------------- | ------------------------------------------------------------------------------------------------------------ |
| `topics`      | `string[]`           | Topics to create.                                                                                            |
| `partitions`  | `number`             | Number of partitions for each topic.                                                                         |
| `replicas`    | `number`             | Number of replicas for each topic.                                                                           |
| `assignments` | `BrokerAssignment[]` | Assignments of partitions.<br/><br/> Each assignment is an object with `partition` and `brokers` properties. |

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

### `close([callback])`

Closes the admin and all its connections.

The return value is `void`.
