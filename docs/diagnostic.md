# Diagnostic

Support for diagnostic and instrumentation is provided via [Node.js Diagnostic Channel API](https://nodejs.org/dist/latest/docs/api/diagnostics_channel.html).

## Execution model for tracing channels

The presence of the `result` and `error` properties and the order of tracing channels events follows the current pseudo-code flow:

```javascript
function operation (options, callback) {
  const channel = tracingChannel('plt:kafka:example')
  const context = { operationId: 0n, options }

  try {
    channel.start.publish(context)

    doSomethingAsync((error, result) => {
      if (error) {
        context.error = error
        channel.error.publish(context)
        channel.asyncStart.publish(context)
        callback(error)
        channel.asyncEnd.publish(context)

        return
      }

      context.result = result
      channel.asyncStart.publish(context)
      callback(error)
      channel.asyncEnd.publish(context)
    })

    channel.end.publish(context)
  } catch (error) {
    context.error = error
    channel.error.publish(context)
    channel.end.publish(context)
  }
}
```

## Common properties

Each tracing channel publishes events with the following common properties:

| Name             | Type                     | Description                                                                                                                |
| ---------------- | ------------------------ | -------------------------------------------------------------------------------------------------------------------------- |
| `connection`     | `Connection`             | The current connection. This is only used by the `plt:kafka:connections:connects` and `plt:kafka:connections:api` channels |
| `connectionPool` | `ConnectionPool`         | The current connection pool. This is only used by the `plt:kafka:connections:pools:gets` channel.                          |
| `client`         | Depends on the operation | The current client. It can be a `Base`, `Admin`, `Producer` or `Consumer` if appropriate.                                  |
| `operationId`    | `bigint`                 | The current operation ID. This is unique across all channels.                                                              |
| `result`         | Depends on the operation | The result of the operation. This is only present in the `asyncStart` and `asyncEnd` events.                               |
| `error`          | `Error`                  | The error thrown by the operation. This is only present in the `error` event.                                              |

## Published channels

| Name                     | Description                                                                                                                                                                                        |
| ------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `plt:kafka:instances`    | Notifies any creation of a `Connection`, `ConnectionPool`, `Base`, `Admin`, `Producer`, `Consumer` or `MessagesStream`. This channel will publish objects with the `type` and `instance` property. |
| `plt:kafka:consumer:lag` | Notifies any `Consumer` lag obtained via `Consumer.getLag` (including the one triggered via `Consumer.startLagMonitoring`).                                                                          |

## Published tracing channels

| Name                                | Target           | Description                                                                                                                  |
| ----------------------------------- | ---------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `plt:kafka:connections:connects`    | `Connection`     | Traces a connection attempt to a broker.                                                                                     |
| `plt:kafka:connections:api`         | `Connection`     | Traces a low level API request.                                                                                              |
| `plt:kafka:connections:pools:gets`  | `ConnectionPool` | Traces a connection retrieval attempt from a connection pool.                                                                |
| `plt:kafka:base:apis`               | `Base`           | Traces a `Base.listApis` request.                                                                                            |
| `plt:kafka:base:metadata`           | `Base`           | Traces a `Base.metadata` request.                                                                                            |
| `plt:kafka:admin:topics`            | `Admin`          | Traces a `Admin.createTopics` or `Admin.deleteTopics` request.                                                               |
| `plt:kafka:admin:groups`            | `Admin`          | Traces a `Admin.listGroups`, `Admin.describeGroups`, `Admin.deleteGroups` or `Admin.removeMembersFromConsumerGroup` request. |
| `plt:kafka:admin:clientQuotas`      | `Admin`          | Traces a `Admin.describeClientQuotas` or `Admin.alterClientQuotas` request.                                                  |
| `plt:kafka:admin:logDirs`           | `Admin`          | Traces a `Admin.describeLogDirs` request.                                                                                    |
| `plt:kafka:producer:initIdempotent` | `Producer`       | Traces a `Producer.initIdempotentProducer` request.                                                                          |
| `plt:kafka:producer:sends`          | `Producer`       | Traces a `Producer.send` request.                                                                                            |
| `plt:kafka:consumer:group`          | `Consumer`       | Traces a `Consumer.findGroupCoordinator`, `Consumer.joinGroup` or `Consumer.leaveGroup` requests.                            |
| `plt:kafka:consumer:heartbeat`      | `Consumer`       | Traces the `Consumer` heartbeat requests.                                                                                    |
| `plt:kafka:consumer:receives`       | `Consumer`       | Traces processing of every message.                                                                                          |
| `plt:kafka:consumer:fetches`        | `Consumer`       | Traces a `Consumer.fetch` request.                                                                                           |
| `plt:kafka:consumer:consumes`       | `Consumer`       | Traces a `Consumer.consume` request.                                                                                         |
| `plt:kafka:consumer:commits`        | `Consumer`       | Traces a `Consumer.commit` request.                                                                                          |
| `plt:kafka:consumer:offsets`        | `Consumer`       | Traces a `Consumer.listOffsets` or `Consumer.listCommittedOffsets` request.                                                  |
