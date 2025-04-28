# Metrics

`@platformatic/kafka` supports integration with [Prometheus](https://prometheus.io/) via [prom-client](https://github.com/siimon/prom-client).

To ensure maximum compatibility, no `prom-client` version is shipped with the package but you are instead required to provide your own via `metrics.client` option when creating any client.

If you provide both the `metrics.client` and `metrics.registry` (an instance of `Registry`) options, then `@platformatic/kafka` will register and provide the following metrics:

| Name                      | Type      | Description                               |
| ------------------------- | --------- | ----------------------------------------- |
| `kafka_producers`         | `Gauge`   | Number of active Kafka producers.         |
| `kafka_produced_messages` | `Counter` | Number of produced Kafka messages.        |
| `kafka_consumers`         | `Gauge`   | Number of active Kafka consumers.         |
| `kafka_consumers_streams` | `Gauge`   | Number of active Kafka consumers streams. |
| `kafka_consumers_topics`  | `Gauge`   | Number of topics being consumed.          |
| `kafka_consumed_messages` | `Counter` | Number of consumed Kafka messages.        |

Optionally, you can provide labels with the `metrics.label` option.

## Example

```javascript
import * as client from 'prom-client'
import { Producer, stringSerializer } from '@platformatic/kafka'

const registry = new client.Registry()

// Create a producer with string serialisers
const producer = new Producer({
  clientId: 'my-producer',
  bootstrapBrokers: ['localhost:9092'],
  serializers: stringSerializers,
  metrics: { client, registry }
})

// Send messages
await producer.send({
  messages: [
    {
      topic: 'events',
      key: 'user-123',
      value: JSON.stringify({ name: 'John', action: 'login' }),
      headers: { source: 'web-app' }
    }
  ]
})

// Close the producer when done
await producer.close()

console.log(JSON.stringify(await registry.getMetricsAsJSON(), null, 2))
```
