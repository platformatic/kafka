# Batch Consumption

This example uses the [`batchIterator`](https://www.npmjs.com/package/hwp) operator from `hwp` to batch messages
across partitions or release an incomplete batch after a timeout. For batches from a single topic-partition and
fetch response, use the client's [native `consume({ batch })` option](../../docs/consumer.md#batch-consumption).

Start Kafka locally and run:

```bash
KAFKA_BROKER=localhost:9092 ./scripts/node examples/batching/index.ts
```

`KAFKA_BROKER` is optional and defaults to `localhost:9092`.

The example creates batches of up to three messages and flushes an incomplete batch after one second. It disables
autocommit and commits the next offset for each topic-partition after the complete batch has been processed. If
processing fails before the commit, the messages in that batch can be delivered again after the consumer restarts.
