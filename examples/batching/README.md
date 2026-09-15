# Batch Consumption

This example shows how to process Kafka messages in batches without a dedicated batch API in the client. It uses
the [`batchIterator`](https://www.npmjs.com/package/hwp) operator from `hwp` on the `MessagesStream` returned by the
consumer.

Start Kafka locally and run:

```bash
KAFKA_BROKER=localhost:9092 ./scripts/node examples/batching/index.ts
```

`KAFKA_BROKER` is optional and defaults to `localhost:9092`.

The example creates batches of up to three messages and flushes an incomplete batch after one second. It disables
autocommit and commits the next offset for each topic-partition after the complete batch has been processed. If
processing fails before the commit, the messages in that batch can be delivered again after the consumer restarts.

Batching remains an application-level operation over the stream, so the client does not need to expose a second
consumption API with batch-specific lifecycle and offset semantics.
