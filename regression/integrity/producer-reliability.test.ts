import { strictEqual } from 'node:assert'
import { randomUUID } from 'node:crypto'
import { once } from 'node:events'
import { test } from 'node:test'
import { ProduceAcks } from '../../src/index.ts'
import {
  assertContiguousIds,
  assertNoDuplicateIds,
  consumeJsonMessages,
  createProducer,
  createTopic
} from '../helpers/index.ts'

test('regression: producer stream handles acks=0 backpressure without message loss', { timeout: 60_000 }, async t => {
  // acks=0 gives no broker acknowledgement, so this test stresses local stream
  // buffering and drain handling before verifying all records are readable.
  const total = 120
  const topic = await createTopic(t, 1)
  const producer = createProducer(t)
  const stream = producer.asStream({ batchSize: 10, batchTime: 1000, highWaterMark: 4, acks: ProduceAcks.NO_RESPONSE })

  for (let id = 0; id < total; id++) {
    const writable = stream.write({ topic, key: String(id), value: JSON.stringify({ id }) })
    if (!writable) {
      // Respect Writable backpressure. Ignoring drain would test Node buffering
      // more than the producer stream implementation.
      await once(stream, 'drain')
    }
  }

  await stream.close()

  const { messages } = await consumeJsonMessages(t, topic, total)

  assertNoDuplicateIds(messages)
  assertContiguousIds(messages, total)
})

test('regression: idempotent transaction commits exactly the produced records', { timeout: 60_000 }, async t => {
  // This is a compact transactional reliability check: committed records should be
  // visible exactly once, with no gaps, after the transaction completes.
  const total = 20
  const topic = await createTopic(t, 1)
  const producer = createProducer(t, {
    idempotent: true,
    strict: true,
    transactionalId: `regression-txn-${randomUUID()}`,
    retries: 0
  })
  const transaction = await producer.beginTransaction()

  await transaction.send({
    messages: Array.from({ length: total }, (_, id) => ({ topic, key: String(id), value: JSON.stringify({ id }) }))
  })
  await transaction.commit()

  const { messages } = await consumeJsonMessages(t, topic, total)

  assertNoDuplicateIds(messages)
  assertContiguousIds(messages, total)
  strictEqual(messages.length, total)
})
