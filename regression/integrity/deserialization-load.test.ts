import { strictEqual } from 'node:assert'
import { test } from 'node:test'
import { assertContiguousIds, assertNoDuplicateIds, consumeJsonMessages, createTopic, produceJsonMessages } from '../helpers/index.ts'

test('regression #227: heavy mixed-size JSON deserialization has no loss or duplicates', { timeout: 60_000 }, async t => {
  // Produce enough JSON records to exercise the deserialize path across multiple
  // fetch responses, then assert payload integrity and delivery invariants.
  const total = 250
  const topic = await createTopic(t, 1)

  await produceJsonMessages(t, topic, total)

  const { messages } = await consumeJsonMessages(t, topic, total)

  // These assertions catch both parser failures hidden by retries and delivery
  // bugs that would otherwise only show up as duplicate or skipped ids.
  assertNoDuplicateIds(messages)
  assertContiguousIds(messages, total)
  strictEqual(messages.every(message => message.value.payload === `payload-${message.value.id}`), true)
})
