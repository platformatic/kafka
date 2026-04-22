import { strictEqual } from 'node:assert'
import { test } from 'node:test'
import { GroupProtocols, MessagesStreamFallbackModes, MessagesStreamModes } from '../../src/index.ts'
import {
  assertCommittedOffset,
  assertContiguousIds,
  assertNoDuplicateIds,
  collectMessages,
  createConsumer,
  createGroupId,
  createTopic,
  produceJsonMessages,
  type ConsumedValue
} from '../helpers/index.ts'

function jsonDeserializers () {
  // Keep deserialization local to this file because the test needs typed payloads
  // while using the consumer group protocol path.
  return {
    key: (data: Buffer | undefined) => data?.toString() ?? '',
    value: (data: Buffer | undefined) => JSON.parse(data?.toString() ?? '{}') as ConsumedValue,
    headerKey: (data: Buffer | undefined) => data?.toString() ?? '',
    headerValue: (data: Buffer | undefined) => data?.toString() ?? ''
  }
}

test('regression #267/#248: consumer protocol resumes from committed offsets after member epoch churn', { timeout: 90_000 }, async t => {
  // The first consumer commits half the topic, then leaves the group. The second
  // consumer rejoins with the consumer protocol and must resume from that commit.
  const topic = await createTopic(t, 1)
  const groupId = createGroupId()

  await produceJsonMessages(t, topic, 10)

  const firstConsumer = createConsumer<string, ConsumedValue, string, string>(t, {
    groupId,
    groupProtocol: GroupProtocols.CONSUMER,
    deserializers: jsonDeserializers()
  })
  await firstConsumer.topics.trackAll(topic)
  await firstConsumer.joinGroup()

  const firstStream = await firstConsumer.consume({
    topics: [topic],
    mode: MessagesStreamModes.EARLIEST,
    autocommit: false,
    maxWaitTime: 1000
  })
  const firstBatch = await collectMessages(firstStream, 5)
  await firstBatch.at(-1)!.commit()
  // Committed offsets are the next offset to read, so the expected value is the
  // last processed offset plus one.
  await assertCommittedOffset(firstConsumer, topic, 0, firstBatch.at(-1)!.offset + 1n)
  await firstConsumer.close(true)

  const secondConsumer = createConsumer<string, ConsumedValue, string, string>(t, {
    groupId,
    groupProtocol: GroupProtocols.CONSUMER,
    deserializers: jsonDeserializers()
  })
  await secondConsumer.topics.trackAll(topic)
  await secondConsumer.joinGroup()

  const secondStream = await secondConsumer.consume({
    topics: [topic],
    mode: MessagesStreamModes.COMMITTED,
    fallbackMode: MessagesStreamFallbackModes.EARLIEST,
    autocommit: false,
    maxWaitTime: 1000
  })
  const secondBatch = await collectMessages(secondStream, 5)

  // The combined stream from both members must be exactly the original sequence:
  // no stale epoch duplicate, no skipped records, and no rewind to offset 0.
  assertNoDuplicateIds([...firstBatch, ...secondBatch])
  assertContiguousIds([...firstBatch, ...secondBatch], 10)
  strictEqual(secondBatch[0]!.offset, firstBatch.at(-1)!.offset + 1n)
})
