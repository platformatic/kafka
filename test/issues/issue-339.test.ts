// Related issue: https://github.com/platformatic/kafka/issues/339

import { deepStrictEqual, ok } from 'node:assert'
import { once } from 'node:events'
import { test, type TestContext } from 'node:test'
import { type Consumer, type GroupAssignment, MessagesStreamModes } from '../../src/index.ts'
import { createConsumer, createGroupId, createTopic, executeWithTimeout, isKafka, retry } from '../helpers.ts'

// Under the consumer group protocol (KIP-848) the new assignment is delivered in the
// ConsumerGroupHeartbeat response, so the REBALANCE_IN_PROGRESS error that drives the
// emission in #getRejoinError never happens: a member losing partitions to a new member
// used to receive no event at all, leaving manual-commit consumers no chance to flush
// their offsets before the partitions were taken away.

const skipConsumerGroupProtocol = { skip: isKafka(['7.5.0', '7.6.0', '7.7.0', '7.8.0', '7.9.0']) }
const rebalanceTimeout = 30000

function assignedPartitions (consumer: Consumer, topic: string): number[] {
  return consumer.assignments?.find(assignment => assignment.topic === topic)?.partitions ?? []
}

function waitForAssignedPartitions (consumer: Consumer, topic: string, expected: number): Promise<void> {
  return retry(60, 500, async () => {
    const partitions = assignedPartitions(consumer, topic)

    if (partitions.length !== expected) {
      throw new Error(`Expected ${expected} assigned partitions, got ${partitions.length}.`)
    }
  })
}

// joinGroup is a no-op under the consumer protocol: membership only starts once the
// consumer is actually consuming.
async function joinByConsuming (t: TestContext, consumer: Consumer, topic: string): Promise<void> {
  const stream = await consumer.consume({ topics: [topic], mode: MessagesStreamModes.LATEST, maxWaitTime: 200 })
  t.after(() => stream.close())
  stream.on('data', () => {})
}

test('should emit consumer:group:rebalance when partitions are revoked', skipConsumerGroupProtocol, async t => {
  const topic = await createTopic(t, true, 3)
  const groupId = createGroupId()

  const consumer1 = createConsumer(t, { groupId, groupProtocol: 'consumer' })
  const consumer2 = createConsumer(t, { groupId, groupProtocol: 'consumer' })

  await joinByConsuming(t, consumer1, topic)
  await waitForAssignedPartitions(consumer1, topic, 3)

  // Record the assignment as seen by the event handler: the event must be delivered
  // before the revocation is applied, otherwise there is nothing left to commit.
  const assignmentsWhenNotified: (GroupAssignment[] | null)[] = []
  consumer1.on('consumer:group:rebalance', () => {
    assignmentsWhenNotified.push(structuredClone(consumer1.assignments))
  })

  const rebalance = once(consumer1, 'consumer:group:rebalance')

  await joinByConsuming(t, consumer2, topic)

  const payloads = await executeWithTimeout(rebalance, rebalanceTimeout)
  ok(Array.isArray(payloads), 'consumer1 was not notified about the revocation.')
  deepStrictEqual(payloads[0], { groupId })

  deepStrictEqual(
    assignmentsWhenNotified[0],
    [{ topic, partitions: [0, 1, 2] }],
    'The event must be emitted while the revoked partitions are still assigned.'
  )

  // The revocation is then actually applied and the group settles on a shared assignment.
  await retry(60, 500, async () => {
    const partitions1 = assignedPartitions(consumer1, topic)
    const partitions2 = assignedPartitions(consumer2, topic)

    if (partitions1.length === 3 || partitions1.length + partitions2.length !== 3) {
      throw new Error(`Partitions were not redistributed: ${partitions1.length} and ${partitions2.length}.`)
    }
  })
})
