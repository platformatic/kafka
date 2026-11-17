import { deepStrictEqual, strictEqual } from 'node:assert'
import { test } from 'node:test'
import { type GroupOptions, roundRobinAssigner } from '../../../src/index.ts'
import { createConsumer, createTopic } from '../../helpers.ts'

for (const invalidMembership of ['generation', 'member']) {
  for (const initialJoin of ['consume', 'joinGroup']) {
    test(`commit rejoin preserves ${initialJoin} group options after invalid ${invalidMembership}`, async t => {
      const consumer = createConsumer(t, { autocommit: false })
      const topic = await createTopic(t, true, 1)
      const userData = Buffer.from('custom-metadata')
      let metadataCalls = 0
      let assignerCalls = 0
      let selectorCalls = 0

      const options: GroupOptions = {
        protocols: [{ name: 'custom-protocol', version: 1 }],
        sessionTimeout: 10000,
        rebalanceTimeout: 12000,
        heartbeatInterval: 1000,
        heartbeatStallTimeout: 15000,
        protocolsMetadata () {
          metadataCalls++
          return userData
        },
        partitionAssigner (current, members, topics, metadata) {
          assignerCalls++
          strictEqual(members.get(current)!.metadata!.toString(), userData.toString())
          return roundRobinAssigner(current, members, topics, metadata)
        },
        partitionAssignerTopicsSelector () {
          selectorCalls++
          return [topic]
        }
      }

      if (initialJoin === 'consume') {
        const stream = await consumer.consume({ ...options, topics: [topic], autocommit: false })
        await stream.close()
        consumer.topics.track(topic)
      } else {
        consumer.topics.track(topic)
        await consumer.joinGroup(options)
      }

      const initialMetadataCalls = metadataCalls
      const initialAssignerCalls = assignerCalls
      const initialSelectorCalls = selectorCalls
      let rejoins = 0
      consumer.on('consumer:group:join', () => {
        rejoins++
      })

      // Let Kafka reject stale membership on the commit; do not mock protocol responses.
      if (invalidMembership === 'generation') {
        consumer.generationId++
      } else {
        consumer.memberId = 'unknown-member'
      }

      await consumer.commit({ offsets: [{ topic, partition: 0, offset: 10n, leaderEpoch: -1 }] })

      // Heartbeats can also detect stale membership and trigger an additional rejoin.
      strictEqual(rejoins >= 1, true)
      strictEqual(metadataCalls > initialMetadataCalls, true)
      strictEqual(assignerCalls > initialAssignerCalls, true)
      strictEqual(selectorCalls > initialSelectorCalls, true)
      deepStrictEqual(consumer.assignments, [{ topic, partitions: [0] }])

      const committed = await consumer.listCommittedOffsets({ topics: [{ topic, partitions: [0] }] })
      deepStrictEqual(committed.get(topic), [10n])

      // Explicit joins must still resolve omitted options from the constructor.
      const callsBeforeExplicitJoin = [metadataCalls, assignerCalls, selectorCalls]
      await consumer.leaveGroup()
      await consumer.joinGroup()
      deepStrictEqual([metadataCalls, assignerCalls, selectorCalls], callsBeforeExplicitJoin)
    })
  }
}
