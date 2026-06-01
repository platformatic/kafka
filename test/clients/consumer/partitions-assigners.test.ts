import { deepStrictEqual } from 'node:assert'
import { test } from 'node:test'
import {
  cooperativeStickyAssigner,
  type ClusterMetadata,
  type ExtendedGroupProtocolSubscription,
  type GroupPartitionsAssignments
} from '../../../src/index.ts'

const topic = 'topic-a'
const otherTopic = 'topic-b'

function createMetadata (partitionsCount: number): ClusterMetadata {
  return {
    id: 'cluster-id',
    brokers: new Map(),
    controllerId: 1,
    topics: new Map([
      [
        topic,
        {
          id: 'topic-id',
          partitions: Array.from({ length: partitionsCount }, () => ({
            leader: 1,
            leaderEpoch: 0,
            replicas: [1],
            isr: [1],
            offlineReplicas: []
          })),
          partitionsCount,
          lastUpdate: Date.now()
        }
      ]
    ]),
    lastUpdate: Date.now()
  }
}

function createMember (
  memberId: string,
  ownedPartitions: number[] = [],
  generationId = 1
): [string, ExtendedGroupProtocolSubscription] {
  return createMemberWithTopics(memberId, [topic], ownedPartitions.length > 0 ? [{ topic, partitions: ownedPartitions }] : [], generationId)
}

function createMemberWithTopics (
  memberId: string,
  topics: string[],
  ownedPartitions: { topic: string; partitions: number[] }[] = [],
  generationId = 1
): [string, ExtendedGroupProtocolSubscription] {
  return [
    memberId,
    {
      memberId,
      version: 3,
      topics,
      metadata: Buffer.alloc(0),
      ownedPartitions,
      generationId,
      rackId: null
    }
  ]
}

function createMultiTopicMetadata (): ClusterMetadata {
  const metadata = createMetadata(3)
  metadata.topics.set(otherTopic, {
    id: 'other-topic-id',
    partitions: Array.from({ length: 3 }, () => ({
      leader: 1,
      leaderEpoch: 0,
      replicas: [1],
      isr: [1],
      offlineReplicas: []
    })),
    partitionsCount: 3,
    lastUpdate: Date.now()
  })
  return metadata
}

function assignmentsObject (assignments: GroupPartitionsAssignments[]): Record<string, number[]> {
  const result: Record<string, number[]> = {}

  for (const assignment of assignments) {
    result[assignment.memberId] = assignment.assignments.get(topic)?.partitions ?? []
  }

  return result
}

test('cooperative sticky assignor should balance initial assignments', () => {
  const members = new Map([createMember('consumer-1'), createMember('consumer-2')])
  const assignments = cooperativeStickyAssigner('consumer-1', members, new Set([topic]), createMetadata(4))

  deepStrictEqual(assignmentsObject(assignments), {
    'consumer-1': [0, 2],
    'consumer-2': [1, 3]
  })
})

test('cooperative sticky assignor should not transfer ownership in the same rebalance', () => {
  const members = new Map([createMember('consumer-1', [0, 1, 2, 3]), createMember('consumer-2')])
  const assignments = cooperativeStickyAssigner('consumer-1', members, new Set([topic]), createMetadata(4))

  deepStrictEqual(assignmentsObject(assignments), {
    'consumer-1': [2, 3],
    'consumer-2': []
  })
})

test('cooperative sticky assignor should complete transfer after revoked partitions are no longer owned', () => {
  const members = new Map([createMember('consumer-1', [2, 3], 2), createMember('consumer-2', [], 2)])
  const assignments = cooperativeStickyAssigner('consumer-1', members, new Set([topic]), createMetadata(4))

  deepStrictEqual(assignmentsObject(assignments), {
    'consumer-1': [2, 3],
    'consumer-2': [0, 1]
  })
})

test('cooperative sticky assignor should invalidate duplicate same-generation ownership claims', () => {
  const members = new Map([
    createMember('consumer-1', [0, 1]),
    createMember('consumer-2', [0, 2]),
    createMember('consumer-3')
  ])
  const assignments = cooperativeStickyAssigner('consumer-1', members, new Set([topic]), createMetadata(3))

  deepStrictEqual(assignmentsObject(assignments), {
    'consumer-1': [1],
    'consumer-2': [2],
    'consumer-3': []
  })
})

test('cooperative sticky assignor should revoke duplicate ownership before assigning to a claimant', () => {
  const members = new Map([
    createMember('consumer-1', [0]),
    createMember('consumer-2', [0, 1]),
    createMember('consumer-3')
  ])
  const assignments = cooperativeStickyAssigner('consumer-1', members, new Set([topic]), createMetadata(2))

  deepStrictEqual(assignmentsObject(assignments), {
    'consumer-1': [],
    'consumer-2': [1],
    'consumer-3': []
  })
})

test('cooperative sticky assignor should ignore stale generation ownership claims', () => {
  const members = new Map([createMember('consumer-1', [0, 1], 1), createMember('consumer-2', [], 2)])
  const assignments = cooperativeStickyAssigner('consumer-1', members, new Set([topic]), createMetadata(2))

  deepStrictEqual(assignmentsObject(assignments), {
    'consumer-1': [0],
    'consumer-2': [1]
  })
})

test('cooperative sticky assignor should respect mixed topic subscriptions', () => {
  const members = new Map([
    createMemberWithTopics('z-a-owner', [topic], [{ topic, partitions: [0, 1, 2] }]),
    createMemberWithTopics('y-b-owner', [otherTopic], [{ topic: otherTopic, partitions: [1, 2] }]),
    createMemberWithTopics('a-under', [otherTopic], [{ topic: otherTopic, partitions: [0] }])
  ])
  const assignments = cooperativeStickyAssigner(
    'z-a-owner',
    members,
    new Set([topic, otherTopic]),
    createMultiTopicMetadata()
  )
  const byMember = new Map(assignments.map(assignment => [assignment.memberId, assignment.assignments]))

  deepStrictEqual(byMember.get('z-a-owner')?.get(topic)?.partitions, [0, 1, 2])
  deepStrictEqual(byMember.get('y-b-owner')?.get(otherTopic)?.partitions, [1, 2])
  deepStrictEqual(byMember.get('a-under')?.get(otherTopic)?.partitions, [0])
})
