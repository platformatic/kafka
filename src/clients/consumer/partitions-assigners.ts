import { type ClusterMetadata } from '../base/types.ts'
import { type ExtendedGroupProtocolSubscription, type GroupPartitionsAssignments } from './types.ts'

export function roundRobinAssigner (
  _current: string,
  members: Map<string, ExtendedGroupProtocolSubscription>,
  topics: Set<string>,
  metadata: ClusterMetadata
): GroupPartitionsAssignments[] {
  const membersSize = members.size
  const assignments: GroupPartitionsAssignments[] = []

  // Flat the list of members and subscribed topics
  for (const memberId of members.keys()) {
    assignments.push({ memberId, assignments: new Map() })
  }

  // Assign topic-partitions in round robin
  let currentMember = 0
  for (const topic of topics) {
    const partitionsCount = metadata.topics.get(topic)!.partitionsCount

    for (let i = 0; i < partitionsCount; i++) {
      const member = assignments[currentMember++ % membersSize]
      let topicAssignments = member.assignments.get(topic)

      if (!topicAssignments) {
        topicAssignments = { topic, partitions: [] }
        member.assignments.set(topic, topicAssignments)
      }

      topicAssignments?.partitions.push(i)
    }
  }

  return assignments
}
