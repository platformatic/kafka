import { type ClusterMetadata } from '../base/types.ts'
import { type ExtendedGroupProtocolSubscription, type GroupAssignment, type GroupPartitionsAssignments } from './types.ts'

export function roundRobinAssigner (
  _current: string,
  members: Map<string, ExtendedGroupProtocolSubscription>,
  topics: Set<string>,
  metadata: ClusterMetadata
): GroupPartitionsAssignments[] {
  const membersSize = members.size
  const assignments: GroupPartitionsAssignments[] = []

  // Flat the list of members and subscribed topics
  for (const memberId of Array.from(members.keys()).sort()) {
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

export function rangeAssigner (
  _current: string,
  members: Map<string, ExtendedGroupProtocolSubscription>,
  topics: Set<string>,
  metadata: ClusterMetadata
): GroupPartitionsAssignments[] {
  const assignments = Array.from(members.keys()).sort().map(memberId => ({ memberId, assignments: new Map<string, GroupAssignment>() }))

  for (const topic of topics) {
    const partitionsCount = metadata.topics.get(topic)!.partitionsCount
    for (let index = 0; index < assignments.length; index++) {
      const start = Math.floor(index * partitionsCount / assignments.length)
      const end = Math.floor((index + 1) * partitionsCount / assignments.length)
      if (start === end) {
        continue
      }
      assignments[index].assignments.set(topic, { topic, partitions: Array.from({ length: end - start }, (_, partition) => start + partition) })
    }
  }

  return assignments
}
