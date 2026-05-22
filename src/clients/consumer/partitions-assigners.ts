import { type ClusterMetadata } from '../base/types.ts'
import { type ExtendedGroupProtocolSubscription, type GroupPartitionsAssignments } from './types.ts'

export const ROUNDROBIN_ASSIGNOR = 'roundrobin'
export const COOPERATIVE_STICKY_ASSIGNOR = 'cooperative-sticky'

interface TopicPartition {
  topic: string
  partition: number
}

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

export function cooperativeStickyAssigner (
  _current: string,
  members: Map<string, ExtendedGroupProtocolSubscription>,
  topics: Set<string>,
  metadata: ClusterMetadata
): GroupPartitionsAssignments[] {
  const assignments = createEmptyAssignments(members)
  const allPartitions = listAssignablePartitions(members, topics, metadata)
  const { owners: previousOwners, contested: contestedPreviousOwners } = previousOwnership(members, allPartitions)

  for (const partition of allPartitions) {
    const owner = previousOwners.get(topicPartitionKey(partition))
    if (owner) {
      addAssignment(assignments, owner, partition)
    }
  }

  assignUnassignedPartitions(assignments, members, allPartitions)
  balanceAssignments(assignments, members)
  adjustCooperativeTransfers(assignments, members, previousOwners, contestedPreviousOwners)

  return Array.from(assignments, ([memberId, memberAssignments]) => ({
    memberId,
    assignments: memberAssignments
  }))
}

function createEmptyAssignments (
  members: Map<string, ExtendedGroupProtocolSubscription>
): Map<string, Map<string, { topic: string; partitions: number[] }>> {
  const assignments = new Map<string, Map<string, { topic: string; partitions: number[] }>>()

  for (const memberId of members.keys()) {
    assignments.set(memberId, new Map())
  }

  return assignments
}

function listAssignablePartitions (
  members: Map<string, ExtendedGroupProtocolSubscription>,
  topics: Set<string>,
  metadata: ClusterMetadata
): TopicPartition[] {
  const result: TopicPartition[] = []

  for (const topic of [...topics].sort()) {
    const topicMetadata = metadata.topics.get(topic)
    if (!topicMetadata || !hasEligibleMember(members, topic)) {
      continue
    }

    for (let partition = 0; partition < topicMetadata.partitionsCount; partition++) {
      result.push({ topic, partition })
    }
  }

  return result
}

function hasEligibleMember (members: Map<string, ExtendedGroupProtocolSubscription>, topic: string): boolean {
  for (const member of members.values()) {
    if (member.topics?.includes(topic)) {
      return true
    }
  }

  return false
}

function previousOwnership (
  members: Map<string, ExtendedGroupProtocolSubscription>,
  allPartitions: TopicPartition[]
): { owners: Map<string, string>; contested: Set<string> } {
  const validPartitions = new Set(allPartitions.map(topicPartitionKey))
  const maxGeneration = Math.max(-1, ...Array.from(members.values(), member => member.generationId ?? -1))
  const claims = new Map<string, { memberId: string; generationId: number }[]>()

  for (const [memberId, member] of members) {
    const generationId = member.generationId ?? -1
    if (maxGeneration >= 0 && generationId < maxGeneration) {
      continue
    }

    for (const owned of member.ownedPartitions ?? []) {
      if (!member.topics?.includes(owned.topic)) {
        continue
      }

      for (const partition of owned.partitions) {
        const topicPartition = { topic: owned.topic, partition }
        const key = topicPartitionKey(topicPartition)
        if (!validPartitions.has(key)) {
          continue
        }

        let partitionClaims = claims.get(key)
        if (!partitionClaims) {
          partitionClaims = []
          claims.set(key, partitionClaims)
        }
        partitionClaims.push({ memberId, generationId })
      }
    }
  }

  const owners = new Map<string, string>()
  const contested = new Set<string>()
  for (const [key, partitionClaims] of claims) {
    const highestGeneration = Math.max(...partitionClaims.map(claim => claim.generationId))
    const highestGenerationClaims = partitionClaims.filter(claim => claim.generationId === highestGeneration)

    // If two members claim the same partition in the same generation, neither
    // claim can be trusted. This mirrors the Kafka assignor's safety check.
    if (highestGenerationClaims.length === 1) {
      owners.set(key, highestGenerationClaims[0].memberId)
    } else {
      contested.add(key)
    }
  }

  return { owners, contested }
}

function assignUnassignedPartitions (
  assignments: Map<string, Map<string, { topic: string; partitions: number[] }>>,
  members: Map<string, ExtendedGroupProtocolSubscription>,
  allPartitions: TopicPartition[]
): void {
  const assigned = assignedPartitionKeys(assignments)

  for (const partition of allPartitions) {
    if (assigned.has(topicPartitionKey(partition))) {
      continue
    }

    const memberId = leastLoadedEligibleMember(assignments, members, partition)
    if (memberId) {
      addAssignment(assignments, memberId, partition)
      assigned.add(topicPartitionKey(partition))
    }
  }
}

function balanceAssignments (
  assignments: Map<string, Map<string, { topic: string; partitions: number[] }>>,
  members: Map<string, ExtendedGroupProtocolSubscription>
): void {
  const totalPartitions = assignedPartitionKeys(assignments).size
  if (totalPartitions === 0 || members.size === 0) {
    return
  }

  const minQuota = Math.floor(totalPartitions / members.size)
  const maxQuota = Math.ceil(totalPartitions / members.size)

  while (true) {
    const underloadedMembers = sortedMemberIds(assignments).filter(
      memberId => assignmentSize(assignments.get(memberId)!) < minQuota
    )
    const overloadedMembers = sortedMemberIds(assignments)
      .filter(memberId => assignmentSize(assignments.get(memberId)!) > maxQuota)
      .sort((left, right) => assignmentSize(assignments.get(right)!) - assignmentSize(assignments.get(left)!))

    if (underloadedMembers.length === 0 || overloadedMembers.length === 0) {
      return
    }

    let moved = false
    for (const underloaded of underloadedMembers) {
      for (const overloaded of overloadedMembers) {
        const partition = removablePartition(assignments.get(overloaded)!, members.get(underloaded)!)
        if (!partition) {
          continue
        }

        removeAssignment(assignments, overloaded, partition)
        addAssignment(assignments, underloaded, partition)
        moved = true
        break
      }

      if (moved) {
        break
      }
    }

    if (!moved) {
      return
    }
  }
}

function adjustCooperativeTransfers (
  assignments: Map<string, Map<string, { topic: string; partitions: number[] }>>,
  members: Map<string, ExtendedGroupProtocolSubscription>,
  previousOwners: Map<string, string>,
  contestedPreviousOwners: Set<string>
): void {
  const added = new Map<string, string>()
  const revoked = new Set<string>()

  for (const [memberId, memberAssignments] of assignments) {
    const trustedOwned = new Set(
      flattenAssignments(members.get(memberId)?.ownedPartitions ?? [])
        .map(topicPartitionKey)
        .filter(key => previousOwners.get(key) === memberId)
    )
    const claimedOwned = new Set(
      flattenAssignments(members.get(memberId)?.ownedPartitions ?? [])
        .map(topicPartitionKey)
        .filter(key => previousOwners.get(key) === memberId || contestedPreviousOwners.has(key))
    )
    const assigned = new Set(flattenAssignments(Array.from(memberAssignments.values())).map(topicPartitionKey))

    for (const key of assigned) {
      if (!trustedOwned.has(key)) {
        added.set(key, memberId)
      }
    }

    for (const key of claimedOwned) {
      if (!assigned.has(key)) {
        revoked.add(key)
      }
    }
  }

  for (const [key, memberId] of added) {
    if (revoked.has(key)) {
      removeAssignment(assignments, memberId, parseTopicPartitionKey(key))
    }
  }
}

function leastLoadedEligibleMember (
  assignments: Map<string, Map<string, { topic: string; partitions: number[] }>>,
  members: Map<string, ExtendedGroupProtocolSubscription>,
  partition: TopicPartition
): string | null {
  let selected: string | null = null
  let selectedSize = Number.POSITIVE_INFINITY

  for (const memberId of sortedMemberIds(assignments)) {
    const member = members.get(memberId)!
    if (!member.topics?.includes(partition.topic)) {
      continue
    }

    const size = assignmentSize(assignments.get(memberId)!)
    if (size < selectedSize) {
      selected = memberId
      selectedSize = size
    }
  }

  return selected
}

function removablePartition (
  assignments: Map<string, { topic: string; partitions: number[] }>,
  targetMember: ExtendedGroupProtocolSubscription
): TopicPartition | null {
  const partitions = flattenAssignments(Array.from(assignments.values())).sort(compareTopicPartitions)

  for (const partition of partitions) {
    if (targetMember.topics?.includes(partition.topic)) {
      return partition
    }
  }

  return null
}

function addAssignment (
  assignments: Map<string, Map<string, { topic: string; partitions: number[] }>>,
  memberId: string,
  partition: TopicPartition
): void {
  const memberAssignments = assignments.get(memberId)!
  let topicAssignment = memberAssignments.get(partition.topic)
  if (!topicAssignment) {
    topicAssignment = { topic: partition.topic, partitions: [] }
    memberAssignments.set(partition.topic, topicAssignment)
  }

  if (!topicAssignment.partitions.includes(partition.partition)) {
    topicAssignment.partitions.push(partition.partition)
    topicAssignment.partitions.sort((a, b) => a - b)
  }
}

function removeAssignment (
  assignments: Map<string, Map<string, { topic: string; partitions: number[] }>>,
  memberId: string,
  partition: TopicPartition
): void {
  const memberAssignments = assignments.get(memberId)
  const topicAssignment = memberAssignments?.get(partition.topic)
  if (!topicAssignment) {
    return
  }

  topicAssignment.partitions = topicAssignment.partitions.filter(current => current !== partition.partition)
  if (topicAssignment.partitions.length === 0) {
    memberAssignments!.delete(partition.topic)
  }
}

function assignedPartitionKeys (
  assignments: Map<string, Map<string, { topic: string; partitions: number[] }>>
): Set<string> {
  const assigned = new Set<string>()

  for (const memberAssignments of assignments.values()) {
    for (const partition of flattenAssignments(Array.from(memberAssignments.values()))) {
      assigned.add(topicPartitionKey(partition))
    }
  }

  return assigned
}

function flattenAssignments (assignments: { topic: string; partitions: number[] }[]): TopicPartition[] {
  const partitions: TopicPartition[] = []

  for (const assignment of assignments) {
    for (const partition of assignment.partitions) {
      partitions.push({ topic: assignment.topic, partition })
    }
  }

  return partitions
}

function assignmentSize (assignments: Map<string, { topic: string; partitions: number[] }>): number {
  let size = 0
  for (const assignment of assignments.values()) {
    size += assignment.partitions.length
  }
  return size
}

function sortedMemberIds (assignments: Map<string, Map<string, { topic: string; partitions: number[] }>>): string[] {
  return [...assignments.keys()].sort()
}

function topicPartitionKey ({ topic, partition }: TopicPartition): string {
  return `${topic}:${partition}`
}

function parseTopicPartitionKey (key: string): TopicPartition {
  const index = key.lastIndexOf(':')
  return {
    topic: key.slice(0, index),
    partition: Number(key.slice(index + 1))
  }
}

function compareTopicPartitions (left: TopicPartition, right: TopicPartition): number {
  const topicComparison = left.topic.localeCompare(right.topic)
  return topicComparison === 0 ? left.partition - right.partition : topicComparison
}
