import { randomBytes } from 'node:crypto'
import type {
  Cluster,
  ICustomPartitioner,
  Logger,
  Message,
  BrokerMetadata,
  PartitionAssigner,
  PartitionMetadata
} from './types.ts'
import type { ClusterMetadata } from '../../clients/base/types.ts'
import type {
  ExtendedGroupProtocolSubscription,
  GroupOptions,
  GroupPartitionsAssignments
} from '../../clients/consumer/types.ts'
import { KafkaJSNotImplemented } from './errors.ts'
import { murmur2 } from '../../protocol/murmur2.ts'
import { AssignerProtocol } from './protocol.ts'

function legacyMurmur2 (key: Buffer | string): number {
  const data = Buffer.isBuffer(key) ? key : Buffer.from(String(key))
  const seed = 0x9747b28c
  const multiplier = 0x5bd1e995
  const length = data.length
  let hash = seed ^ length

  for (let index = 0; index < length / 4; index++) {
    const offset = index * 4
    let value =
      (data[offset] & 0xff) +
      ((data[offset + 1] & 0xff) << 8) +
      ((data[offset + 2] & 0xff) << 16) +
      ((data[offset + 3] & 0xff) << 24)
    value *= multiplier
    value ^= value >>> 24
    value *= multiplier
    hash *= multiplier
    hash ^= value
  }

  switch (length % 4) {
    case 3:
      hash ^= (data[(length & ~3) + 2] & 0xff) << 16
    // fallthrough
    case 2:
      hash ^= (data[(length & ~3) + 1] & 0xff) << 8
    // fallthrough
    case 1:
      hash ^= data[length & ~3] & 0xff
      hash *= multiplier
  }

  hash ^= hash >>> 13
  hash *= multiplier
  hash ^= hash >>> 15
  return hash
}

function createPartitioner (hash: (key: Buffer | string) => number): ICustomPartitioner {
  return () => {
    const counters = new Map<string, number>()
    return ({ topic, partitionMetadata, message }) => {
      if (!counters.has(topic)) {
        counters.set(topic, randomBytes(32).readUInt32BE(0))
      }
      if (message.partition !== null && message.partition !== undefined) {
        return message.partition
      }
      if (message.key !== null && message.key !== undefined) {
        return (hash(message.key) & 0x7fffffff) % partitionMetadata.length
      }
      const counter = counters.get(topic)! + 1
      counters.set(topic, counter)
      const available = partitionMetadata.filter(partition => partition.leader >= 0)
      if (available.length > 0) {
        return available[(counter & 0x7fffffff) % available.length].partitionId
      }
      return (counter & 0x7fffffff) % partitionMetadata.length
    }
  }
}

const DefaultPartitioner = createPartitioner(murmur2)
const LegacyPartitioner = createPartitioner(legacyMurmur2)
export const Partitioners = { DefaultPartitioner, LegacyPartitioner, JavaCompatiblePartitioner: DefaultPartitioner }

export const roundRobin: PartitionAssigner = ({ cluster }) => ({
  name: 'RoundRobinAssigner',
  version: 0,
  async assign ({ members, topics }) {
    const memberIds = members.map(member => member.memberId).sort()
    const assigned: Record<string, Record<string, number[]>> = {}
    const partitions = topics.flatMap(topic =>
      cluster.findTopicPartitionMetadata(topic).map(partition => ({ topic, partition: partition.partitionId })))
    for (let index = 0; index < partitions.length; index++) {
      const memberId = memberIds[index % memberIds.length]
      const entry = partitions[index]
      assigned[memberId] ??= {}
      assigned[memberId][entry.topic] ??= []
      assigned[memberId][entry.topic].push(entry.partition)
    }
    return memberIds.map(memberId => ({
      memberId,
      memberAssignment: AssignerProtocol.MemberAssignment.encode({
        version: 0,
        assignment: assigned[memberId] ?? {},
        userData: Buffer.alloc(0)
      })
    }))
  },
  protocol ({ topics }) {
    return {
      name: 'RoundRobinAssigner',
      metadata: AssignerProtocol.MemberMetadata.encode({ version: 0, topics, userData: Buffer.alloc(0) })
    }
  }
})

export const PartitionAssigners = { roundRobin }

export type KafkaJSGroupAssignments = (
  members: Map<string, ExtendedGroupProtocolSubscription>,
  topics: Set<string>,
  metadata: ClusterMetadata,
  protocol: string | null
) => Promise<GroupPartitionsAssignments[]>

class AssignerCluster {
  #metadata?: ClusterMetadata

  setMetadata (metadata: ClusterMetadata): void {
    this.#metadata = metadata
  }

  asKafkaJSCluster (): Cluster {
    const unsupported = (): never => {
      throw new KafkaJSNotImplemented('Broker-oriented custom partition assigner APIs are not supported.')
    }
    const metadata = (): BrokerMetadata => ({
      brokers: Array.from(this.#metadata?.brokers ?? [], ([nodeId, broker]) => ({
        nodeId,
        host: broker.host,
        port: broker.port,
        ...(broker.rack ? { rack: broker.rack } : {})
      })),
      topicMetadata: Array.from(this.#metadata?.topics ?? [], ([topic, value]) => ({
        topicErrorCode: 0,
        topic,
        partitionMetadata: toPartitionMetadata(value.partitions)
      }))
    })
    const findTopicPartitionMetadata = (topic: string): PartitionMetadata[] => {
      const value = this.#metadata?.topics.get(topic)
      return value ? toPartitionMetadata(value.partitions) : []
    }

    return {
      getNodeIds: () => Array.from(this.#metadata?.brokers.keys() ?? []),
      metadata: async () => metadata(),
      removeBroker: unsupported,
      addMultipleTargetTopics: async () => {},
      isConnected: () => true,
      connect: async () => {},
      disconnect: async () => {},
      refreshMetadata: async () => {},
      refreshMetadataIfNecessary: async () => {},
      addTargetTopic: async () => {},
      findBroker: unsupported,
      findControllerBroker: unsupported,
      findTopicPartitionMetadata,
      findLeaderForPartitions: (topic, partitions) => {
        const leaders: Record<string, number[]> = {}
        for (const partition of findTopicPartitionMetadata(topic)) {
          if (partitions.includes(partition.partitionId)) {
            leaders[partition.leader] ??= []
            leaders[partition.leader].push(partition.partitionId)
          }
        }
        return leaders
      },
      findGroupCoordinator: unsupported,
      findGroupCoordinatorMetadata: unsupported,
      defaultOffset: ({ fromBeginning }) => (fromBeginning ? -2 : -1),
      fetchTopicsOffset: unsupported
    }
  }
}

export function createPartitionAssignerOptions (
  assigners: PartitionAssigner[],
  groupId: string,
  logger: Logger
): Pick<GroupOptions, 'protocolMetadata' | 'protocols'> & { createAssignments: KafkaJSGroupAssignments } {
  const cluster = new AssignerCluster()
  const assignersByName = new Map(assigners.map(assigner => {
    const instance = assigner({ cluster: cluster.asKafkaJSCluster(), groupId, logger })
    return [instance.name, instance] as const
  }))

  return {
    protocols: Array.from(assignersByName.values(), assigner => ({ name: assigner.name, version: assigner.version })),
    protocolMetadata: (protocol, topics, metadata) => {
      cluster.setMetadata(metadata)
      const assigner = assignersByName.get(protocol.name)
      if (!assigner) {
        throw new KafkaJSNotImplemented(`Partition assigner ${protocol.name} is not configured.`)
      }
      return assigner.protocol({ topics }).metadata
    },
    createAssignments: async (members, topics, metadata, protocol) => {
      cluster.setMetadata(metadata)
      const assigner = assignersByName.get(protocol ?? '')
      if (!assigner) {
        throw new KafkaJSNotImplemented(`Partition assigner ${protocol ?? ''} is not configured.`)
      }
      const assignments = await assigner.assign({
        members: Array.from(members.values(), member => ({
          memberId: member.memberId,
          memberMetadata: AssignerProtocol.MemberMetadata.encode({
            version: member.version,
            topics: member.topics ?? [],
            userData: Buffer.isBuffer(member.metadata) ? member.metadata : Buffer.from(member.metadata ?? '')
          })
        })),
        topics: Array.from(topics)
      })
      return assignments.map(({ memberId, memberAssignment }) => ({
        memberId,
        assignments: new Map(),
        assignment: memberAssignment
      }))
    }
  }
}

export function toPartitionMetadata (
  partitions: Array<{
    leader: number
    replicas: number[]
    isr: number[]
    offlineReplicas: number[]
  }>
): PartitionMetadata[] {
  return partitions.map((partition, partitionId) => ({
    partitionErrorCode: 0,
    partitionId,
    ...partition
  }))
}

export function normalizeMessage (message: Message, usePartitioner = false): {
  key?: Buffer
  value?: Buffer
  partition?: number
  timestamp?: bigint
  headers?: Map<Buffer, Buffer>
  metadata: { kafkaJSMessage: Message }
} {
  const headers = new Map<Buffer, Buffer>()
  for (const [key, value] of Object.entries(message.headers ?? {})) {
    const values = Array.isArray(value) ? value : [value]
    for (const entry of values) {
      if (entry !== undefined) {
        const normalizedKey = Buffer.from(key)
        const normalizedValue = Buffer.isBuffer(entry) ? entry : Buffer.from(entry)
        headers.set(normalizedKey, normalizedValue)
      }
    }
  }
  return {
    key: message.key === null || message.key === undefined ? undefined : Buffer.from(message.key),
    value: message.value === null ? undefined : Buffer.from(message.value),
    partition: usePartitioner ? undefined : message.partition,
    timestamp: message.timestamp === undefined ? undefined : BigInt(message.timestamp),
    headers,
    metadata: { kafkaJSMessage: message }
  }
}
