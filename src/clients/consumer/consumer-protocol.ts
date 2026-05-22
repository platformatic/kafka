import { Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { type GroupAssignment } from './types.ts'

export const CONSUMER_PROTOCOL_LOWEST_VERSION = 0
export const CONSUMER_PROTOCOL_HIGHEST_VERSION = 3

export interface ConsumerProtocolSubscriptionData {
  version: number
  topics: string[]
  userData: Buffer
  ownedPartitions: GroupAssignment[]
  generationId: number
  rackId: string | null
}

export interface ConsumerProtocolAssignmentData {
  version: number
  assignedPartitions: GroupAssignment[]
  userData: Buffer
}

function supportedVersion (version: number): number {
  if (version < CONSUMER_PROTOCOL_LOWEST_VERSION) {
    return CONSUMER_PROTOCOL_LOWEST_VERSION
  }

  if (version > CONSUMER_PROTOCOL_HIGHEST_VERSION) {
    return CONSUMER_PROTOCOL_HIGHEST_VERSION
  }

  return version
}

function sortedAssignments (assignments: GroupAssignment[]): GroupAssignment[] {
  return assignments
    .filter(({ partitions }) => partitions.length > 0)
    .map(({ topic, partitions }) => ({ topic, partitions: [...partitions].sort((a, b) => a - b) }))
    .sort((a, b) => a.topic.localeCompare(b.topic))
}

function appendTopicPartitions (writer: Writer, assignments: GroupAssignment[]): Writer {
  return writer.appendArray(
    sortedAssignments(assignments),
    (w, { topic, partitions }) => {
      w.appendString(topic, false).appendArray(partitions, (w, partition) => w.appendInt32(partition), false, false)
    },
    false,
    false
  )
}

function readTopicPartitions (reader: Reader): GroupAssignment[] {
  return reader.readArray(
    r => ({
      topic: r.readString(false),
      partitions: r.readArray(r => r.readInt32(), false, false)
    }),
    false,
    false
  )
}

export function encodeConsumerProtocolSubscription (data: {
  version: number
  topics: string[]
  userData?: Buffer | string
  ownedPartitions?: GroupAssignment[]
  generationId?: number
  rackId?: string | null
}): Buffer {
  const version = supportedVersion(data.version)
  const userData = typeof data.userData === 'string' ? Buffer.from(data.userData) : data.userData
  const writer = Writer.create()
    .appendInt16(version)
    .appendArray([...data.topics].sort(), (w, topic) => w.appendString(topic, false), false, false)
    .appendBytes(userData, false)

  if (version >= 1) {
    appendTopicPartitions(writer, data.ownedPartitions ?? [])
  }

  if (version >= 2) {
    writer.appendInt32(data.generationId ?? -1)
  }

  if (version >= 3) {
    writer.appendString(data.rackId || null, false)
  }

  return writer.buffer
}

export function decodeConsumerProtocolSubscription (buffer: Buffer): ConsumerProtocolSubscriptionData {
  const reader = Reader.from(buffer)
  const encodedVersion = reader.readInt16()
  const version = supportedVersion(encodedVersion)
  const subscription: ConsumerProtocolSubscriptionData = {
    version: encodedVersion,
    topics: reader.readArray(r => r.readString(false), false, false),
    userData: reader.readBytes(false),
    ownedPartitions: [],
    generationId: -1,
    rackId: null
  }

  if (version >= 1) {
    subscription.ownedPartitions = readTopicPartitions(reader)
  }

  if (version >= 2) {
    subscription.generationId = reader.readInt32()
  }

  if (version >= 3) {
    subscription.rackId = reader.readNullableString(false)
  }

  return subscription
}

export function decodeCooperativeStickyGeneration (userData: Buffer): number {
  if (userData.length < 4) {
    return -1
  }

  try {
    return Reader.from(userData).readInt32()
  } catch {
    return -1
  }
}

export function encodeConsumerProtocolAssignment (data: {
  version: number
  assignedPartitions: GroupAssignment[]
  userData?: Buffer | string | null
}): Buffer {
  const userData = typeof data.userData === 'string' ? Buffer.from(data.userData) : data.userData
  const writer = Writer.create().appendInt16(supportedVersion(data.version))
  appendTopicPartitions(writer, data.assignedPartitions)
  writer.appendBytes(userData ?? null, false)
  return writer.buffer
}

export function decodeConsumerProtocolAssignment (buffer: Buffer): ConsumerProtocolAssignmentData {
  if (buffer.length === 0) {
    return {
      version: 0,
      assignedPartitions: [],
      userData: Buffer.alloc(0)
    }
  }

  const reader = Reader.from(buffer)
  const encodedVersion = reader.readInt16()

  if (reader.remaining === 0) {
    return {
      version: encodedVersion,
      assignedPartitions: [],
      userData: Buffer.alloc(0)
    }
  }

  const assignedPartitions = readTopicPartitions(reader)
  const userData = reader.remaining > 0 ? reader.readBytes(false) : Buffer.alloc(0)

  return {
    version: encodedVersion,
    assignedPartitions,
    userData
  }
}
