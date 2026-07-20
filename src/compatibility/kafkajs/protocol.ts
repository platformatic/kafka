import type { Assignment, MemberAssignment, MemberMetadata } from './types.ts'
import { Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'

const metadata = {
  encode ({ version, topics, userData = Buffer.alloc(0) }: MemberMetadata): Buffer {
    return Writer.create()
      .appendInt16(version)
      .appendArray(topics, (writer, topic) => writer.appendString(topic, false), false, false)
      .appendBytes(userData, false).buffer
  },
  decode (buffer: Buffer): MemberMetadata {
    const reader = Reader.from(buffer)
    return {
      version: reader.readInt16(),
      topics: reader.readArray(item => item.readString(false), false, false),
      userData: reader.readBytes(false)
    }
  }
}

const assignment = {
  encode ({ version, assignment, userData = Buffer.alloc(0) }: MemberAssignment): Buffer {
    return Writer.create()
      .appendInt16(version)
      .appendArray(
        Object.entries(assignment),
        (writer, [topic, partitions]) => {
          writer
            .appendString(topic, false)
            .appendArray(partitions, (writer, partition) => writer.appendInt32(partition), false, false)
        },
        false,
        false
      )
      .appendBytes(userData, false).buffer
  },
  decode (buffer: Buffer): MemberAssignment | null {
    const reader = Reader.from(buffer)
    if (reader.remaining < 2) {
      return null
    }
    const result: Assignment = {}
    const version = reader.readInt16()
    for (const [topic, partitions] of reader.readArray(
      item => [item.readString(false), item.readArray(value => value.readInt32(), false, false)] as const,
      false,
      false
    )) {
      result[topic] = partitions
    }
    return { version, assignment: result, userData: reader.readBytes(false) }
  }
}

export const AssignerProtocol = { MemberMetadata: metadata, MemberAssignment: assignment }
