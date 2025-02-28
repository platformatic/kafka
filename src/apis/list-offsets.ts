import BufferList from 'bl'
import { ERROR_RESPONSE_WITH_ERROR, KafkaError } from '../error.ts'
import {
  EMPTY_OR_SINGLE_COMPACT_LENGTH_SIZE,
  EMPTY_TAGGED_FIELDS_BUFFER,
  INT32_SIZE,
  INT64_SIZE,
  INT8_SIZE,
  sizeOfCompactable,
  sizeOfCompactableLength
} from '../protocol/definitions.ts'
import { Reader } from '../protocol/reader.ts'
import { Writer } from '../protocol/writer.ts'
import { createAPI } from './index.ts'

interface ListOffsetsRequestPartition {
  partitionIndex: number
  currentLeaderEpoch: number
  timestamp: bigint
}

interface ListOffsetsRequestTopic {
  name: string
  partitions: ListOffsetsRequestPartition[]
}

export type ListOffsetsRequest = [replica: number, isolationLevel: number, topics: ListOffsetsRequestTopic[]]

export interface ListOffsetResponsePartition {
  errorCode: number
  timestamp: bigint
  offset: bigint
  leaderEpoch: number
}

export interface ListOffsetsResponse {
  throttleTime: number
  topics: Record<string, Record<number, ListOffsetResponsePartition>>
}

/*
  ListOffsets Request (Version: 9) => replica_id isolation_level [topics] TAG_BUFFER
    replica_id => INT32
    isolation_level => INT8
    topics => name [partitions] TAG_BUFFER
      name => COMPACT_STRING
      partitions => partition_index current_leader_epoch timestamp TAG_BUFFER
        partition_index => INT32
        current_leader_epoch => INT32
        timestamp => INT64

  Buffer allocations: topics * partitions + 1
*/
function createRequest (replica: number, isolationLevel: number, topics: ListOffsetsRequestTopic[]): BufferList {
  const buffer = new BufferList()

  if (!Array.isArray(topics)) {
    topics = [topics]
  }

  const writer = Writer.create(INT32_SIZE + INT8_SIZE + sizeOfCompactableLength(topics))
    .writeInt32(replica)
    .writeInt8(isolationLevel)
    .writeCompactableLength(topics)
    .appendTo(buffer)

  for (const topic of topics) {
    writer
      .reset(sizeOfCompactable(topic.name) + sizeOfCompactableLength(topic.partitions))
      .writeCompactable(topic.name)
      .writeCompactableLength(topic.partitions)
      .appendTo(buffer)

    for (const partition of topic.partitions) {
      writer
        .reset(INT32_SIZE + INT32_SIZE + INT64_SIZE + EMPTY_OR_SINGLE_COMPACT_LENGTH_SIZE)
        .writeInt32(partition.partitionIndex)
        .writeInt32(partition.currentLeaderEpoch)
        .writeInt64(partition.timestamp)
        .writeTaggedFields()
        .appendTo(buffer)
    }

    buffer.append(EMPTY_TAGGED_FIELDS_BUFFER)
  }

  buffer.append(EMPTY_TAGGED_FIELDS_BUFFER)

  return buffer
}

/*
  ListOffsets Response (Version: 9) => throttle_time_ms [topics] TAG_BUFFER
    throttle_time_ms => INT32
    topics => name [partitions] TAG_BUFFER
      name => COMPACT_STRING
      partitions => partition_index error_code timestamp offset leader_epoch TAG_BUFFER
        partition_index => INT32
        error_code => INT16
        timestamp => INT64
        offset => INT64
        leader_epoch => INT32
*/
function parseResponse (raw: BufferList): ListOffsetsResponse {
  const reader = Reader.from(raw)
  let hasError = false

  const response: ListOffsetsResponse = {
    throttleTime: reader.readInt32(),
    topics: {}
  }

  const topicsSize = reader.readCollectionSize()

  for (let i = 0; i < topicsSize; i++) {
    const name = reader.readCompactableString()!
    const topic: Record<number, ListOffsetResponsePartition> = {}

    const topicsSize = reader.readCollectionSize()

    for (let j = 0; j < topicsSize; j++) {
      const index = reader.readInt32()
      const errorCode = reader.readInt16()
      const timestamp = reader.readInt64()
      const offset = reader.readInt64()
      const leaderEpoch = reader.readInt32()

      if (errorCode !== 0) {
        hasError = true
      }

      topic[index] = {
        errorCode,
        timestamp,
        offset,
        leaderEpoch
      }

      reader.readTaggedFields()
    }

    response.topics[name] = topic
    reader.readTaggedFields()
  }

  if (hasError) {
    throw new KafkaError('Received response with error while executing API ListOffsets(v9)', {
      code: ERROR_RESPONSE_WITH_ERROR,
      response
    })
  }

  return response
}

export const listOffsetsV9 = createAPI<ListOffsetsRequest, ListOffsetsResponse>(2, 9, createRequest, parseResponse)
