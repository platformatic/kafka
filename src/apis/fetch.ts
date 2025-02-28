import BufferList from 'bl'
import { ERROR_RESPONSE_WITH_ERROR, KafkaError } from '../error.ts'
import {
  EMPTY_OR_SINGLE_COMPACT_LENGTH_SIZE,
  EMPTY_TAGGED_FIELDS_BUFFER,
  INT32_SIZE,
  INT64_SIZE,
  INT8_SIZE,
  sizeOfCompactable,
  sizeOfCompactableLength,
  UUID_SIZE
} from '../protocol/definitions.ts'
import { Reader } from '../protocol/reader.ts'
import { readRecordsBatch, type KafkaRecordsBatch } from '../protocol/records.ts'
import { Writer } from '../protocol/writer.ts'
import { createAPI } from './index.ts'

export interface FetchRequestPartition {
  partition: number
  currentLeaderEpoch: number
  fetchOffset: bigint
  lastFetchedEpoch: number
  // logStartOffset: bigint - This is only used between brokers
  partitionMaxBytes: number
}

export interface FetchRequestTopic {
  topicId: string
  partitions: FetchRequestPartition[]
}

export interface FetchRequestForgottenTopicsData {
  topic: string
  partitions: number[]
}

export type FetchRequest = [
  maxWaitMs: number,
  minBytes: number,
  maxBytes: number,
  isolationLevel: number,
  sessionId: number,
  sessionEpoch: number,
  topics: FetchRequestTopic[],
  forgottenTopicsData: FetchRequestForgottenTopicsData[],
  rackId: string | null
]

export interface FetchResponsePartitionAbortedTransaction {
  producerId: bigint
  firstOffset: bigint
}

export interface FetchResponsePartition {
  partitionIndex: number
  errorCode: number
  highWatermark: bigint
  lastStableOffset: bigint
  logStartOffset: bigint
  abortedTransactions: FetchResponsePartitionAbortedTransaction[]
  preferredReadReplica: number
  records: KafkaRecordsBatch
}

export interface FetchResponseTopic {
  topicId: string
  partitions: FetchResponsePartition[]
}

export type FetchResponse = {
  throttleTimeMs: number
  errorCode: number
  sessionId: number
  responses: FetchResponseTopic[]
}

/*
  Fetch Request (Version: 17) => max_wait_ms min_bytes max_bytes isolation_level session_id session_epoch [topics] [forgotten_topics_data] rack_id TAG_BUFFER
  max_wait_ms => INT32
  min_bytes => INT32
  max_bytes => INT32
  isolation_level => INT8
  session_id => INT32
  session_epoch => INT32
  topics => topic_id [partitions] TAG_BUFFER
    topic_id => UUID
    partitions => partition current_leader_epoch fetch_offset last_fetched_epoch log_start_offset partition_max_bytes TAG_BUFFER
      partition => INT32
      current_leader_epoch => INT32
      fetch_offset => INT64
      last_fetched_epoch => INT32
      log_start_offset => INT64
      partition_max_bytes => INT32
      replica_directory_id => UUID
  forgotten_topics_data => topic_id [partitions] TAG_BUFFER
    topic_id => UUID
    partitions => INT32
  rack_id => COMPACT_STRING

  Buffer allocations: topics * topics.partitions + forgotten_topics_data * forgotten_topics_data.partitions + 3
*/
async function createRequest (
  maxWaitMs: number,
  minBytes: number,
  maxBytes: number,
  isolationLevel: number,
  sessionId: number,
  sessionEpoch: number,
  topics: FetchRequestTopic[],
  forgottenTopicsData: FetchRequestForgottenTopicsData[],
  rackId: string
): Promise<BufferList> {
  const buffer = new BufferList()

  const writer = Writer.create(
    INT32_SIZE + INT32_SIZE + INT32_SIZE + INT8_SIZE + INT32_SIZE + INT32_SIZE + sizeOfCompactableLength(topics)
  )
    .writeInt32(maxWaitMs)
    .writeInt32(minBytes)
    .writeInt32(maxBytes)
    .writeInt8(isolationLevel)
    .writeInt32(sessionId)
    .writeInt32(sessionEpoch)
    .writeCompactableLength(topics)
    .appendTo(buffer)

  for (const topic of topics) {
    writer
      .reset(UUID_SIZE + sizeOfCompactableLength(topic.partitions))
      .writeUUID(topic.topicId)
      .writeCompactableLength(topic.partitions)
      .appendTo(buffer)

    for (const partition of topic.partitions) {
      writer
        .reset(
          INT32_SIZE +
            INT32_SIZE +
            INT64_SIZE +
            INT32_SIZE +
            INT64_SIZE +
            INT32_SIZE +
            EMPTY_OR_SINGLE_COMPACT_LENGTH_SIZE
        )
        .writeInt32(partition.partition)
        .writeInt32(partition.currentLeaderEpoch)
        .writeInt64(partition.fetchOffset)
        .writeInt32(partition.lastFetchedEpoch)
        .writeInt64(-1n)
        .writeInt32(partition.partitionMaxBytes)
        .writeTaggedFields()
        .appendTo(buffer)
    }

    buffer.append(EMPTY_TAGGED_FIELDS_BUFFER)
  }

  writer
    .reset(sizeOfCompactableLength(forgottenTopicsData))
    .writeCompactableLength(forgottenTopicsData)
    .appendTo(buffer)

  for (const forgottenTopic of forgottenTopicsData) {
    Writer.create(UUID_SIZE + sizeOfCompactableLength(forgottenTopic.partitions) + EMPTY_OR_SINGLE_COMPACT_LENGTH_SIZE)
      .writeUUID(forgottenTopic.topic)
      .writeCompactableLength(forgottenTopic.partitions)
      .writeTaggedFields()
      .appendTo(buffer)
  }

  writer
    .reset(sizeOfCompactable(rackId) + EMPTY_OR_SINGLE_COMPACT_LENGTH_SIZE)
    .writeCompactable(rackId)
    .writeTaggedFields()
    .appendTo(buffer)

  return buffer
}

/*
  Fetch Response (Version: 17) => throttle_time_ms error_code session_id [responses] TAG_BUFFER
    throttle_time_ms => INT32
    error_code => INT16
    session_id => INT32
    responses => topic_id [partitions] TAG_BUFFER
      topic_id => UUID
      partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] preferred_read_replica records TAG_BUFFER
        partition_index => INT32
        error_code => INT16
        high_watermark => INT64
        last_stable_offset => INT64
        log_start_offset => INT64
        aborted_transactions => producer_id first_offset TAG_BUFFER
          producer_id => INT64
          first_offset => INT64
        preferred_read_replica => INT32
        records => COMPACT_RECORDS
*/
async function parseResponse (raw: BufferList): Promise<FetchResponse> {
  const reader = Reader.from(raw)
  let hasError = false

  const response: FetchResponse = {
    throttleTimeMs: reader.readInt32(),
    errorCode: reader.readInt16(),
    sessionId: reader.readInt32(),
    responses: []
  }

  const responsesLength = reader.readCollectionSize()
  for (let i = 0; i < responsesLength; i++) {
    const topic: FetchResponseTopic = {
      topicId: reader.readUUID(),
      partitions: []
    }

    const partitionsLength = reader.readCollectionSize()
    for (let j = 0; j < partitionsLength; j++) {
      const partitionIndex = reader.readInt32()
      const errorCode = reader.readInt16()
      const highWatermark = reader.readInt64()
      const lastStableOffset = reader.readInt64()
      const logStartOffset = reader.readInt64()
      const abortedTransactions: FetchResponsePartitionAbortedTransaction[] = []

      const abortedTransactionsLength = reader.readCollectionSize()
      for (let k = 0; k < abortedTransactionsLength; k++) {
        abortedTransactions.push({
          producerId: reader.readInt64(),
          firstOffset: reader.readInt64()
        })

        reader.readTaggedFields()
      }

      const preferredReadReplica = reader.readInt32()
      const recordsSize = reader.readCollectionSize()

      if (errorCode !== 0) {
        hasError = true
      }

      topic.partitions.push({
        partitionIndex,
        errorCode,
        highWatermark,
        lastStableOffset,
        logStartOffset,
        abortedTransactions,
        preferredReadReplica,
        records: await readRecordsBatch(
          Reader.from(reader.buffer.shallowSlice(reader.position, reader.position + recordsSize))
        )
      })

      reader.skip(recordsSize)

      reader.readTaggedFields()
    }

    response.responses.push(topic)
  }

  if (hasError) {
    throw new KafkaError('Received response with error while executing API Fetch(v17)', {
      code: ERROR_RESPONSE_WITH_ERROR,
      response
    })
  }

  return response
}

export const fetchV17 = createAPI<FetchRequest, FetchResponse>(1, 17, createRequest, parseResponse)
