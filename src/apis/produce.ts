import BufferList from 'bl'
import { ERROR_RESPONSE_WITH_ERROR, KafkaError } from '../error.ts'
import { type CompressionAlgorithms } from '../protocol/compression.ts'
import {
  EMPTY_TAGGED_FIELDS_BUFFER,
  INT16_SIZE,
  INT32_SIZE,
  sizeOfCompactable,
  sizeOfCompactableLength
} from '../protocol/definitions.ts'
import { Reader } from '../protocol/reader.ts'
import { createRecordsBatch, type Message } from '../protocol/records.ts'
import { Writer } from '../protocol/writer.ts'
import { groupByProperty } from '../utils.ts'
import { createAPI } from './index.ts'

export type ProduceRequest = [acks: number, timeout: number, messages: Message[], compression?: CompressionAlgorithms]

interface ProduceResponsePartitionRecordError {
  batchIndex: number
  batchIndexErrorMessage: string
}

interface ProduceResponsePartition {
  index: number
  errorCode: number
  baseOffset: bigint
  logAppendTimeMs: bigint
  logStartOffset: bigint
  recordErrors: ProduceResponsePartitionRecordError[]
}

interface ProduceResponseTopic {
  name: string
  partitionResponses: ProduceResponsePartition[]
}

interface ProduceResponse {
  responses: ProduceResponseTopic[]
  throttleTimeMs: number
}

/*
  Produce Request (Version: 11) => transactional_id acks timeout_ms [topic_data] TAG_BUFFER
    transactional_id => COMPACT_NULLABLE_STRING
    acks => INT16
    timeout_ms => INT32
    topic_data => name [partition_data] TAG_BUFFER
      name => COMPACT_STRING
      partition_data => index records TAG_BUFFER
        index => INT32
        records => COMPACT_RECORDS

  Buffer allocations: topic_data * partition_data * records + 1
*/
// TODO(ShogunPanda): Add support for transactional_id
async function createRequest (
  acks: number = 1,
  timeout: number = 0,
  topicData: Message[],
  compression: CompressionAlgorithms = 'none'
): Promise<BufferList> {
  // Normalize the messages
  const now = BigInt(Date.now())
  for (const message of topicData) {
    if (typeof message.partition === 'undefined') {
      message.partition = 0
    }

    if (typeof message.timestamp === 'undefined') {
      message.timestamp = now
    }
  }

  // Group messages by topic
  const byTopic = groupByProperty<string, Message>(topicData, 'topic')

  const buffer = new BufferList()

  const writer = Writer.create(
    // transactional_id acks timeout_ms topic_data_length
    sizeOfCompactable(null) + INT16_SIZE + INT32_SIZE + sizeOfCompactableLength(byTopic)
  )
    .writeCompactable(undefined)
    .writeInt16(acks)
    .writeInt32(timeout)
    .writeCompactableLength(byTopic)
    .appendTo(buffer)

  for (const [topic, messages] of byTopic) {
    const partitionData = groupByProperty<number, Message>(messages, 'partition')

    // Append the topic name
    writer
      .reset(sizeOfCompactable(topic) + sizeOfCompactableLength(partitionData))
      .writeCompactable(topic)
      .writeCompactableLength(partitionData)
      .appendTo(buffer)

    for (const [partition, messages] of partitionData) {
      const records = await createRecordsBatch(messages, { compression })

      writer
        .reset(INT32_SIZE + sizeOfCompactableLength(records))
        .writeInt32(partition)
        .writeCompactableLength(records)
        .appendTo(buffer)

      buffer.append(records)
      buffer.append(EMPTY_TAGGED_FIELDS_BUFFER)
    }

    buffer.append(EMPTY_TAGGED_FIELDS_BUFFER)
  }

  buffer.append(EMPTY_TAGGED_FIELDS_BUFFER)

  return buffer
}

/*
  Produce Response (Version: 11) => [responses] throttle_time_ms TAG_BUFFER
    responses => name [partition_responses] TAG_BUFFER
      name => COMPACT_STRING
      partition_responses => index error_code base_offset log_append_time_ms log_start_offset [record_errors] error_message TAG_BUFFER
        index => INT32
        error_code => INT16
        base_offset => INT64
        log_append_time_ms => INT64
        log_start_offset => INT64
        record_errors => batch_index batch_index_error_message TAG_BUFFER
          batch_index => INT32
          batch_index_error_message => COMPACT_NULLABLE_STRING
        error_message => COMPACT_NULLABLE_STRING
    throttle_time_ms => INT32
*/
function parseResponse (raw: BufferList): ProduceResponse {
  const reader = Reader.from(raw)
  let hasError = false

  const topicsLength = reader.readCollectionSize()
  const responses: ProduceResponseTopic[] = []

  for (let i = 0; i < topicsLength; i++) {
    const name = reader.readCompactableString()!

    const partitionsLength = reader.readCollectionSize()
    const partitionResponses: ProduceResponsePartition[] = []

    for (let j = 0; j < partitionsLength; j++) {
      const index = reader.readInt32()
      const errorCode = reader.readInt16()
      const baseOffset = reader.readInt64()
      const logAppendTimeMs = reader.readInt64()
      const logStartOffset = reader.readInt64()

      const recordErrorsLength = reader.readCollectionSize()
      const recordErrors: ProduceResponsePartitionRecordError[] = []

      if (errorCode !== 0) {
        hasError = true
      }

      for (let k = 0; k < recordErrorsLength; k++) {
        const batchIndex = reader.readInt32()
        const batchIndexErrorMessage = reader.readCompactableString()

        if (batchIndexErrorMessage) {
          recordErrors.push({ batchIndex, batchIndexErrorMessage })
          hasError = true
        }

        reader.readTaggedFields()
      }

      partitionResponses.push({
        index,
        errorCode,
        baseOffset,
        logAppendTimeMs,
        logStartOffset,
        recordErrors
      })

      reader.readTaggedFields()
    }

    responses.push({
      name,
      partitionResponses
    })

    reader.readTaggedFields()
  }

  const response: ProduceResponse = {
    responses,
    throttleTimeMs: reader.readInt32()
  }

  if (hasError) {
    throw new KafkaError('Received response with error while executing API Produce(v11)', {
      code: ERROR_RESPONSE_WITH_ERROR,
      response
    })
  }

  return response
}

export const produceV11 = createAPI<ProduceRequest, ProduceResponse>(0, 11, createRequest, parseResponse)
