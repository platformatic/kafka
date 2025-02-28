import BufferList from 'bl'
import { ResponseError } from '../../errors.ts'
import { Reader } from '../../protocol/reader.ts'
import { createRecordsBatch, type CreateRecordsBatchOptions, type Message } from '../../protocol/records.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../index.ts'

export type ProduceRequest = Parameters<typeof createRequest>

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

function groupByProperty<K, T> (entries: T[], property: keyof T): [K, T[]][] {
  const grouped: Map<K, T[]> = new Map()

  for (const entry of entries) {
    const value = entry[property] as K

    if (!grouped.has(value)) {
      grouped.set(value, [])
    }

    grouped.get(value)!.push(entry)
  }

  return Array.from(grouped.entries())
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
*/
function createRequest (
  acks: number = 1,
  timeout: number = 0,
  topicData: Message[],
  options: Partial<CreateRecordsBatchOptions> = {}
): Writer {
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

  return Writer.create()
    .appendString(options.transactionalId)
    .appendInt16(acks)
    .appendInt32(timeout)
    .appendArray(groupByProperty<string, Message>(topicData, 'topic'), (w, [topic, messages]) => {
      w.appendString(topic).appendArray(
        groupByProperty<number, Message>(messages, 'partition'),
        (w, [partition, messages]) => {
          const records = createRecordsBatch(messages, options)

          w.appendInt32(partition)
            .appendUnsignedVarInt(records.length + 1)
            .append(records)
        }
      )
    })
    .appendTaggedFields()
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
function parseResponse (_correlationId: number, apiKey: number, apiVersion: number, raw: BufferList): ProduceResponse {
  const reader = Reader.from(raw)
  const errors: ResponseErrorWithLocation[] = []

  const response: ProduceResponse = {
    responses: reader.readArray((r, i) => {
      const topicResponse = {
        name: r.readString()!,
        partitionResponses: r.readArray((r, j) => {
          const index = r.readInt32()
          const errorCode = r.readInt16()

          if (errorCode !== 0) {
            errors.push([`/responses/${i}/partition_responses/${j}`, errorCode])
          }

          return {
            index,
            errorCode,
            baseOffset: r.readInt64(),
            logAppendTimeMs: r.readInt64(),
            logStartOffset: r.readInt64(),
            recordErrors: r.readArray((r, k) => {
              const recordError = {
                batchIndex: r.readInt32(),
                batchIndexErrorMessage: r.readString()!
              }

              if (recordError.batchIndexErrorMessage) {
                errors.push([`/responses/${i}/partition_responses/${j}/record_errors/${k}`, -1])
              }

              return recordError
            })!
          }
        })!
      }

      return topicResponse
    })!,
    throttleTimeMs: reader.readInt32()
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, { errors: Object.fromEntries(errors), response })
  }

  return response
}

export const produceV11 = createAPI<ProduceRequest, ProduceResponse>(0, 11, createRequest, parseResponse)
