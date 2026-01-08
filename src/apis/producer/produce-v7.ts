import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { createRecordsBatch, type CreateRecordsBatchOptions, type MessageRecord } from '../../protocol/records.ts'
import { Writer } from '../../protocol/writer.ts'
import { groupByProperty } from '../../utils.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'
import { ProduceAcks } from '../enumerations.ts'

export type ProduceRequest = Parameters<typeof createRequest>

export interface ProduceResponsePartitionRecordError {
  batchIndex: number
  batchIndexErrorMessage: NullableString
}

export interface ProduceResponsePartition {
  index: number
  errorCode: number
  baseOffset: bigint
  logAppendTimeMs: bigint
  logStartOffset: bigint
  recordErrors: ProduceResponsePartitionRecordError[]
  errorMessage: NullableString
}

export interface ProduceResponseTopic {
  name: string
  partitionResponses: ProduceResponsePartition[]
}

export interface ProduceResponse {
  responses: ProduceResponseTopic[]
  throttleTimeMs: number
}

/*
  Produce Request (Version: 7) => transactional_id acks timeout_ms [topic_data]
    transactional_id => NULLABLE_STRING
    acks => INT16
    timeout_ms => INT32
    topic_data => name [partition_data]
      name => STRING
      partition_data => index records
        index => INT32
        records => RECORDS
*/
export function createRequest (
  acks: number = 1,
  timeout: number = 0,
  topicData: MessageRecord[],
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

  const writer = Writer.create()
    .appendString(options.transactionalId, false)
    .appendInt16(acks)
    .appendInt32(timeout)
    .appendArray(
      groupByProperty<string, MessageRecord>(topicData, 'topic'),
      (w, [topic, messages]) => {
        w.appendString(topic, false).appendArray(
          groupByProperty<number, MessageRecord>(messages, 'partition'),
          (w, [partition, messages]) => {
            const records = createRecordsBatch(messages, options)

            w.appendInt32(partition).appendInt32(records.length).appendFrom(records)
          },
          false,
          false
        )
      },
      false,
      false
    )

  if (acks === ProduceAcks.NO_RESPONSE) {
    writer.context.noResponse = true
  }

  return writer
}

/*
  Produce Response (Version: 7) => [responses] throttle_time_ms
    responses => name [partition_responses]
      name => STRING
      partition_responses => index error_code base_offset log_append_time_ms log_start_offset [record_errors] error_message
        index => INT32
        error_code => INT16
        base_offset => INT64
        log_append_time_ms => INT64
        log_start_offset => INT64
    throttle_time_ms => INT32
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): ProduceResponse {
  const errors: ResponseErrorWithLocation[] = []

  const response: ProduceResponse = {
    responses: reader.readArray(
      (r, i) => {
        const topicResponse = {
          name: r.readString(false),
          partitionResponses: r.readArray(
            (r, j) => {
              const index = r.readInt32()
              const errorCode = r.readInt16()

              if (errorCode !== 0) {
                errors.push([`/responses/${i}/partition_responses/${j}`, [errorCode, null]])
              }

              return {
                index,
                errorCode,
                baseOffset: r.readInt64(),
                logAppendTimeMs: r.readInt64(),
                logStartOffset: r.readInt64(),
                recordErrors: [],
                errorMessage: null
              }
            },
            false,
            false
          )
        }

        return topicResponse
      },
      false,
      false
    ),
    throttleTimeMs: reader.readInt32()
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const api = createAPI<ProduceRequest, ProduceResponse | boolean>(
  0,
  7,
  createRequest,
  parseResponse,
  false,
  false
)
