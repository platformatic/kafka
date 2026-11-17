import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { Reader } from '../../protocol/reader.ts'
import { readRecordsBatches, type RecordsBatch } from '../../protocol/records.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export interface ShareFetchRequestAcknowledgementBatch {
  firstOffset: bigint
  lastOffset: bigint
  acknowledgeTypes: number[]
}

export interface ShareFetchRequestPartition {
  partitionIndex: number
  acknowledgementBatches: ShareFetchRequestAcknowledgementBatch[]
}

export interface ShareFetchRequestTopic {
  topicId: string
  partitions: ShareFetchRequestPartition[]
}

export interface ShareFetchRequestForgottenTopic {
  topicId: string
  partitions: number[]
}

export type ShareFetchRequest = Parameters<typeof createRequest>

export interface ShareFetchResponseCurrentLeader {
  leaderId: number
  leaderEpoch: number
}

export interface ShareFetchResponseAcquiredRecords {
  firstOffset: bigint
  lastOffset: bigint
  deliveryCount: number
}

export interface ShareFetchResponsePartition {
  partitionIndex: number
  errorCode: number
  errorMessage: NullableString
  acknowledgeErrorCode: number
  acknowledgeErrorMessage: NullableString
  currentLeader: ShareFetchResponseCurrentLeader
  records: RecordsBatch[]
  acquiredRecords: ShareFetchResponseAcquiredRecords[]
}

export interface ShareFetchResponseTopic {
  topicId: string
  partitions: ShareFetchResponsePartition[]
}

export interface ShareFetchResponseNodeEndpoint {
  nodeId: number
  host: string
  port: number
  rack: NullableString
}

export interface ShareFetchResponse {
  throttleTimeMs: number
  errorCode: number
  errorMessage: NullableString
  acquisitionLockTimeoutMs: number
  responses: ShareFetchResponseTopic[]
  nodeEndpoints: ShareFetchResponseNodeEndpoint[]
}

/*
  ShareFetch Request (Version: 1) => group_id member_id share_session_epoch max_wait_ms min_bytes max_bytes max_records batch_size [topics] [forgotten_topics_data] TAG_BUFFER
    group_id => COMPACT_NULLABLE_STRING
    member_id => COMPACT_NULLABLE_STRING
    share_session_epoch => INT32
    max_wait_ms => INT32
    min_bytes => INT32
    max_bytes => INT32
    max_records => INT32
    batch_size => INT32
    topics => topic_id [partitions] TAG_BUFFER
      topic_id => UUID
      partitions => partition_index [acknowledgement_batches] TAG_BUFFER
        partition_index => INT32
        acknowledgement_batches => first_offset last_offset [acknowledge_types] TAG_BUFFER
          first_offset => INT64
          last_offset => INT64
          acknowledge_types => INT8
    forgotten_topics_data => topic_id [partitions] TAG_BUFFER
      topic_id => UUID
      partitions => INT32
*/
export function createRequest (
  groupId: NullableString,
  memberId: NullableString,
  shareSessionEpoch: number,
  maxWaitMs: number,
  minBytes: number,
  maxBytes: number,
  maxRecords: number,
  batchSize: number,
  topics: ShareFetchRequestTopic[],
  forgottenTopicsData: ShareFetchRequestForgottenTopic[]
): Writer {
  return Writer.create()
    .appendString(groupId)
    .appendString(memberId)
    .appendInt32(shareSessionEpoch)
    .appendInt32(maxWaitMs)
    .appendInt32(minBytes)
    .appendInt32(maxBytes)
    .appendInt32(maxRecords)
    .appendInt32(batchSize)
    .appendArray(topics, writeTopic)
    .appendArray(forgottenTopicsData, (w, t) => {
      w.appendUUID(t.topicId).appendArray(t.partitions, (w, p) => w.appendInt32(p), true, false)
    })
    .appendTaggedFields()
}

export function writeTopic (w: Writer, t: ShareFetchRequestTopic): void {
  w.appendUUID(t.topicId).appendArray(t.partitions, (w, p) => {
    w.appendInt32(p.partitionIndex).appendArray(p.acknowledgementBatches, (w, b) => {
      w.appendInt64(b.firstOffset)
        .appendInt64(b.lastOffset)
        .appendArray(b.acknowledgeTypes, (w, a) => w.appendInt8(a), true, false)
    })
  })
}

/*
  ShareFetch Response (Version: 1) => throttle_time_ms error_code error_message acquisition_lock_timeout_ms [responses] [node_endpoints] TAG_BUFFER
    throttle_time_ms => INT32
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    acquisition_lock_timeout_ms => INT32
    responses => topic_id [partitions] TAG_BUFFER
      topic_id => UUID
      partitions => partition_index error_code error_message acknowledge_error_code acknowledge_error_message current_leader records [acquired_records] TAG_BUFFER
        partition_index => INT32
        error_code => INT16
        error_message => COMPACT_NULLABLE_STRING
        acknowledge_error_code => INT16
        acknowledge_error_message => COMPACT_NULLABLE_STRING
        current_leader => leader_id leader_epoch TAG_BUFFER
          leader_id => INT32
          leader_epoch => INT32
        records => COMPACT_RECORDS
        acquired_records => first_offset last_offset delivery_count TAG_BUFFER
          first_offset => INT64
          last_offset => INT64
          delivery_count => INT16
    node_endpoints => node_id host port rack TAG_BUFFER
      node_id => INT32
      host => COMPACT_STRING
      port => INT32
      rack => COMPACT_NULLABLE_STRING
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): ShareFetchResponse {
  const errors: ResponseErrorWithLocation[] = []

  const throttleTimeMs = reader.readInt32()
  const errorCode = reader.readInt16()
  const errorMessage = reader.readNullableString()

  if (errorCode !== 0) {
    errors.push(['/', [errorCode, errorMessage]])
  }

  const response: ShareFetchResponse = {
    throttleTimeMs,
    errorCode,
    errorMessage,
    acquisitionLockTimeoutMs: reader.readInt32(),
    responses: reader.readArray((r, i) => {
      return {
        topicId: r.readUUID(),
        partitions: r.readArray((r, j) => {
          const partitionIndex = r.readInt32()
          const errorCode = r.readInt16()
          const errorMessage = r.readNullableString()
          const acknowledgeErrorCode = r.readInt16()
          const acknowledgeErrorMessage = r.readNullableString()

          if (errorCode !== 0) {
            errors.push([`/responses/${i}/partitions/${j}`, [errorCode, errorMessage]])
          }

          if (acknowledgeErrorCode !== 0) {
            errors.push([
              `/responses/${i}/partitions/${j}/acknowledge`,
              [acknowledgeErrorCode, acknowledgeErrorMessage]
            ])
          }

          const currentLeader = { leaderId: r.readInt32(), leaderEpoch: r.readInt32() }
          r.readTaggedFields()

          // We need to reduce the size by one to follow the COMPACT_RECORDS specification.
          let records: RecordsBatch[] = []
          let recordsSize = r.readUnsignedVarInt()
          if (recordsSize > 1) {
            recordsSize--
            records = readRecordsBatches(Reader.from(r.buffer.subarray(r.position, r.position + recordsSize)))
            r.skip(recordsSize)
          }

          return {
            partitionIndex,
            errorCode,
            errorMessage,
            acknowledgeErrorCode,
            acknowledgeErrorMessage,
            currentLeader,
            records,
            acquiredRecords: r.readArray(r => {
              return { firstOffset: r.readInt64(), lastOffset: r.readInt64(), deliveryCount: r.readInt16() }
            })
          }
        })
      }
    }),
    nodeEndpoints: reader.readArray(r => {
      return { nodeId: r.readInt32(), host: r.readString(), port: r.readInt32(), rack: r.readNullableString() }
    })
  }

  reader.readTaggedFields()

  // The response can still hold acquired records, for instance next to an acknowledgement error, so the caller
  // should take them from error.response and acknowledge them, or they stay locked until the acquisition lock expires.
  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const api = createAPI<ShareFetchRequest, ShareFetchResponse>(78, 1, createRequest, parseResponse)
