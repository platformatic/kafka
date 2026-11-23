import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export interface ShareAcknowledgeRequestAcknowledgementBatch {
  firstOffset: bigint
  lastOffset: bigint
  acknowledgeTypes: number[]
}

export interface ShareAcknowledgeRequestPartition {
  partitionIndex: number
  acknowledgementBatches: ShareAcknowledgeRequestAcknowledgementBatch[]
}

export interface ShareAcknowledgeRequestTopic {
  topicId: string
  partitions: ShareAcknowledgeRequestPartition[]
}

export type ShareAcknowledgeRequest = Parameters<typeof createRequest>

export interface ShareAcknowledgeResponseCurrentLeader {
  leaderId: number
  leaderEpoch: number
}

export interface ShareAcknowledgeResponsePartition {
  partitionIndex: number
  errorCode: number
  errorMessage: NullableString
  currentLeader: ShareAcknowledgeResponseCurrentLeader
}

export interface ShareAcknowledgeResponseTopic {
  topicId: string
  partitions: ShareAcknowledgeResponsePartition[]
}

export interface ShareAcknowledgeResponseNodeEndpoint {
  nodeId: number
  host: string
  port: number
  rack: NullableString
}

export interface ShareAcknowledgeResponse {
  throttleTimeMs: number
  errorCode: number
  errorMessage: NullableString
  responses: ShareAcknowledgeResponseTopic[]
  nodeEndpoints: ShareAcknowledgeResponseNodeEndpoint[]
}

/*
  ShareAcknowledge Request (Version: 1) => group_id member_id share_session_epoch [topics] TAG_BUFFER
    group_id => COMPACT_NULLABLE_STRING
    member_id => COMPACT_NULLABLE_STRING
    share_session_epoch => INT32
    topics => topic_id [partitions] TAG_BUFFER
      topic_id => UUID
      partitions => partition_index [acknowledgement_batches] TAG_BUFFER
        partition_index => INT32
        acknowledgement_batches => first_offset last_offset [acknowledge_types] TAG_BUFFER
          first_offset => INT64
          last_offset => INT64
          acknowledge_types => INT8
*/
export function createRequest (
  groupId: NullableString,
  memberId: NullableString,
  shareSessionEpoch: number,
  topics: ShareAcknowledgeRequestTopic[]
): Writer {
  return Writer.create()
    .appendString(groupId)
    .appendString(memberId)
    .appendInt32(shareSessionEpoch)
    .appendArray(topics, writeTopic)
    .appendTaggedFields()
}

export function writeTopic (w: Writer, t: ShareAcknowledgeRequestTopic): void {
  w.appendUUID(t.topicId).appendArray(t.partitions, (w, p) => {
    w.appendInt32(p.partitionIndex).appendArray(p.acknowledgementBatches, (w, b) => {
      w.appendInt64(b.firstOffset)
        .appendInt64(b.lastOffset)
        .appendArray(b.acknowledgeTypes, (w, a) => w.appendInt8(a), true, false)
    })
  })
}

export function readResponses (reader: Reader, errors: ResponseErrorWithLocation[]): ShareAcknowledgeResponseTopic[] {
  return reader.readArray((r, i) => {
    return {
      topicId: r.readUUID(),
      partitions: r.readArray((r, j) => {
        const partitionIndex = r.readInt32()
        const errorCode = r.readInt16()
        const errorMessage = r.readNullableString()

        if (errorCode !== 0) {
          errors.push([`/responses/${i}/partitions/${j}`, [errorCode, errorMessage]])
        }

        const currentLeader = { leaderId: r.readInt32(), leaderEpoch: r.readInt32() }
        r.readTaggedFields()

        return { partitionIndex, errorCode, errorMessage, currentLeader }
      })
    }
  })
}

export function readNodeEndpoints (reader: Reader): ShareAcknowledgeResponseNodeEndpoint[] {
  return reader.readArray(r => {
    return { nodeId: r.readInt32(), host: r.readString(), port: r.readInt32(), rack: r.readNullableString() }
  })
}

/*
  ShareAcknowledge Response (Version: 1) => throttle_time_ms error_code error_message [responses] [node_endpoints] TAG_BUFFER
    throttle_time_ms => INT32
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    responses => topic_id [partitions] TAG_BUFFER
      topic_id => UUID
      partitions => partition_index error_code error_message current_leader TAG_BUFFER
        partition_index => INT32
        error_code => INT16
        error_message => COMPACT_NULLABLE_STRING
        current_leader => leader_id leader_epoch TAG_BUFFER
          leader_id => INT32
          leader_epoch => INT32
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
): ShareAcknowledgeResponse {
  const errors: ResponseErrorWithLocation[] = []

  const throttleTimeMs = reader.readInt32()
  const errorCode = reader.readInt16()
  const errorMessage = reader.readNullableString()

  if (errorCode !== 0) {
    errors.push(['/', [errorCode, errorMessage]])
  }

  const response: ShareAcknowledgeResponse = {
    throttleTimeMs,
    errorCode,
    errorMessage,
    responses: readResponses(reader, errors),
    nodeEndpoints: readNodeEndpoints(reader)
  }

  reader.readTaggedFields()

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const api = createAPI<ShareAcknowledgeRequest, ShareAcknowledgeResponse>(79, 1, createRequest, parseResponse)
