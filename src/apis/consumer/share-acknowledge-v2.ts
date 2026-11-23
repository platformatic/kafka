import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'
import {
  readNodeEndpoints,
  readResponses,
  type ShareAcknowledgeRequestTopic,
  type ShareAcknowledgeResponse as ShareAcknowledgeV1Response,
  writeTopic
} from './share-acknowledge-v1.ts'

export type {
  ShareAcknowledgeRequestAcknowledgementBatch,
  ShareAcknowledgeRequestPartition,
  ShareAcknowledgeRequestTopic,
  ShareAcknowledgeResponseCurrentLeader,
  ShareAcknowledgeResponseNodeEndpoint,
  ShareAcknowledgeResponsePartition,
  ShareAcknowledgeResponseTopic
} from './share-acknowledge-v1.ts'

export type ShareAcknowledgeRequest = Parameters<typeof createRequest>

export interface ShareAcknowledgeResponse extends ShareAcknowledgeV1Response {
  acquisitionLockTimeoutMs: number
}

/*
  ShareAcknowledge Request (Version: 2) => group_id member_id share_session_epoch is_renew_ack [topics] TAG_BUFFER
    group_id => COMPACT_NULLABLE_STRING
    member_id => COMPACT_NULLABLE_STRING
    share_session_epoch => INT32
    is_renew_ack => BOOLEAN
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
  isRenewAck: boolean,
  topics: ShareAcknowledgeRequestTopic[]
): Writer {
  return Writer.create()
    .appendString(groupId)
    .appendString(memberId)
    .appendInt32(shareSessionEpoch)
    .appendBoolean(isRenewAck)
    .appendArray(topics, writeTopic)
    .appendTaggedFields()
}

/*
  ShareAcknowledge Response (Version: 2) => throttle_time_ms error_code error_message acquisition_lock_timeout_ms [responses] [node_endpoints] TAG_BUFFER
    throttle_time_ms => INT32
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    acquisition_lock_timeout_ms => INT32
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
    acquisitionLockTimeoutMs: reader.readInt32(),
    responses: readResponses(reader, errors),
    nodeEndpoints: readNodeEndpoints(reader)
  }

  reader.readTaggedFields()

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const api = createAPI<ShareAcknowledgeRequest, ShareAcknowledgeResponse>(79, 2, createRequest, parseResponse)
