import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'
import {
  parseResponse as parseV1Response,
  type ShareFetchRequestForgottenTopic,
  type ShareFetchRequestTopic,
  type ShareFetchResponse,
  writeTopic
} from './share-fetch-v1.ts'

export type {
  ShareFetchRequestAcknowledgementBatch,
  ShareFetchRequestForgottenTopic,
  ShareFetchRequestPartition,
  ShareFetchRequestTopic,
  ShareFetchResponse,
  ShareFetchResponseAcquiredRecords,
  ShareFetchResponseCurrentLeader,
  ShareFetchResponseNodeEndpoint,
  ShareFetchResponsePartition,
  ShareFetchResponseTopic
} from './share-fetch-v1.ts'

export type ShareFetchRequest = Parameters<typeof createRequest>

/*
  ShareFetch Request (Version: 2) => group_id member_id share_session_epoch max_wait_ms min_bytes max_bytes max_records batch_size share_acquire_mode is_renew_ack [topics] [forgotten_topics_data] TAG_BUFFER
    group_id => COMPACT_NULLABLE_STRING
    member_id => COMPACT_NULLABLE_STRING
    share_session_epoch => INT32
    max_wait_ms => INT32
    min_bytes => INT32
    max_bytes => INT32
    max_records => INT32
    batch_size => INT32
    share_acquire_mode => INT8
    is_renew_ack => BOOLEAN
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
  shareAcquireMode: number,
  isRenewAck: boolean,
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
    .appendInt8(shareAcquireMode)
    .appendBoolean(isRenewAck)
    .appendArray(topics, writeTopic)
    .appendArray(forgottenTopicsData, (w, t) => {
      w.appendUUID(t.topicId).appendArray(t.partitions, (w, p) => w.appendInt32(p), true, false)
    })
    .appendTaggedFields()
}

/* ShareFetch v2 has the same response layout as v1. */
export function parseResponse (
  correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): ShareFetchResponse {
  return parseV1Response(correlationId, apiKey, apiVersion, reader)
}

export const api = createAPI<ShareFetchRequest, ShareFetchResponse>(78, 2, createRequest, parseResponse)
