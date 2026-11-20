import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'

export type ShareGroupHeartbeatRequest = Parameters<typeof createRequest>

export interface ShareGroupHeartbeatResponseAssignmentTopicPartition {
  topicId: string
  partitions: number[]
}

export interface ShareGroupHeartbeatResponseAssignment {
  topicPartitions: ShareGroupHeartbeatResponseAssignmentTopicPartition[]
}

export interface ShareGroupHeartbeatResponse {
  throttleTimeMs: number
  errorCode: number
  errorMessage: NullableString
  memberId: NullableString
  memberEpoch: number
  heartbeatIntervalMs: number
  assignment: ShareGroupHeartbeatResponseAssignment | null
}

/*
  ShareGroupHeartbeat Request (Version: 1) => group_id member_id member_epoch rack_id [subscribed_topic_names] TAG_BUFFER
    group_id => COMPACT_STRING
    member_id => COMPACT_STRING
    member_epoch => INT32
    rack_id => COMPACT_NULLABLE_STRING
    subscribed_topic_names => COMPACT_STRING
*/
export function createRequest (
  groupId: string,
  memberId: string,
  memberEpoch: number,
  rackId: NullableString,
  subscribedTopicNames: string[] | null
): Writer {
  return Writer.create()
    .appendString(groupId)
    .appendString(memberId)
    .appendInt32(memberEpoch)
    .appendString(rackId)
    .appendArray(subscribedTopicNames, (w, t) => w.appendString(t), true, false)
    .appendTaggedFields()
}

/*
  ShareGroupHeartbeat Response (Version: 1) => throttle_time_ms error_code error_message member_id member_epoch heartbeat_interval_ms assignment TAG_BUFFER
    throttle_time_ms => INT32
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    member_id => COMPACT_NULLABLE_STRING
    member_epoch => INT32
    heartbeat_interval_ms => INT32
    assignment => [topic_partitions] TAG_BUFFER
      topic_partitions => topic_id [partitions] TAG_BUFFER
        topic_id => UUID
        partitions => INT32
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): ShareGroupHeartbeatResponse {
  const response: ShareGroupHeartbeatResponse = {
    throttleTimeMs: reader.readInt32(),
    errorCode: reader.readInt16(),
    errorMessage: reader.readNullableString(),
    memberId: reader.readNullableString(),
    memberEpoch: reader.readInt32(),
    heartbeatIntervalMs: reader.readInt32(),
    assignment: reader.readNullableStruct(() => {
      const assignment = {
        topicPartitions: reader.readArray(r => {
          return {
            topicId: r.readUUID(),
            partitions: r.readArray(r => r.readInt32(), true, false)
          }
        })
      }
      reader.readTaggedFields()
      return assignment
    })
  }
  reader.readTaggedFields()

  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '/': [response.errorCode, response.errorMessage] }, response)
  }

  return response
}

export const api = createAPI<ShareGroupHeartbeatRequest, ShareGroupHeartbeatResponse>(
  76,
  1,
  createRequest,
  parseResponse
)
