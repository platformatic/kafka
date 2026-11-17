import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export type ShareGroupDescribeRequest = Parameters<typeof createRequest>

export interface ShareGroupDescribeResponseMemberTopic {
  topicId: string
  topicName: string
  partitions: number[]
}

export interface ShareGroupDescribeResponseMemberAssignment {
  topicPartitions: ShareGroupDescribeResponseMemberTopic[]
}

export interface ShareGroupDescribeResponseMember {
  memberId: string
  rackId: NullableString
  memberEpoch: number
  clientId: string
  clientHost: string
  subscribedTopicNames: string[]
  assignment: ShareGroupDescribeResponseMemberAssignment
}

export interface ShareGroupDescribeResponseGroup {
  errorCode: number
  errorMessage: NullableString
  groupId: string
  groupState: string
  groupEpoch: number
  assignmentEpoch: number
  assignorName: string
  members: ShareGroupDescribeResponseMember[]
  authorizedOperations: number
}

export interface ShareGroupDescribeResponse {
  throttleTimeMs: number
  groups: ShareGroupDescribeResponseGroup[]
}

/*
  ShareGroupDescribe Request (Version: 1) => [group_ids] include_authorized_operations TAG_BUFFER
    group_ids => COMPACT_STRING
    include_authorized_operations => BOOLEAN
*/
export function createRequest (groupIds: string[], includeAuthorizedOperations: boolean): Writer {
  return Writer.create()
    .appendArray(groupIds, (w, r) => w.appendString(r), true, false)
    .appendBoolean(includeAuthorizedOperations)
    .appendTaggedFields()
}

/*
  ShareGroupDescribe Response (Version: 1) => throttle_time_ms [groups] TAG_BUFFER
    throttle_time_ms => INT32
    groups => error_code error_message group_id group_state group_epoch assignment_epoch assignor_name [members] authorized_operations TAG_BUFFER
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
      group_id => COMPACT_STRING
      group_state => COMPACT_STRING
      group_epoch => INT32
      assignment_epoch => INT32
      assignor_name => COMPACT_STRING
      members => member_id rack_id member_epoch client_id client_host [subscribed_topic_names] assignment TAG_BUFFER
        member_id => COMPACT_STRING
        rack_id => COMPACT_NULLABLE_STRING
        member_epoch => INT32
        client_id => COMPACT_STRING
        client_host => COMPACT_STRING
        subscribed_topic_names => COMPACT_STRING
        assignment => [topic_partitions] TAG_BUFFER
          topic_partitions => topic_id topic_name [partitions] TAG_BUFFER
            topic_id => UUID
            topic_name => COMPACT_STRING
            partitions => INT32
      authorized_operations => INT32
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): ShareGroupDescribeResponse {
  const errors: ResponseErrorWithLocation[] = []

  const response: ShareGroupDescribeResponse = {
    throttleTimeMs: reader.readInt32(),
    groups: reader.readArray((r, i) => {
      const errorCode = r.readInt16()
      const errorMessage = r.readNullableString()

      if (errorCode !== 0) {
        errors.push([`/groups/${i}`, [errorCode, errorMessage]])
      }

      return {
        errorCode,
        errorMessage,
        groupId: r.readString(),
        groupState: r.readString(),
        groupEpoch: r.readInt32(),
        assignmentEpoch: r.readInt32(),
        assignorName: r.readString(),
        members: r.readArray(r => {
          const memberId = r.readString()
          const rackId = r.readNullableString()
          const memberEpoch = r.readInt32()
          const clientId = r.readString()
          const clientHost = r.readString()
          const subscribedTopicNames = r.readArray(r => r.readString(), true, false)
          const assignment = {
            topicPartitions: r.readArray(r => {
              return {
                topicId: r.readUUID(),
                topicName: r.readString(),
                partitions: r.readArray(r => r.readInt32(), true, false)
              }
            })
          }
          r.readTaggedFields()

          return { memberId, rackId, memberEpoch, clientId, clientHost, subscribedTopicNames, assignment }
        }),
        authorizedOperations: r.readInt32()
      }
    })
  }

  reader.readTaggedFields()

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const api = createAPI<ShareGroupDescribeRequest, ShareGroupDescribeResponse>(77, 1, createRequest, parseResponse)
