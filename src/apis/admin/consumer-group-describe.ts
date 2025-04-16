import type BufferList from 'bl'
import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export type ConsumerGroupDescribeRequest = Parameters<typeof createRequest>

export interface ConsumerGroupDescribeResponseMemberTopic {
  topicId: string
  topicName: string
  partitions: number[]
}

export interface ConsumerGroupDescribeResponseMemberAssignment {
  topicPartitions: ConsumerGroupDescribeResponseMemberTopic[]
}

export interface ConsumerGroupDescribeResponseMember {
  memberId: string
  instanceId: NullableString
  rackId: NullableString
  memberEpoch: number
  clientId: string
  clientHost: string
  subscribedTopicNames: string
  subscribedTopicRegex: NullableString
  assignment: ConsumerGroupDescribeResponseMemberAssignment
  targetAssignment: ConsumerGroupDescribeResponseMemberAssignment
}

export interface ConsumerGroupDescribeResponseGroup {
  errorCode: number
  errorMessage: NullableString
  groupId: string
  groupState: string
  groupEpoch: number
  assignmentEpoch: number
  assignorName: string
  members: ConsumerGroupDescribeResponseMember[]
}

export interface ConsumerGroupDescribeResponse {
  throttleTimeMs: number
  groups: ConsumerGroupDescribeResponseGroup[]
}

/*
ConsumerGroupDescribe Request (Version: 0) => [group_ids] include_authorized_operations TAG_BUFFER
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
  ConsumerGroupDescribe Response (Version: 0) => throttle_time_ms [groups] TAG_BUFFER
    throttle_time_ms => INT32
    groups => error_code error_message group_id group_state group_epoch assignment_epoch assignor_name [members] authorized_operations TAG_BUFFER
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
      group_id => COMPACT_STRING
      group_state => COMPACT_STRING
      group_epoch => INT32
      assignment_epoch => INT32
      assignor_name => COMPACT_STRING
      members => member_id instance_id rack_id member_epoch client_id client_host [subscribed_topic_names] subscribed_topic_regex assignment target_assignment TAG_BUFFER
        member_id => COMPACT_STRING
        instance_id => COMPACT_NULLABLE_STRING
        rack_id => COMPACT_NULLABLE_STRING
        member_epoch => INT32
        client_id => COMPACT_STRING
        client_host => COMPACT_STRING
        subscribed_topic_names => COMPACT_STRING
        subscribed_topic_regex => COMPACT_NULLABLE_STRING
        assignment => [topic_partitions] TAG_BUFFER
          topic_partitions => topic_id topic_name [partitions] TAG_BUFFER
            topic_id => UUID
            topic_name => COMPACT_STRING
            partitions => INT32
        target_assignment => [topic_partitions] TAG_BUFFER
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
  raw: BufferList
): ConsumerGroupDescribeResponse {
  const reader = Reader.from(raw)
  const errors: ResponseErrorWithLocation[] = []

  const response: ConsumerGroupDescribeResponse = {
    throttleTimeMs: reader.readInt32(),
    groups: reader.readArray((r, i) => {
      const errorCode = r.readInt16()

      if (errorCode !== 0) {
        errors.push([`/groups/${i}`, errorCode])
      }

      return {
        errorCode,
        errorMessage: r.readNullableString(),
        groupId: r.readString(),
        groupState: r.readString(),
        groupEpoch: r.readInt32(),
        assignmentEpoch: r.readInt32(),
        assignorName: r.readString(),
        members: r.readArray(r => {
          return {
            memberId: r.readString(),
            instanceId: r.readNullableString(),
            rackId: r.readNullableString(),
            memberEpoch: r.readInt32(),
            clientId: r.readString(),
            clientHost: r.readString(),
            subscribedTopicNames: r.readString(),
            subscribedTopicRegex: r.readNullableString(),
            assignment: {
              topicPartitions: r.readArray(r => {
                return {
                  topicId: r.readUUID(),
                  topicName: r.readString(),
                  partitions: r.readArray(() => r.readInt32())
                }
              })
            },
            targetAssignment: {
              topicPartitions: r.readArray(r => {
                return {
                  topicId: r.readUUID(),
                  topicName: r.readString(),
                  partitions: r.readArray(() => r.readInt32())
                }
              })
            }
          }
        }),
        authorizedOperations: r.readInt32()
      }
    })
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const api = createAPI<ConsumerGroupDescribeRequest, ConsumerGroupDescribeResponse>(
  69,
  0,
  createRequest,
  parseResponse
)
