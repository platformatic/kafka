import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
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
  subscribedTopicNames: string[]
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
  authorizedOperations: number
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
  reader: Reader
): ConsumerGroupDescribeResponse {
  const errors: ResponseErrorWithLocation[] = []

  const response: ConsumerGroupDescribeResponse = {
    throttleTimeMs: reader.readInt32(),
    groups: reader.readArray((r, i) => {
      const errorCode = r.readInt16()
      const errorMessage = r.readNullableString()

      if (errorCode !== 0) {
        errors.push([`/groups/${i}`, [errorCode, errorMessage]])
      }

      const group = {
        errorCode,
        errorMessage,
        groupId: r.readString(),
        groupState: r.readString(),
        groupEpoch: r.readInt32(),
        assignmentEpoch: r.readInt32(),
        assignorName: r.readString(),
        members: r.readArray(r => {
          const memberId = r.readString()
          const instanceId = r.readNullableString()
          const rackId = r.readNullableString()
          const memberEpoch = r.readInt32()
          const clientId = r.readString()
          const clientHost = r.readString()
          const subscribedTopicNames = r.readArray(r => r.readString(true), true, false)
          const subscribedTopicRegex = r.readNullableString()
          const assignment = {
            topicPartitions: r.readArray(r => {
              const topicPartition = {
                topicId: r.readUUID(),
                topicName: r.readString(),
                partitions: r.readArray(() => r.readInt32(), true, false)
              }
              r.readTaggedFields()
              return topicPartition
            }, true, false)
          }
          r.readTaggedFields()

          const targetAssignment = {
            topicPartitions: r.readArray(r => {
              const topicPartition = {
                topicId: r.readUUID(),
                topicName: r.readString(),
                partitions: r.readArray(() => r.readInt32(), true, false)
              }
              r.readTaggedFields()
              return topicPartition
            }, true, false)
          }
          r.readTaggedFields()

          const member = {
            memberId,
            instanceId,
            rackId,
            memberEpoch,
            clientId,
            clientHost,
            subscribedTopicNames,
            subscribedTopicRegex,
            assignment,
            targetAssignment
          }
          r.readTaggedFields()
          return member
        }, true, false),
        authorizedOperations: r.readInt32()
      }
      r.readTaggedFields()
      return group
    }, true, false)
  }

  reader.readTaggedFields()

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
