import { type NullableString } from '../../protocol/definitions.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'
import {
  parseResponse,
  type OffsetCommitRequestTopic,
  type OffsetCommitResponse
} from './offset-commit-v0.ts'
export type {
  OffsetCommitRequestPartition,
  OffsetCommitRequestTopic,
  OffsetCommitResponse,
  OffsetCommitResponsePartition,
  OffsetCommitResponseTopic
} from './offset-commit-v0.ts'

export type OffsetCommitRequest = Parameters<typeof createRequest>

/*
  OffsetCommit Request (Version: 1) => group_id generation_id member_id [topics]
    group_id => STRING
    generation_id => INT32
    member_id => STRING
    topics => name [partitions]
      name => STRING
      partitions => partition_index committed_offset commit_timestamp metadata
        partition_index => INT32
        committed_offset => INT64
        metadata => NULLABLE_STRING
*/
export function createRequest (
  groupId: string,
  generationId: number,
  memberId: string,
  _groupInstanceId: NullableString,
  topics: OffsetCommitRequestTopic[]
): Writer {
  return Writer.create()
    .appendString(groupId, false)
    .appendInt32(generationId)
    .appendString(memberId, false)
    .appendArray(
      topics,
      (w, topic) => {
        w.appendString(topic.name, false).appendArray(
          topic.partitions,
          (w, partition) => {
            w.appendInt32(partition.partitionIndex)
              .appendInt64(partition.committedOffset)
              .appendInt64(-1n)
              .appendString(partition.committedMetadata, false)
          },
          false,
          false
        )
      },
      false,
      false
    )
}

export const api = createAPI<OffsetCommitRequest, OffsetCommitResponse>(
  8,
  1,
  createRequest,
  parseResponse,
  false,
  false
)
