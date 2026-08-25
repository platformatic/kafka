import { type NullableString } from '../../protocol/definitions.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'
import {
  parseResponse,
  type OffsetCommitResponse,
  type OffsetCommitRequestPartition,
  type OffsetCommitRequestTopic,
  type OffsetCommitResponsePartition,
  type OffsetCommitResponseTopic
} from './offset-commit-v0.ts'
export type {
  OffsetCommitRequestPartition,
  OffsetCommitRequestTopic,
  OffsetCommitResponse,
  OffsetCommitResponsePartition,
  OffsetCommitResponseTopic
}
export { parseResponse }
export type OffsetCommitRequest = Parameters<typeof createRequest>
/*
  OffsetCommit Request (Version: 2) => group_id generation_id member_id retention_time_ms [topics]
    group_id => STRING
    generation_id => INT32
    member_id => STRING
    retention_time_ms => INT64
    topics => name [partitions]
      name => STRING
      partitions => partition_index committed_offset metadata
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
    .appendString(memberId ?? '', false)
    .appendInt64(-1n)
    .appendArray(
      topics,
      (w, topic) => {
        w.appendString(topic.name, false).appendArray(
          topic.partitions,
          (w, partition: OffsetCommitRequestPartition) =>
            w
              .appendInt32(partition.partitionIndex)
              .appendInt64(partition.committedOffset)
              .appendString(partition.committedMetadata, false),
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
  2,
  createRequest,
  parseResponse,
  false,
  false
)
