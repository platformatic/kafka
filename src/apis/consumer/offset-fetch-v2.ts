import { ResponseError } from '../../errors.ts'
import { type Reader } from '../../protocol/reader.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'
import { Writer } from '../../protocol/writer.ts'
import { type OffsetFetchRequest, type OffsetFetchResponse } from './offset-fetch-v0.ts'
export type {
  OffsetFetchRequest,
  OffsetFetchRequestGroup,
  OffsetFetchRequestTopic,
  OffsetFetchResponse,
  OffsetFetchResponseGroup,
  OffsetFetchResponsePartition,
  OffsetFetchResponseTopic
} from './offset-fetch-v0.ts'

export function createRequest (groups: OffsetFetchRequest[0], _requireStable: boolean): Writer {
  const group = groups[0] ?? { groupId: '', topics: null }
  return Writer.create()
    .appendString(group.groupId, false)
    .appendArray(
      group.topics,
      (writer, topic) => writer.appendString(topic.name, false).appendArray(
        topic.partitionIndexes,
        (writer, partitionIndex) => writer.appendInt32(partitionIndex),
        false,
        false
      ),
      false,
      false
    )
}
/*
  OffsetFetch Response (Version: 2) => [topics] error_code
    topics => name [partitions]
      name => STRING
      partitions => partition_index committed_offset metadata error_code
        partition_index => INT32
        committed_offset => INT64
        metadata => NULLABLE_STRING
        error_code => INT16
    error_code => INT16
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): OffsetFetchResponse {
  const errors: ResponseErrorWithLocation[] = []
  const topics = reader.readArray(
    (r, topicIndex) => ({
      name: r.readString(false),
      partitions: r.readArray(
        (r, partitionIndex) => {
          const partition = {
            partitionIndex: r.readInt32(),
            committedOffset: r.readInt64(),
            committedLeaderEpoch: -1,
            metadata: r.readNullableString(false),
            errorCode: r.readInt16()
          }
          if (partition.errorCode !== 0) {
            errors.push([`/topics/${topicIndex}/partitions/${partitionIndex}`, [partition.errorCode, null]])
          }
          return partition
        },
        false,
        false
      )
    }),
    false,
    false
  )
  const response = { throttleTimeMs: 0, topics, errorCode: reader.readInt16(), groups: [] }
  if (response.errorCode !== 0) {
    errors.push(['/errorCode', [response.errorCode, null]])
  }
  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }
  return response
}
export const api = createAPI<OffsetFetchRequest, OffsetFetchResponse>(9, 2, createRequest, parseResponse, false, false)
