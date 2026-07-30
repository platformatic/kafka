import { ResponseError } from '../../errors.ts'
import { type Reader } from '../../protocol/reader.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'
import { createRequestForVersion, type OffsetFetchRequest, type OffsetFetchResponse } from './offset-fetch-v0.ts'
export type {
  OffsetFetchRequest,
  OffsetFetchRequestGroup,
  OffsetFetchRequestTopic,
  OffsetFetchResponse,
  OffsetFetchResponseGroup,
  OffsetFetchResponsePartition,
  OffsetFetchResponseTopic
} from './offset-fetch-v0.ts'

export function createRequest (...args: OffsetFetchRequest): ReturnType<typeof createRequestForVersion> {
  return createRequestForVersion(...args, 1)
}
/*
  OffsetFetch Response (Version: 1) => [topics]
    topics => name [partitions]
      name => STRING
      partitions => partition_index committed_offset metadata error_code
        partition_index => INT32
        committed_offset => INT64
        metadata => NULLABLE_STRING
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
  const response = { throttleTimeMs: 0, topics, errorCode: 0, groups: [] }
  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }
  return response
}
export const api = createAPI<OffsetFetchRequest, OffsetFetchResponse>(9, 1, createRequest, parseResponse, false, false)
