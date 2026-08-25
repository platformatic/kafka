import { ResponseError } from '../../errors.ts'
import { Reader } from '../../protocol/reader.ts'
import { readRecordsBatches, type RecordsBatch } from '../../protocol/records.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export interface FetchRequestPartition {
  partition: number
  currentLeaderEpoch: number
  fetchOffset: bigint
  lastFetchedEpoch: number
  partitionMaxBytes: number
}

export interface FetchRequestTopic {
  topicId: string
  partitions: FetchRequestPartition[]
}

export type FetchRequestForgottenTopicsData =
  | { topicId: string, /** @deprecated Use topicId instead. */ topic?: string, partitions: number[] }
  | { /** @deprecated Use topicId instead. */ topic: string, topicId?: never, partitions: number[] }

// Keep the current Consumer's Fetch call contract across all supported versions.
export type FetchRequest = Parameters<typeof createRequest>

export interface FetchResponsePartitionAbortedTransaction {
  producerId: bigint
  firstOffset: bigint
}

export interface FetchResponsePartition {
  partitionIndex: number
  errorCode: number
  highWatermark: bigint
  lastStableOffset: bigint
  logStartOffset: bigint
  abortedTransactions: FetchResponsePartitionAbortedTransaction[] | null
  preferredReadReplica: number
  records: RecordsBatch[] | null
}

export interface FetchResponseTopic {
  topicId: string
  partitions: FetchResponsePartition[]
}

export interface FetchResponse {
  throttleTimeMs: number
  errorCode: number
  sessionId: number
  responses: FetchResponseTopic[]
}

/*
  Fetch Request (Version: 5) => replica_id max_wait_ms min_bytes max_bytes isolation_level [topics]
    replica_id => INT32
    max_wait_ms => INT32
    min_bytes => INT32
    max_bytes => INT32
    isolation_level => INT8
    topics => topic [partitions]
      topic => STRING
      partitions => partition fetch_offset log_start_offset partition_max_bytes
        partition => INT32
        fetch_offset => INT64
        log_start_offset => INT64
        partition_max_bytes => INT32
*/
export function createRequest (
  maxWaitMs: number,
  minBytes: number,
  maxBytes: number,
  isolationLevel: number,
  _sessionId: number,
  _sessionEpoch: number,
  topics: FetchRequestTopic[],
  _forgottenTopicsData: FetchRequestForgottenTopicsData[],
  _rackId: string
): Writer {
  return Writer.create()
    .appendInt32(-1)
    .appendInt32(maxWaitMs)
    .appendInt32(minBytes)
    .appendInt32(maxBytes)
    .appendInt8(isolationLevel)
    .appendArray(
      topics,
      (w, topic) => {
        w.appendString(topic.topicId, false).appendArray(
          topic.partitions,
          (w, partition) => w.appendInt32(partition.partition).appendInt64(partition.fetchOffset).appendInt64(-1n).appendInt32(partition.partitionMaxBytes),
          false,
          false
        )
      },
      false,
      false
    )
}

/*
  Fetch Response (Version: 5) => throttle_time_ms [responses]
    throttle_time_ms => INT32
    responses => topic [partitions]
      topic => STRING
      partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] records
        partition_index => INT32
        error_code => INT16
        high_watermark => INT64
        last_stable_offset => INT64
        log_start_offset => INT64
        aborted_transactions => producer_id first_offset
          producer_id => INT64
          first_offset => INT64
        records => RECORDS
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): FetchResponse {
  const errors: ResponseErrorWithLocation[] = []
  const response: FetchResponse = {
    throttleTimeMs: reader.readInt32(),
    errorCode: 0,
    sessionId: 0,
    responses: reader.readArray(
      (r, i) => ({
        topicId: r.readString(false),
        partitions: r.readArray(
          (r, j) => {
            const partition: FetchResponsePartition = {
              partitionIndex: r.readInt32(),
              errorCode: r.readInt16(),
              highWatermark: r.readInt64(),
              lastStableOffset: r.readInt64(),
              logStartOffset: r.readInt64(),
              abortedTransactions: r.readNullableArray(
                r => ({ producerId: r.readInt64(), firstOffset: r.readInt64() }),
                false,
                false
              ),
              records: null,
              preferredReadReplica: -1
            }

            if (partition.errorCode !== 0) {
              errors.push([`/responses/${i}/partitions/${j}`, [partition.errorCode, null]])
            }

            const records = r.readNullableBytes(false)
            if (records === null) {
              partition.records = null
            } else if (records.length > 0) {
              partition.records = readRecordsBatches(Reader.from(records))
            } else {
              partition.records = []
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
  }

  if (errors.length > 0) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }
  return response
}

export const api = createAPI<FetchRequest, FetchResponse>(1, 5, createRequest, parseResponse, false, false)
