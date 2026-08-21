import { ResponseError } from '../../errors.ts'
import { Reader } from '../../protocol/reader.ts'
import { readRecordsBatches, type RecordsBatch } from '../../protocol/records.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'
import { readKnownTaggedFields } from '../tagged-fields.ts'

export interface FetchRequestPartition {
  partition: number
  currentLeaderEpoch: number
  fetchOffset: bigint
  lastFetchedEpoch: number
  // logStartOffset: bigint - This is only used between brokers
  partitionMaxBytes: number
}

export interface FetchRequestTopic {
  topicId: string
  partitions: FetchRequestPartition[]
}

export type FetchRequestForgottenTopicsData =
  | { topicId: string, /** @deprecated Use topicId instead. */ topic?: string, partitions: number[] }
  | { /** @deprecated Use topicId instead. */ topic: string, topicId?: never, partitions: number[] }

export type FetchRequest = Parameters<typeof createRequest>

export interface FetchResponsePartitionAbortedTransaction {
  producerId: bigint
  firstOffset: bigint
}

export interface FetchResponsePartitionEpochEndOffset {
  epoch: number
  endOffset: bigint
}

export interface FetchResponsePartitionCurrentLeader {
  leaderId: number
  leaderEpoch: number
}

export interface FetchResponseNodeEndpoint {
  nodeId: number
  host: string
  port: number
  rack: string | null
}

export interface FetchResponsePartition {
  partitionIndex: number
  errorCode: number
  highWatermark: bigint
  lastStableOffset: bigint
  logStartOffset: bigint
  // The largest epoch and end offset known to diverge from the requested fetch offset.
  divergingEpoch?: FetchResponsePartitionEpochEndOffset
  // The current partition leader, or -1 values when the leader is unknown.
  currentLeader?: FetchResponsePartitionCurrentLeader
  // The snapshot to use when the requested offset precedes the log start offset.
  snapshotId?: FetchResponsePartitionEpochEndOffset
  abortedTransactions: FetchResponsePartitionAbortedTransaction[] | null
  preferredReadReplica: number
  records?: RecordsBatch[] | null
}

export interface FetchResponseTopic {
  topicId: string
  partitions: FetchResponsePartition[]
}

export type FetchResponse = {
  throttleTimeMs: number
  errorCode: number
  sessionId: number
  responses: FetchResponseTopic[]
  // Endpoints for leaders reported by partitions with leader-related errors.
  nodeEndpoints?: FetchResponseNodeEndpoint[]
}

/*
  Fetch Request (Version: 16) => max_wait_ms min_bytes max_bytes isolation_level session_id session_epoch [topics] [forgotten_topics_data] rack_id TAG_BUFFER
  max_wait_ms => INT32
  min_bytes => INT32
  max_bytes => INT32
  isolation_level => INT8
  session_id => INT32
  session_epoch => INT32
  topics => topic_id [partitions] TAG_BUFFER
    topic_id => UUID
    partitions => partition current_leader_epoch fetch_offset last_fetched_epoch log_start_offset partition_max_bytes TAG_BUFFER
      partition => INT32
      current_leader_epoch => INT32
      fetch_offset => INT64
      last_fetched_epoch => INT32
      log_start_offset => INT64
      partition_max_bytes => INT32
  forgotten_topics_data => topic_id [partitions] TAG_BUFFER
    topic_id => UUID
    partitions => INT32
  rack_id => COMPACT_STRING
*/
export function createRequest (
  maxWaitMs: number,
  minBytes: number,
  maxBytes: number,
  isolationLevel: number,
  sessionId: number,
  sessionEpoch: number,
  topics: FetchRequestTopic[],
  forgottenTopicsData: FetchRequestForgottenTopicsData[],
  rackId: string
): Writer {
  return Writer.create()
    .appendInt32(maxWaitMs)
    .appendInt32(minBytes)
    .appendInt32(maxBytes)
    .appendInt8(isolationLevel)
    .appendInt32(sessionId)
    .appendInt32(sessionEpoch)
    .appendArray(topics, (w, t) => {
      w.appendUUID(t.topicId).appendArray(t.partitions, (w, p) => {
        w.appendInt32(p.partition)
          .appendInt32(p.currentLeaderEpoch)
          .appendInt64(p.fetchOffset)
          .appendInt32(p.lastFetchedEpoch)
          .appendInt64(-1n)
          .appendInt32(p.partitionMaxBytes)
      })
    })
    .appendArray(forgottenTopicsData, (w, t) => {
      w.appendUUID(t.topicId ?? t.topic).appendArray(
        t.partitions,
        (w, p) => {
          w.appendInt32(p)
        },
        true,
        false
      )
    })
    .appendString(rackId)
    .appendTaggedFields()
}

/*
  Fetch Response (Version: 16) => throttle_time_ms error_code session_id [responses] node_endpoints TAG_BUFFER
    throttle_time_ms => INT32
    error_code => INT16
    session_id => INT32
    responses => topic_id [partitions] TAG_BUFFER
      topic_id => UUID
      partitions => partition_index error_code high_watermark last_stable_offset log_start_offset diverging_epoch current_leader snapshot_id [aborted_transactions] preferred_read_replica records TAG_BUFFER
        partition_index => INT32
        error_code => INT16
        high_watermark => INT64
        last_stable_offset => INT64
        log_start_offset => INT64
        diverging_epoch => epoch end_offset
        current_leader => leader_id leader_epoch
        snapshot_id => end_offset epoch
        aborted_transactions => producer_id first_offset TAG_BUFFER
          producer_id => INT64
          first_offset => INT64
        preferred_read_replica => INT32
        records => COMPACT_RECORDS
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): FetchResponse {
  const errors: ResponseErrorWithLocation[] = []

  const throttleTimeMs = reader.readInt32()
  const errorCode = reader.readInt16()

  if (errorCode !== 0) {
    errors.push(['', [errorCode, null]])
  }

  const response: FetchResponse = {
    throttleTimeMs,
    errorCode,
    sessionId: reader.readInt32(),
    nodeEndpoints: [],
    responses: reader.readArray((r, i) => {
      return {
        topicId: r.readUUID(),
        partitions: r.readArray((r, j) => {
          const partition: FetchResponsePartition = {
            partitionIndex: r.readInt32(),
            errorCode: r.readInt16(),
            highWatermark: r.readInt64(),
            lastStableOffset: r.readInt64(),
            logStartOffset: r.readInt64(),
            divergingEpoch: { epoch: -1, endOffset: -1n },
            currentLeader: { leaderId: -1, leaderEpoch: -1 },
            snapshotId: { endOffset: -1n, epoch: -1 },
            abortedTransactions: r.readNullableArray(r => {
              return {
                producerId: r.readInt64(),
                firstOffset: r.readInt64()
              }
            }),
            preferredReadReplica: r.readInt32(),
            records: []
          }

          if (partition.errorCode !== 0) {
            errors.push([`/responses/${i}/partitions/${j}`, [partition.errorCode, null]])
          }

          // We need to reduce the size by one to follow the COMPACT_RECORDS specification.
          let recordsSize = r.readUnsignedVarInt()
          if (recordsSize === 0) {
            partition.records = null
          }
          if (recordsSize > 1) {
            recordsSize--
            partition.records = readRecordsBatches(Reader.from(r.buffer.subarray(r.position, r.position + recordsSize)))
            r.skip(recordsSize)
          }

          readKnownTaggedFields(r, {
            0: r => { partition.divergingEpoch = { epoch: r.readInt32(), endOffset: r.readInt64() } },
            1: r => { partition.currentLeader = { leaderId: r.readInt32(), leaderEpoch: r.readInt32() } },
            2: r => { partition.snapshotId = { endOffset: r.readInt64(), epoch: r.readInt32() } }
          })

          return partition
        }, true, false)
      }
    })
  }
  readKnownTaggedFields(reader, {
    0: r => {
      response.nodeEndpoints = r.readArray(r => ({
        nodeId: r.readInt32(),
        host: r.readString(),
        port: r.readInt32(),
        rack: r.readNullableString()
      }))
    }
  })

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const api = createAPI<FetchRequest, FetchResponse>(1, 16, createRequest, parseResponse)
