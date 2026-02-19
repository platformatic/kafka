import { ResponseError } from '../../errors.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export interface DescribeLogDirsRequestTopic {
  name: string
  partitions: number[]
}

export type DescribeLogDirsRequest = Parameters<typeof createRequest>

export interface DescribeLogDirsResponsePartition {
  partitionIndex: number
  partitionSize: bigint
  offsetLag: bigint
  isFutureKey: boolean
}

export interface DescribeLogDirsResponseTopic {
  name: string
  partitions: DescribeLogDirsResponsePartition[]
}

export interface DescribeLogDirsResponseResult {
  errorCode: number
  logDir: string
  topics: DescribeLogDirsResponseTopic[]
  totalBytes: bigint
  usableBytes: bigint
}

export interface DescribeLogDirsResponse {
  throttleTimeMs: number
  errorCode: number
  results: DescribeLogDirsResponseResult[]
}

/*
  DescribeLogDirs Request (Version: 2) => [topics] TAG_BUFFER
    topics => topic [partitions] TAG_BUFFER
      topic => COMPACT_STRING
      partitions => INT32
*/
export function createRequest (topics: DescribeLogDirsRequestTopic[]): Writer {
  return Writer.create()
    .appendArray(topics, (w, t) => {
      w.appendString(t.name).appendArray(t.partitions, (w, p) => w.appendInt32(p), true, false)
    })
    .appendTaggedFields()
}

/*
  DescribeLogDirs Response (Version: 2) => throttle_time_ms [results] TAG_BUFFER
    throttle_time_ms => INT32
    results => error_code log_dir [topics] TAG_BUFFER
      error_code => INT16
      log_dir => COMPACT_STRING
      topics => name [partitions] TAG_BUFFER
        name => COMPACT_STRING
        partitions => partition_index partition_size offset_lag is_future_key TAG_BUFFER
          partition_index => INT32
          partition_size => INT64
          offset_lag => INT64
          is_future_key => BOOLEAN
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): DescribeLogDirsResponse {
  const errors: ResponseErrorWithLocation[] = []

  const response: DescribeLogDirsResponse = {
    throttleTimeMs: reader.readInt32(),
    errorCode: 0,
    results: reader.readArray((r, i) => {
      const errorCode = r.readInt16()

      if (errorCode !== 0) {
        errors.push([`/results/${i}`, [errorCode, null]])
      }

      return {
        errorCode,
        logDir: r.readString(),
        topics: r.readArray(reader => {
          return {
            name: reader.readString(),
            partitions: reader.readArray(reader => {
              return {
                partitionIndex: reader.readInt32(),
                partitionSize: reader.readInt64(),
                offsetLag: reader.readInt64(),
                isFutureKey: reader.readBoolean()
              }
            })
          }
        }),
        totalBytes: 0n,
        usableBytes: 0n
      }
    })
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const api = createAPI<DescribeLogDirsRequest, DescribeLogDirsResponse>(35, 2, createRequest, parseResponse)
