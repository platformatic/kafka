import { ResponseError } from '../../errors.ts'
import { type Nullable } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export interface DescribeLogDirsRequestTopic {
  name: string
  partitions: number[]
}

export type DescribeLogDirsRequest = Parameters<typeof createRequest>

export interface DescribeLogDirsResponse {
  throttleTimeMs: number
  errorCode: number
  results: {
    errorCode: number
    logDir: string
    topics: {
      name: string
      partitions: { partitionIndex: number; partitionSize: bigint; offsetLag: bigint; isFutureKey: boolean }[]
    }[]
    totalBytes: bigint
    usableBytes: bigint
  }[]
}

/* DescribeLogDirs Request (Version: 0) => [topics]; topics => topic [partitions] */
export function createRequest (topics: Nullable<DescribeLogDirsRequestTopic[]>): Writer {
  return Writer.create().appendArray(
    topics,
    (w, t) => w.appendString(t.name, false).appendArray(t.partitions, (w, p) => w.appendInt32(p), false, false),
    false,
    false
  )
}
/* DescribeLogDirs Response (Version: 0) => throttle_time_ms [results]; results => error_code log_dir [topics]; partitions => partition_index partition_size offset_lag is_future_key */
export function parseResponse (_: number, apiKey: number, apiVersion: number, reader: Reader): DescribeLogDirsResponse {
  const errors: ResponseErrorWithLocation[] = []
  const response = {
    throttleTimeMs: reader.readInt32(),
    errorCode: 0,
    results: reader.readArray(
      (r, i) => {
        const errorCode = r.readInt16()
        if (errorCode) errors.push([`/results/${i}`, [errorCode, null]])
        return {
          errorCode,
          logDir: r.readString(false),
          topics: r.readArray(
            r => ({
              name: r.readString(false),
              partitions: r.readArray(
                r => ({
                  partitionIndex: r.readInt32(),
                  partitionSize: r.readInt64(),
                  offsetLag: r.readInt64(),
                  isFutureKey: r.readBoolean()
                }),
                false,
                false
              )
            }),
            false,
            false
          ),
          totalBytes: -1n,
          usableBytes: -1n
        }
      },
      false,
      false
    )
  }
  if (errors.length) throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  return response
}
export const api = createAPI<DescribeLogDirsRequest, DescribeLogDirsResponse>(
  35,
  0,
  createRequest,
  parseResponse,
  false,
  false
)
