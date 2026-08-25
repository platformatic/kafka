import { ResponseError } from '../../errors.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export interface AlterReplicaLogDirsRequestDir {
  path: string
  topics: { name: string; partitions: number[] }[]
}

export type AlterReplicaLogDirsRequest = Parameters<typeof createRequest>

export interface AlterReplicaLogDirsResponse {
  throttleTimeMs: number
  results: { topicName: string; partitions: { partitionIndex: number; errorCode: number }[] }[]
}

/* AlterReplicaLogDirs Request (Version: 0) => [dirs]; dirs => path [topics]; topics => name [partitions] */
export function createRequest (dirs: AlterReplicaLogDirsRequestDir[]): Writer {
  return Writer.create().appendArray(
    dirs,
    (w, d) =>
      w
        .appendString(d.path, false)
        .appendArray(
          d.topics,
          (w, t) => w.appendString(t.name, false).appendArray(t.partitions, (w, p) => w.appendInt32(p), false, false),
          false,
          false
        ),
    false,
    false
  )
}
/* AlterReplicaLogDirs Response (Version: 0) => throttle_time_ms [results]; results => topic_name [partitions]; partitions => partition_index error_code */
export function parseResponse (
  _: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): AlterReplicaLogDirsResponse {
  const errors: ResponseErrorWithLocation[] = []
  const response = {
    throttleTimeMs: reader.readInt32(),
    results: reader.readArray(
      (r, i) => ({
        topicName: r.readString(false),
        partitions: r.readArray(
          (r, j) => {
            const partition = { partitionIndex: r.readInt32(), errorCode: r.readInt16() }
            if (partition.errorCode) errors.push([`/results/${i}/partitions/${j}`, [partition.errorCode, null]])
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
  if (errors.length) throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  return response
}
export const api = createAPI<AlterReplicaLogDirsRequest, AlterReplicaLogDirsResponse>(
  34,
  0,
  createRequest,
  parseResponse,
  false,
  false
)
