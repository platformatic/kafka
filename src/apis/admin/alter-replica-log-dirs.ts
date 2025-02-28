import BufferList from 'bl'
import { ResponseError } from '../../errors.ts'
import { Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../index.ts'

export interface AlterReplicaLogDirsRequestTopic {
  name: string
  partitions: number[]
}

export interface AlterReplicaLogDirsRequestDir {
  path: string
  topics: AlterReplicaLogDirsRequestTopic[]
}
export type AlterReplicaLogDirsRequest = Parameters<typeof createRequest>

export interface AlterReplicaLogDirsResponsePartition {
  partitionIndex: number
  errorCode: number
}

export interface AlterReplicaLogDirsResponseResult {
  topicName: string
  partitions: AlterReplicaLogDirsResponsePartition[]
}

export interface AlterReplicaLogDirsResponse {
  throttleTimeMs?: number
  results: AlterReplicaLogDirsResponseResult[]
}

/*
  AlterReplicaLogDirs Request (Version: 2) => [dirs] TAG_BUFFER
    dirs => path [topics] TAG_BUFFER
      path => COMPACT_STRING
      topics => name [partitions] TAG_BUFFER
        name => COMPACT_STRING
        partitions => INT32
*/
function createRequest (dirs: AlterReplicaLogDirsRequestDir[]): Writer {
  return Writer.create()
    .appendArray(dirs, (w, d) => {
      w.appendString(d.path).appendArray(d.topics, (w, t) => {
        w.appendString(t.name).appendArray(t.partitions, (w, p) => w.appendInt32(p), true, false)
      })
    })
    .appendTaggedFields()
}

/*
  AlterReplicaLogDirs Response (Version: 2) => throttle_time_ms [results] TAG_BUFFER
    throttle_time_ms => INT32
    results => topic_name [partitions] TAG_BUFFER
      topic_name => COMPACT_STRING
      partitions => partition_index error_code TAG_BUFFER
        partition_index => INT32
        error_code => INT16
*/
function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  raw: BufferList
): AlterReplicaLogDirsResponse {
  const reader = Reader.from(raw)
  const errors: ResponseErrorWithLocation[] = []

  const response: AlterReplicaLogDirsResponse = {
    throttleTimeMs: reader.readInt32(),
    results: reader.readArray((r, i) => {
      return {
        topicName: r.readString()!,
        partitions: r.readArray((r, j) => {
          const partition = {
            partitionIndex: r.readInt32(),
            errorCode: r.readInt16()
          }

          if (partition.errorCode !== 0) {
            errors.push([`/results/${i}/partitions/${j}`, partition.errorCode])
          }

          return partition
        })!
      }
    })!
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, { errors: Object.fromEntries(errors), response })
  }

  return response
}

export const alterReplicaLogDirsV2 = createAPI<AlterReplicaLogDirsRequest, AlterReplicaLogDirsResponse>(
  34,
  2,
  createRequest,
  parseResponse
)
