import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'
import type { MetadataRequestTopic } from './metadata-v12.ts'

export type MetadataRequest = Parameters<typeof createRequest>
export interface MetadataResponsePartition {
  errorCode: number
  partitionIndex: number
  leaderId: number
  leaderEpoch: number
  replicaNodes: number[]
  isrNodes: number[]
  offlineReplicas: number[]
}
export interface MetadataResponseTopic {
  errorCode: number
  name: string
  topicId: string
  isInternal: boolean
  partitions: MetadataResponsePartition[]
  topicAuthorizedOperations: number
}
export interface MetadataResponseBroker {
  nodeId: number
  host: string
  port: number
  rack: NullableString
}
export interface MetadataResponse {
  throttleTimeMs: number
  brokers: MetadataResponseBroker[]
  clusterId: NullableString
  controllerId: number
  topics: MetadataResponseTopic[]
}

/* Metadata Request (Version: 2) => [topics] */
export function createRequest (
  topics: Array<string | MetadataRequestTopic> | null,
  _allowAutoTopicCreation = false,
  _includeTopicAuthorizedOperations = false
): Writer {
  return Writer.create().appendArray(topics, (w, topic) => w.appendString(typeof topic === 'string' ? topic : topic.name ?? '', false), false, false)
}

/* Metadata Response (Version: 2) => [brokers] cluster_id controller_id [topics] */
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): MetadataResponse {
  const errors: ResponseErrorWithLocation[] = []
  const response: MetadataResponse = {
    throttleTimeMs: 0,
    brokers: reader.readArray(
      r => ({
        nodeId: r.readInt32(),
        host: r.readString(false),
        port: r.readInt32(),
        rack: r.readNullableString(false)
      }),
      false,
      false
    )!,
    clusterId: reader.readNullableString(false),
    controllerId: reader.readInt32(),
    topics: reader.readArray(
      (r, i) => {
        const errorCode = r.readInt16()
        if (errorCode !== 0) errors.push([`/topics/${i}`, [errorCode, null]])
        return {
          errorCode,
          name: r.readString(false),
          topicId: '00000000-0000-0000-0000-000000000000',
          isInternal: r.readBoolean(),
          topicAuthorizedOperations: -2147483648,
          partitions: r.readArray(
            (r, j) => {
              const errorCode = r.readInt16()
              if (errorCode !== 0) errors.push([`/topics/${i}/partitions/${j}`, [errorCode, null]])
              return {
                errorCode,
                partitionIndex: r.readInt32(),
                leaderId: r.readInt32(),
                leaderEpoch: -1,
                replicaNodes: r.readArray(() => r.readInt32(), false, false)!,
                isrNodes: r.readArray(() => r.readInt32(), false, false)!,
                offlineReplicas: []
              }
            },
            false,
            false
          )!
        }
      },
      false,
      false
    )!
  }
  if (errors.length) throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  return response
}

export const api = createAPI<MetadataRequest, MetadataResponse>(3, 2, createRequest, parseResponse, false, false)
