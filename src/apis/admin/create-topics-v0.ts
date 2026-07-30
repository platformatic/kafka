import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export interface CreateTopicsRequestTopicAssignment {
  partitionIndex: number
  brokerIds: number[]
}
export interface CreateTopicsRequestTopicConfig {
  name: string
  value?: NullableString
}
export interface CreateTopicsRequestTopic {
  name: string
  numPartitions: number
  replicationFactor: number
  assignments: CreateTopicsRequestTopicAssignment[]
  configs: CreateTopicsRequestTopicConfig[]
}
export type CreateTopicsRequest = Parameters<typeof createRequest>
export interface CreateTopicsResponseTopicConfig {
  name: string
  value: NullableString
  readOnly: boolean
  configSource: number
  isSensitive: boolean
}
export interface CreateTopicsResponseTopic {
  name: string
  topicId: string
  errorCode: number
  errorMessage: NullableString
  numPartitions: number
  replicationFactor: number
  configs: CreateTopicsResponseTopicConfig[] | null
}
export interface CreateTopicsResponse {
  throttleTimeMs: number
  topics: CreateTopicsResponseTopic[]
}

/*
  CreateTopics Request (Version: 0) => [topics] timeout_ms
    topics => name num_partitions replication_factor [assignments] [configs]
      name => STRING
      num_partitions => INT32
      replication_factor => INT16
      assignments => partition_index [broker_ids]
        partition_index => INT32
        broker_ids => INT32
      configs => name value
        name => STRING
        value => NULLABLE_STRING
    timeout_ms => INT32
  validateOnly is retained for the Admin public call shape but is not on this wire version.
*/
export function createRequest (topics: CreateTopicsRequestTopic[], timeoutMs: number, _validateOnly?: boolean): Writer {
  return Writer.create()
    .appendArray(
      topics,
      (writer, topic) =>
        writer
          .appendString(topic.name, false)
          .appendInt32(topic.numPartitions)
          .appendInt16(topic.replicationFactor)
          .appendArray(
            topic.assignments,
            (writer, assignment) =>
              writer
                .appendInt32(assignment.partitionIndex)
                .appendArray(assignment.brokerIds, (writer, brokerId) => writer.appendInt32(brokerId), false, false),
            false,
            false
          )
          .appendArray(
            topic.configs,
            (writer, config) => writer.appendString(config.name, false).appendString(config.value, false),
            false,
            false
          ),
      false,
      false
    )
    .appendInt32(timeoutMs)
}

/*
  CreateTopics Response (Version: 0) => [topics]
    topics => name error_code
      name => STRING
      error_code => INT16
  throttle_time_ms, topic_id, partition metadata, and configs are normalized for callers.
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): CreateTopicsResponse {
  const errors: ResponseErrorWithLocation[] = []
  const response: CreateTopicsResponse = {
    throttleTimeMs: 0,
    topics: reader.readArray(
      (r, index) => {
        const topic = {
          name: r.readString(false),
          topicId: '00000000-0000-0000-0000-000000000000',
          errorCode: r.readInt16(),
          errorMessage: null,
          numPartitions: -1,
          replicationFactor: -1,
          configs: null
        }
        if (topic.errorCode !== 0) {
          errors.push([`/topics/${index}`, [topic.errorCode, topic.errorMessage]])
        }
        return topic
      },
      false,
      false
    )
  }
  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }
  return response
}

export const api = createAPI<CreateTopicsRequest, CreateTopicsResponse>(
  19,
  0,
  createRequest,
  parseResponse,
  false,
  false
)
