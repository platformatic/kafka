import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'
import type { DeleteTopicsRequestTopic as DeleteTopicsRequestTopicV6 } from './delete-topics-v6.ts'

export type DeleteTopicsRequestTopic = DeleteTopicsRequestTopicV6
export type DeleteTopicsRequest = Parameters<typeof createRequest>
export interface DeleteTopicsResponseResponse {
  name: string
  topicId: string
  errorCode: number
  errorMessage: NullableString
}
export interface DeleteTopicsResponse {
  throttleTimeMs: number
  responses: DeleteTopicsResponseResponse[]
}

/*
  DeleteTopics Request (Version: 0) => [topics] timeout_ms
    topics => name
      name => STRING
    timeout_ms => INT32
*/
export function createRequest (topics: DeleteTopicsRequestTopic[], timeoutMs: number): Writer {
  return Writer.create()
    .appendArray(topics, (writer, topic) => writer.appendString(topic.name ?? '', false), false, false)
    .appendInt32(timeoutMs)
}

/*
  DeleteTopics Response (Version: 0) => [responses]
    responses => name error_code
      name => STRING
      error_code => INT16
  throttle_time_ms, topic_id, and error_message are normalized for callers.
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): DeleteTopicsResponse {
  const errors: ResponseErrorWithLocation[] = []
  const response: DeleteTopicsResponse = {
    throttleTimeMs: 0,
    responses: reader.readArray(
      (r, index) => {
        const topic = { name: r.readString(false), topicId: '00000000-0000-0000-0000-000000000000', errorCode: r.readInt16(), errorMessage: null }
        if (topic.errorCode !== 0) {
          errors.push([`/responses/${index}`, [topic.errorCode, topic.errorMessage]])
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

export const api = createAPI<DeleteTopicsRequest, DeleteTopicsResponse>(
  20,
  0,
  createRequest,
  parseResponse,
  false,
  false
)
