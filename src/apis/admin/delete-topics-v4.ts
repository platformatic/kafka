import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'
import type { DeleteTopicsRequestTopic as DeleteTopicsRequestTopicV6 } from './delete-topics-v6.ts'

export type DeleteTopicsRequestTopic = DeleteTopicsRequestTopicV6
export type DeleteTopicsRequest = Parameters<typeof createRequest>
export interface DeleteTopicsResponseResponse { name: string; topicId: string; errorCode: number; errorMessage: NullableString }
export interface DeleteTopicsResponse { throttleTimeMs: number; responses: DeleteTopicsResponseResponse[] }

/*
  DeleteTopics Request (Version: 4) => [topics] timeout_ms TAG_BUFFER
    topics => name TAG_BUFFER
      name => COMPACT_STRING
    timeout_ms => INT32
*/
export function createRequest (topics: DeleteTopicsRequestTopic[], timeoutMs: number): Writer {
  return Writer.create().appendArray(topics, (w, topic) => w.appendString(topic.name ?? '')).appendInt32(timeoutMs).appendTaggedFields()
}

/*
  DeleteTopics Response (Version: 4) => throttle_time_ms [responses] TAG_BUFFER
    throttle_time_ms => INT32
    responses => name error_code TAG_BUFFER
      name => COMPACT_STRING
      error_code => INT16
*/
export function parseResponse (_correlationId: number, apiKey: number, apiVersion: number, reader: Reader): DeleteTopicsResponse {
  const errors: ResponseErrorWithLocation[] = []
  const response: DeleteTopicsResponse = {
    throttleTimeMs: reader.readInt32(),
    responses: reader.readArray((r, i) => {
      const topicResponse = { name: r.readString(), topicId: '00000000-0000-0000-0000-000000000000', errorCode: r.readInt16(), errorMessage: null }
      if (topicResponse.errorCode !== 0) {
        errors.push([`/responses/${i}`, [topicResponse.errorCode, topicResponse.errorMessage]])
      }
      return topicResponse
    })
  }
  reader.readTaggedFields()
  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }
  return response
}

export const api = createAPI<DeleteTopicsRequest, DeleteTopicsResponse>(20, 4, createRequest, parseResponse)
