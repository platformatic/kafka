import { ResponseError, UserError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export interface DeleteTopicsRequestTopic {
  name?: NullableString
  topicId?: NullableString
}

export type DeleteTopicsRequest = Parameters<typeof createRequest>

export interface DeleteTopicsResponseResponse {
  name: NullableString
  topicId: string
  errorCode: number
  errorMessage: NullableString
}

export interface DeleteTopicsResponse {
  throttleTimeMs: number
  responses: DeleteTopicsResponseResponse[]
}

/*
  DeleteTopics Request (Version: 6) => [topics] timeout_ms TAG_BUFFER
    topics => name topic_id TAG_BUFFER
      name => COMPACT_NULLABLE_STRING
      topic_id => UUID
    timeout_ms => INT32
*/
export function createRequest (topics: DeleteTopicsRequestTopic[], timeoutMs: number): Writer {
  return Writer.create()
    .appendArray(topics, (w, topic) => {
      if (topic.topicId != null && !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(topic.topicId)) {
        throw new UserError(`Invalid topic ID: ${topic.topicId}.`)
      }
      w.appendString(topic.name).appendUUID(topic.topicId)
    })
    .appendInt32(timeoutMs)
    .appendTaggedFields()
}

/*
  DeleteTopics Response (Version: 6) => throttle_time_ms [responses] TAG_BUFFER
    throttle_time_ms => INT32
    responses => name topic_id error_code error_message TAG_BUFFER
      name => COMPACT_NULLABLE_STRING
      topic_id => UUID
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): DeleteTopicsResponse {
  const errors: ResponseErrorWithLocation[] = []

  const response: DeleteTopicsResponse = {
    throttleTimeMs: reader.readInt32(),
    responses: reader.readArray((r, i) => {
      const topicResponse = {
        name: r.readNullableString(),
        topicId: r.readUUID(),
        errorCode: r.readInt16(),
        errorMessage: r.readNullableString()
      }

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

export const api = createAPI<DeleteTopicsRequest, DeleteTopicsResponse>(20, 6, createRequest, parseResponse)
