import BufferList from 'bl'
import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../index.ts'

export interface DeleteTopicsRequestTopic {
  name: string
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
function createRequest (topics: DeleteTopicsRequestTopic[], timeoutMs: number): Writer {
  return Writer.create()
    .appendArray(topics, (w, topic) => {
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
function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  raw: BufferList
): DeleteTopicsResponse {
  const reader = Reader.from(raw)
  const errors: ResponseErrorWithLocation[] = []

  const response: DeleteTopicsResponse = {
    throttleTimeMs: reader.readInt32(),
    responses: reader.readArray((r, i) => {
      const topicResponse = {
        name: r.readString(),
        topicId: r.readUUID(),
        errorCode: r.readInt16(),
        errorMessage: r.readString()
      }

      if (topicResponse.errorCode !== 0) {
        errors.push([`/responses/${i}}`, topicResponse.errorCode])
      }

      return topicResponse
    })!
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, { errors: Object.fromEntries(errors), response })
  }

  return response
}

export const deleteTopicsV6 = createAPI<DeleteTopicsRequest, DeleteTopicsResponse>(20, 6, createRequest, parseResponse)
