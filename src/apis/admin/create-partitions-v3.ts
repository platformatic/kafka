import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export interface CreatePartitionsRequestAssignment {
  brokerIds: number[]
}

export interface CreatePartitionsRequestTopic {
  name: string
  count: number
  assignments: CreatePartitionsRequestAssignment[]
}

export type CreatePartitionsRequest = Parameters<typeof createRequest>

export interface CreatePartitionsResponseResult {
  name: string
  errorCode: number
  errorMessage: NullableString
}

export interface CreatePartitionsResponse {
  throttleTimeMs: number
  results: CreatePartitionsResponseResult[]
}

/*
  CreatePartitions Request (Version: 3) => [topics] timeout_ms validate_only TAG_BUFFER
    topics => name count [assignments] TAG_BUFFER
      name => COMPACT_STRING
      count => INT32
      assignments => [broker_ids] TAG_BUFFER
        broker_ids => INT32
    timeout_ms => INT32
    validate_only => BOOLEAN
*/
export function createRequest (
  topics: CreatePartitionsRequestTopic[],
  timeoutMs: number,
  validateOnly: boolean
): Writer {
  return Writer.create()
    .appendArray(topics, (w, t) => {
      w.appendString(t.name)
        .appendInt32(t.count)
        .appendArray(t.assignments, (w, a) => w.appendArray(a.brokerIds, (w, b) => w.appendInt32(b), true, false))
    })
    .appendInt32(timeoutMs)
    .appendBoolean(validateOnly)
    .appendTaggedFields()
}

/*
  CreatePartitions Response (Version: 3) => throttle_time_ms [results] TAG_BUFFER
    throttle_time_ms => INT32
    results => name error_code error_message TAG_BUFFER
      name => COMPACT_STRING
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): CreatePartitionsResponse {
  const errors: ResponseErrorWithLocation[] = []

  const response: CreatePartitionsResponse = {
    throttleTimeMs: reader.readInt32(),
    results: reader.readArray((r, i) => {
      const result = {
        name: r.readString(),
        errorCode: r.readInt16(),
        errorMessage: r.readNullableString()
      }

      if (result.errorCode !== 0) {
        errors.push([`/results/${i}`, result.errorCode])
      }

      return result
    })
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const api = createAPI<CreatePartitionsRequest, CreatePartitionsResponse>(37, 3, createRequest, parseResponse)
