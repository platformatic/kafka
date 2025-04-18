import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export interface CreateAclsRequestCreation {
  resourceType: number
  resourceName: string
  resourcePatternType: number
  principal: string
  host: string
  operation: number
  permissionType: number
}

export type CreateAclsRequest = Parameters<typeof createRequest>

export interface CreateAclsResponseResult {
  errorCode: number
  errorMessage: NullableString
}

export interface CreateAclsResponse {
  throttleTimeMs: number
  results: CreateAclsResponseResult[]
}

/*
CreateAcls Request (Version: 3) => [creations] TAG_BUFFER
  creations => resource_type resource_name resource_pattern_type principal host operation permission_type TAG_BUFFER
    resource_type => INT8
    resource_name => COMPACT_STRING
    resource_pattern_type => INT8
    principal => COMPACT_STRING
    host => COMPACT_STRING
    operation => INT8
    permission_type => INT8
*/
export function createRequest (creations: CreateAclsRequestCreation[]): Writer {
  return Writer.create()
    .appendArray(creations, (w, c) => {
      w.appendInt8(c.resourceType)
        .appendString(c.resourceName)
        .appendInt8(c.resourcePatternType)
        .appendString(c.principal)
        .appendString(c.host)
        .appendInt8(c.operation)
        .appendInt8(c.permissionType)
    })
    .appendTaggedFields()
}

/*
CreateAcls Response (Version: 3) => throttle_time_ms [results] TAG_BUFFER
  throttle_time_ms => INT32
  results => error_code error_message TAG_BUFFER
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): CreateAclsResponse {
  const errors: ResponseErrorWithLocation[] = []

  const response: CreateAclsResponse = {
    throttleTimeMs: reader.readInt32(),
    results: reader.readArray((r, i) => {
      const result = {
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

export const api = createAPI<CreateAclsRequest, CreateAclsResponse>(30, 3, createRequest, parseResponse)
