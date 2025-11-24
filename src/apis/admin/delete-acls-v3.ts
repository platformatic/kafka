import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export interface DeleteAclsRequestFilter {
  resourceTypeFilter: number
  resourceNameFilter?: NullableString
  patternTypeFilter: number
  principalFilter?: NullableString
  hostFilter?: NullableString
  operation: number
  permissionType: number
}
export type DeleteAclsRequest = Parameters<typeof createRequest>

export interface DeleteAclsResponseMatchingAcl {
  errorCode: number
  errorMessage: NullableString
  resourceType: number
  resourceName: string
  patternType: number
  principal: string
  host: string
  operation: number
  permissionType: number
}

export interface DeleteAclsResponseFilterResults {
  errorCode: number
  errorMessage: NullableString
  matchingAcls: DeleteAclsResponseMatchingAcl[]
}
export interface DeleteAclsResponse {
  throttleTimeMs: number
  filterResults: DeleteAclsResponseFilterResults[]
}

/*
  DeleteAcls Request (Version: 3) => [filters] TAG_BUFFER
    filters => resource_type_filter resource_name_filter pattern_type_filter principal_filter host_filter operation permission_type TAG_BUFFER
      resource_type_filter => INT8
      resource_name_filter => COMPACT_NULLABLE_STRING
      pattern_type_filter => INT8
      principal_filter => COMPACT_NULLABLE_STRING
      host_filter => COMPACT_NULLABLE_STRING
      operation => INT8
      permission_type => INT8
*/
export function createRequest (filters: DeleteAclsRequestFilter[]): Writer {
  return Writer.create()
    .appendArray(filters, (w, f) => {
      w.appendInt8(f.resourceTypeFilter)
        .appendString(f.resourceNameFilter)
        .appendInt8(f.patternTypeFilter)
        .appendString(f.principalFilter)
        .appendString(f.hostFilter)
        .appendInt8(f.operation)
        .appendInt8(f.permissionType)
    })
    .appendTaggedFields()
}

/*
  DeleteAcls Response (Version: 3) => throttle_time_ms [filter_results] TAG_BUFFER
    throttle_time_ms => INT32
    filter_results => error_code error_message [matching_acls] TAG_BUFFER
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
      matching_acls => error_code error_message resource_type resource_name pattern_type principal host operation permission_type TAG_BUFFER
        error_code => INT16
        error_message => COMPACT_NULLABLE_STRING
        resource_type => INT8
        resource_name => COMPACT_STRING
        pattern_type => INT8
        principal => COMPACT_STRING
        host => COMPACT_STRING
        operation => INT8
        permission_type => INT8
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): DeleteAclsResponse {
  const errors: ResponseErrorWithLocation[] = []

  const response: DeleteAclsResponse = {
    throttleTimeMs: reader.readInt32(),
    filterResults: reader.readArray((r, i) => {
      const errorCode = r.readInt16()
      const errorMessage = r.readNullableString()

      if (errorCode !== 0) {
        errors.push([`/filter_results/${i}`, [errorCode, errorMessage]])
      }

      return {
        errorCode,
        errorMessage,
        matchingAcls: r.readArray((r, j) => {
          const errorCode = r.readInt16()
          const errorMessage = r.readNullableString()

          if (errorCode !== 0) {
            errors.push([`/filter_results/${i}/matching_acls/${j}`, [errorCode, errorMessage]])
          }

          return {
            errorCode,
            errorMessage,
            resourceType: r.readInt8(),
            resourceName: r.readString(),
            patternType: r.readInt8(),
            principal: r.readString(),
            host: r.readString(),
            operation: r.readInt8(),
            permissionType: r.readInt8()
          }
        })
      }
    })
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const api = createAPI<DeleteAclsRequest, DeleteAclsResponse>(31, 3, createRequest, parseResponse)
