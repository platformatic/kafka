import BufferList from 'bl'
import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../index.ts'

export type DescribeAclsRequest = Parameters<typeof createRequest>

export interface DescribeAclsResponseAcl {
  principal: string
  host: string
  operation: number
  permissionType: number
}

export interface DescribeAclsResponseResource {
  resourceType: number
  resourceName: string
  patternType: number
  acls: DescribeAclsResponseAcl[]
}
export interface DescribeAclsResponse {
  throttleTimeMs: number
  errorCode: number
  errorMessage: NullableString
  resources: DescribeAclsResponseResource[]
}

/*
  DescribeAcls Request (Version: 3) => resource_type_filter resource_name_filter pattern_type_filter principal_filter host_filter operation permission_type TAG_BUFFER
    resource_type_filter => INT8
    resource_name_filter => COMPACT_NULLABLE_STRING
    pattern_type_filter => INT8
    principal_filter => COMPACT_NULLABLE_STRING
    host_filter => COMPACT_NULLABLE_STRING
    operation => INT8
    permission_type => INT8
*/
function createRequest (
  resourceTypeFilter: number,
  resourceNameFilter: NullableString,
  patternTypeFilter: number,
  principalFilter: NullableString,
  hostFilter: NullableString,
  operation: number,
  permissionType: number
): Writer {
  return Writer.create()
    .appendInt8(resourceTypeFilter)
    .appendString(resourceNameFilter)
    .appendInt8(patternTypeFilter)
    .appendString(principalFilter)
    .appendString(hostFilter)
    .appendInt8(operation)
    .appendInt8(permissionType)
    .appendTaggedFields()
}

/*
DescribeAcls Response (Version: 3) => throttle_time_ms error_code error_message [resources] TAG_BUFFER
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  resources => resource_type resource_name pattern_type [acls] TAG_BUFFER
    resource_type => INT8
    resource_name => COMPACT_STRING
    pattern_type => INT8
    acls => principal host operation permission_type TAG_BUFFER
      principal => COMPACT_STRING
      host => COMPACT_STRING
      operation => INT8
      permission_type => INT8
*/
function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  raw: BufferList
): DescribeAclsResponse {
  const reader = Reader.from(raw)

  const response: DescribeAclsResponse = {
    throttleTimeMs: reader.readInt32(),
    errorCode: reader.readInt16(),
    errorMessage: reader.readString(),
    resources: reader.readArray(r => {
      return {
        resourceType: r.readInt8(),
        resourceName: r.readString()!,
        patternType: r.readInt8(),
        acls: r.readArray(r => {
          return {
            principal: r.readString()!,
            host: r.readString()!,
            operation: r.readInt8(),
            permissionType: r.readInt8()
          }
        })!
      }
    })!
  }

  if (response.errorCode) {
    throw new ResponseError(apiKey, apiVersion, { errors: { '': response.errorCode }, response })
  }

  return response
}

export const describeAclsV3 = createAPI<DescribeAclsRequest, DescribeAclsResponse>(29, 3, createRequest, parseResponse)
