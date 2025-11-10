import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'
import { type AclOperation, type AclPermissionType, type PatternType, type ResourceType } from '../enumerations.ts'
import { type AclPermission, type AclTarget, type AclFilter } from '../types.ts'

export type DescribeAclsRequest = Parameters<typeof createRequest>

export interface DescribeAclsResponseResource extends AclTarget {
  acls: AclPermission[]
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
export function createRequest (filter: AclFilter): Writer {
  return Writer.create()
    .appendInt8(filter.resourceType)
    .appendString(filter.resourceName)
    .appendInt8(filter.patternType)
    .appendString(filter.principal)
    .appendString(filter.host)
    .appendInt8(filter.operation)
    .appendInt8(filter.permissionType)
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
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): DescribeAclsResponse {
  const response: DescribeAclsResponse = {
    throttleTimeMs: reader.readInt32(),
    errorCode: reader.readInt16(),
    errorMessage: reader.readNullableString(),
    resources: reader.readArray(r => {
      return {
        resourceType: r.readInt8() as ResourceType,
        resourceName: r.readString(),
        patternType: r.readInt8() as PatternType,
        acls: r.readArray(r => {
          return {
            principal: r.readString(),
            host: r.readString(),
            operation: r.readInt8() as AclOperation,
            permissionType: r.readInt8() as AclPermissionType
          }
        })
      }
    })
  }

  if (response.errorCode) {
    throw new ResponseError(apiKey, apiVersion, { '': response.errorCode }, response)
  }

  return response
}

export const api = createAPI<DescribeAclsRequest, DescribeAclsResponse>(29, 3, createRequest, parseResponse)
