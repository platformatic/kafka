import { createAPI } from '../definitions.ts'
import { Writer } from '../../protocol/writer.ts'
import { type Reader } from '../../protocol/reader.ts'
import { ResponseError } from '../../errors.ts'
import { type AclFilter, type AclPermission } from '../types.ts'
import { type AclOperationValue, type AclPermissionTypeValue, type ResourcePatternTypeValue, type ResourceTypeValue } from '../enumerations.ts'
import { type NullableString } from '../../protocol/definitions.ts'
export type DescribeAclsRequest = Parameters<typeof createRequest>
export interface DescribeAclsResponseResource { resourceType: ResourceTypeValue; resourceName: string; resourcePatternType: ResourcePatternTypeValue; acls: AclPermission[] }
export interface DescribeAclsResponse { throttleTimeMs: number; errorCode: number; errorMessage: NullableString; resources: DescribeAclsResponseResource[] }
/* DescribeAcls Request (Version: 1) => resource_type_filter resource_name_filter pattern_type_filter principal_filter host_filter operation permission_type.
   Strings are NULLABLE_STRING and all collections in the response are classic ARRAY. */
export function createRequest (filter: AclFilter): Writer {
  return Writer.create().appendInt8(filter.resourceType).appendString(filter.resourceName, false).appendInt8(filter.resourcePatternType)
    .appendString(filter.principal, false).appendString(filter.host, false).appendInt8(filter.operation).appendInt8(filter.permissionType)
}
/* DescribeAcls Response (Version: 1) => throttle_time_ms error_code error_message [resources].
   resources => resource_type resource_name pattern_type [acls]; acls => principal host operation permission_type. */
export function parseResponse (_: number, apiKey: number, apiVersion: number, reader: Reader): DescribeAclsResponse {
  const response = { throttleTimeMs: reader.readInt32(), errorCode: reader.readInt16(), errorMessage: reader.readNullableString(false), resources: reader.readArray(r => ({ resourceType: r.readInt8() as ResourceTypeValue, resourceName: r.readString(false), resourcePatternType: r.readInt8() as ResourcePatternTypeValue, acls: r.readArray(r => ({ principal: r.readString(false), host: r.readString(false), operation: r.readInt8() as AclOperationValue, permissionType: r.readInt8() as AclPermissionTypeValue }), false, false) }), false, false) }
  if (response.errorCode) throw new ResponseError(apiKey, apiVersion, { '/': [response.errorCode, response.errorMessage] }, response)
  return response
}
export const api = createAPI(29, 1, createRequest, parseResponse, false, false)
