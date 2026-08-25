import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'
import { type AclOperationValue, type AclPermissionTypeValue, ResourcePatternTypes, type ResourcePatternTypeValue, type ResourceTypeValue } from '../enumerations.ts'
import { type AclFilter, type AclPermission } from '../types.ts'

export type DescribeAclsRequest = Parameters<typeof createRequest>
export interface DescribeAclsResponseResource { resourceType: ResourceTypeValue; resourceName: string; resourcePatternType: ResourcePatternTypeValue; acls: AclPermission[] }
export interface DescribeAclsResponse { throttleTimeMs: number; errorCode: number; errorMessage: NullableString; resources: DescribeAclsResponseResource[] }

/* DescribeAcls Request (Version: 0) => resource_type_filter resource_name_filter principal_filter host_filter operation permission_type
   resource_type_filter => INT8; resource_name_filter, principal_filter, host_filter => NULLABLE_STRING; operation, permission_type => INT8 */
export function createRequest (filter: AclFilter): Writer {
  return Writer.create().appendInt8(filter.resourceType).appendString(filter.resourceName, false).appendString(filter.principal, false)
    .appendString(filter.host, false).appendInt8(filter.operation).appendInt8(filter.permissionType)
}
/* DescribeAcls Response (Version: 0) => throttle_time_ms error_code error_message [resources]
   resources => resource_type resource_name [acls]; acls => principal host operation permission_type */
export function parseResponse (_: number, apiKey: number, apiVersion: number, reader: Reader): DescribeAclsResponse {
  const response = { throttleTimeMs: reader.readInt32(), errorCode: reader.readInt16(), errorMessage: reader.readNullableString(false), resources: reader.readArray(r => ({ resourceType: r.readInt8() as ResourceTypeValue, resourceName: r.readString(false), resourcePatternType: ResourcePatternTypes.LITERAL, acls: r.readArray(r => ({ principal: r.readString(false), host: r.readString(false), operation: r.readInt8() as AclOperationValue, permissionType: r.readInt8() as AclPermissionTypeValue }), false, false) }), false, false) }
  if (response.errorCode) throw new ResponseError(apiKey, apiVersion, { '/': [response.errorCode, response.errorMessage] }, response)
  return response
}
export const api = createAPI<DescribeAclsRequest, DescribeAclsResponse>(29, 0, createRequest, parseResponse, false, false)
