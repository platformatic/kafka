import { createAPI } from '../definitions.ts'
import { Writer } from '../../protocol/writer.ts'
import { type Reader } from '../../protocol/reader.ts'
import { type AclFilter } from '../types.ts'
import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type ResponseErrorWithLocation } from '../definitions.ts'
import { type AclOperationValue, type AclPermissionTypeValue, type ResourcePatternTypeValue, type ResourceTypeValue } from '../enumerations.ts'
export type DeleteAclsRequest = Parameters<typeof createRequest>
export interface DeleteAclsResponse { throttleTimeMs: number; filterResults: { errorCode: number; errorMessage: NullableString; matchingAcls: { errorCode: number; errorMessage: NullableString; resourceType: ResourceTypeValue; resourceName: string; resourcePatternType: ResourcePatternTypeValue; principal: string; host: string; operation: AclOperationValue; permissionType: AclPermissionTypeValue }[] }[] }
/* DeleteAcls Request (Version: 1) => [filters]; filters include pattern_type_filter and otherwise use v0 fields. */
export function createRequest (filters: AclFilter[]): Writer { return Writer.create().appendArray(filters, (w, f) => { w.appendInt8(f.resourceType).appendString(f.resourceName, false).appendInt8(f.resourcePatternType).appendString(f.principal, false).appendString(f.host, false).appendInt8(f.operation).appendInt8(f.permissionType) }, false, false) }
/* DeleteAcls Response (Version: 1) => throttle_time_ms [filter_results]; matching ACLs include pattern_type. */
export function parseResponse (_: number, apiKey: number, apiVersion: number, reader: Reader): DeleteAclsResponse { const errors: ResponseErrorWithLocation[] = []; const response = { throttleTimeMs: reader.readInt32(), filterResults: reader.readArray((r, i) => { const errorCode = r.readInt16(); const errorMessage = r.readNullableString(false); if (errorCode) errors.push([`/filter_results/${i}`, [errorCode, errorMessage]]); return { errorCode, errorMessage, matchingAcls: r.readArray((r, j) => { const errorCode = r.readInt16(); const errorMessage = r.readNullableString(false); if (errorCode) errors.push([`/filter_results/${i}/matching_acls/${j}`, [errorCode, errorMessage]]); return { errorCode, errorMessage, resourceType: r.readInt8() as ResourceTypeValue, resourceName: r.readString(false), resourcePatternType: r.readInt8() as ResourcePatternTypeValue, principal: r.readString(false), host: r.readString(false), operation: r.readInt8() as AclOperationValue, permissionType: r.readInt8() as AclPermissionTypeValue } }, false, false) } }, false, false) }; if (errors.length) throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response); return response }
export const api = createAPI(31, 1, createRequest, parseResponse, false, false)
