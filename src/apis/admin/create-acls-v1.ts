import { createAPI } from '../definitions.ts'
import { Writer } from '../../protocol/writer.ts'
import { type Reader } from '../../protocol/reader.ts'
import { type Acl } from '../types.ts'
import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type ResponseErrorWithLocation } from '../definitions.ts'
export type CreateAclsRequest = Parameters<typeof createRequest>
export interface CreateAclsResponse { throttleTimeMs: number; results: { errorCode: number; errorMessage: NullableString }[] }
/* CreateAcls Request (Version: 1) => [creations]; creations => resource_type resource_name resource_pattern_type principal host operation permission_type. */
export function createRequest (creations: Acl[]): Writer { return Writer.create().appendArray(creations, (w, c) => { w.appendInt8(c.resourceType).appendString(c.resourceName, false).appendInt8(c.resourcePatternType).appendString(c.principal, false).appendString(c.host, false).appendInt8(c.operation).appendInt8(c.permissionType) }, false, false) }
/* CreateAcls Response (Version: 1) => throttle_time_ms [results]; results => error_code error_message. */
export function parseResponse (_: number, apiKey: number, apiVersion: number, reader: Reader): CreateAclsResponse { const errors: ResponseErrorWithLocation[] = []; const response = { throttleTimeMs: reader.readInt32(), results: reader.readArray((r, i) => { const result = { errorCode: r.readInt16(), errorMessage: r.readNullableString(false) }; if (result.errorCode) errors.push([`/results/${i}`, [result.errorCode, result.errorMessage]]); return result }, false, false) }; if (errors.length) throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response); return response }
export const api = createAPI(30, 1, createRequest, parseResponse, false, false)
