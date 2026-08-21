import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'
import { type ConfigResourceTypeValue, type IncrementalAlterConfigOperationTypeValue } from '../enumerations.ts'
export interface IncrementalAlterConfigsRequestResource { resourceType: ConfigResourceTypeValue; resourceName: string; configs: { name: string; configOperation: IncrementalAlterConfigOperationTypeValue; value?: string | null }[] }
export type IncrementalAlterConfigsRequest = Parameters<typeof createRequest>
export interface IncrementalAlterConfigsResponse { throttleTimeMs: number; responses: { errorCode: number; errorMessage: NullableString; resourceType: ConfigResourceTypeValue; resourceName: string }[] }
/* IncrementalAlterConfigs Request (Version: 0) => [resources] validate_only; configs => name config_operation value */
export function createRequest (resources: IncrementalAlterConfigsRequestResource[], validateOnly: boolean): Writer { return Writer.create().appendArray(resources, (w, r) => w.appendInt8(r.resourceType).appendString(r.resourceName, false).appendArray(r.configs, (w, c) => w.appendString(c.name, false).appendInt8(c.configOperation).appendString(c.value, false), false, false), false, false).appendBoolean(validateOnly) }
/* IncrementalAlterConfigs Response (Version: 0) => throttle_time_ms [responses]; responses => error_code error_message resource_type resource_name */
export function parseResponse (_: number, apiKey: number, apiVersion: number, reader: Reader): IncrementalAlterConfigsResponse { const errors: ResponseErrorWithLocation[] = []; const response = { throttleTimeMs: reader.readInt32(), responses: reader.readArray((r, i) => { const errorCode = r.readInt16(); const errorMessage = r.readNullableString(false); if (errorCode) errors.push([`/responses/${i}`, [errorCode, errorMessage]]); return { errorCode, errorMessage, resourceType: r.readInt8() as ConfigResourceTypeValue, resourceName: r.readString(false) } }, false, false) }; if (errors.length) throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response); return response }
export const api = createAPI<IncrementalAlterConfigsRequest, IncrementalAlterConfigsResponse>(44, 0, createRequest, parseResponse, false, false)
