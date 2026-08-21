import { ResponseError } from '../../errors.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { type ResponseErrorWithLocation } from '../definitions.ts'
import { createAPI } from '../definitions.ts'
import { type ConfigResourceTypeValue } from '../enumerations.ts'
import { type AlterConfigsRequestResource, type AlterConfigsResponse } from './alter-configs-v2.ts'
export * from './alter-configs-v2.ts'
/* AlterConfigs Request (Version: 0) => [resources] validate_only; configs => name value. Response entries use classic strings. */
export function createRequest (resources: AlterConfigsRequestResource[], validateOnly: boolean): Writer { return Writer.create().appendArray(resources, (w, r) => w.appendInt8(r.resourceType).appendString(r.resourceName, false).appendArray(r.configs, (w, c) => w.appendString(c.name, false).appendString(c.value, false), false, false), false, false).appendBoolean(validateOnly) }
export function parseResponse (_: number, apiKey: number, apiVersion: number, reader: Reader): AlterConfigsResponse { const errors: ResponseErrorWithLocation[] = []; const response: AlterConfigsResponse = { throttleTimeMs: reader.readInt32(), responses: reader.readArray((r, i) => { const errorCode = r.readInt16(); const errorMessage = r.readNullableString(false); if (errorCode) errors.push([`/responses/${i}`, [errorCode, errorMessage]]); return { errorCode, errorMessage, resourceType: r.readInt8() as ConfigResourceTypeValue, resourceName: r.readString(false) } }, false, false) }; if (errors.length) throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response); return response }
export const api = createAPI(33, 0, createRequest, parseResponse, false, false)
