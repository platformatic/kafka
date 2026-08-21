import { type Reader } from '../../protocol/reader.ts'
import { createAPI } from '../definitions.ts'
import { parseResponse as parseV0Response, createRequest } from './describe-log-dirs-v0.ts'
export * from './describe-log-dirs-v0.ts'
/* DescribeLogDirs v1 has the same response layout as v0. */
export function parseResponse (_: number, apiKey: number, apiVersion: number, reader: Reader) {
  return parseV0Response(0, apiKey, apiVersion, reader)
}
export const api = createAPI(35, 1, createRequest, parseResponse, false, false)
