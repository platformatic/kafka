import { ResponseError } from '../../errors.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'

export type EnvelopeRequest = Parameters<typeof createRequest>

export interface EnvelopeResponse {
  responseData: Buffer | null
  errorCode: number
}

/*
Envelope Request (Version: 0) => request_data request_principal client_host_address TAG_BUFFER
  request_data => COMPACT_BYTES
  request_principal => COMPACT_NULLABLE_BYTES
  client_host_address => COMPACT_BYTES
*/
export function createRequest (
  requestData: Buffer,
  requestPrincipal: Buffer | undefined | null,
  clientHostAddress: Buffer
): Writer {
  return Writer.create()
    .appendBytes(requestData)
    .appendBytes(requestPrincipal)
    .appendBytes(clientHostAddress)
    .appendTaggedFields()
}

/*
Envelope Response (Version: 0) => response_data error_code TAG_BUFFER
  response_data => COMPACT_NULLABLE_BYTES
  error_code => INT16
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): EnvelopeResponse {
  const response: EnvelopeResponse = {
    responseData: reader.readNullableBytes(),
    errorCode: reader.readInt16()
  }

  if (response.errorCode) {
    throw new ResponseError(apiKey, apiVersion, { '': response.errorCode }, response)
  }

  return response
}

export const api = createAPI<EnvelopeRequest, EnvelopeResponse>(58, 0, createRequest, parseResponse)
