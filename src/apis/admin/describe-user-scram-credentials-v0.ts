import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export interface DescribeUserScramCredentialsRequestUser {
  name: string
}

export type DescribeUserScramCredentialsRequest = Parameters<typeof createRequest>

export interface DescribeUserScramCredentialsResponseResultCredentialInfo {
  mechanism: number
  iterations: number
}

export interface DescribeUserScramCredentialsResponseResult {
  user: string
  errorCode: number
  errorMessage: NullableString
  credentialInfos: DescribeUserScramCredentialsResponseResultCredentialInfo[]
}

export interface DescribeUserScramCredentialsResponse {
  throttleTimeMs: number
  errorCode: number
  errorMessage: NullableString
  results: DescribeUserScramCredentialsResponseResult[]
}

/*
  DescribeUserScramCredentials Request (Version: 0) => [users] TAG_BUFFER
    users => name TAG_BUFFER
      name => COMPACT_STRING
*/
export function createRequest (users: DescribeUserScramCredentialsRequestUser[]): Writer {
  return Writer.create()
    .appendArray(users, (w, u) => w.appendString(u.name))
    .appendTaggedFields()
}

/*
  DescribeUserScramCredentials Response (Version: 0) => throttle_time_ms error_code error_message [results] TAG_BUFFER
    throttle_time_ms => INT32
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    results => user error_code error_message [credential_infos] TAG_BUFFER
      user => COMPACT_STRING
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
      credential_infos => mechanism iterations TAG_BUFFER
        mechanism => INT8
        iterations => INT32
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): DescribeUserScramCredentialsResponse {
  const errors: ResponseErrorWithLocation[] = []

  const throttleTimeMs = reader.readInt32()
  const errorCode = reader.readInt16()
  const errorMessage = reader.readNullableString()

  if (errorCode !== 0) {
    errors.push(['', [errorCode, errorMessage]])
  }

  const response: DescribeUserScramCredentialsResponse = {
    throttleTimeMs,
    errorCode,
    errorMessage,
    results: reader.readArray((r, i) => {
      const user = r.readString()
      const errorCode = r.readInt16()
      const errorMessage = r.readNullableString()

      if (errorCode !== 0) {
        errors.push([`/results/${i}`, [errorCode, errorMessage]])
      }

      return {
        user,
        errorCode,
        errorMessage,
        credentialInfos: r.readArray(r => {
          return {
            mechanism: r.readInt8(),
            iterations: r.readInt32()
          }
        })
      }
    })
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const api = createAPI<DescribeUserScramCredentialsRequest, DescribeUserScramCredentialsResponse>(
  50,
  0,
  createRequest,
  parseResponse
)
