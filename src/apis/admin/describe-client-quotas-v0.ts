import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'
import type { ClientQuotaEntityType, ClientQuotaKey, ClientQuotaMatchTypes } from '../enumerations.ts'

export type { ClientQuotaEntityType, ClientQuotaKey } from '../enumerations.ts'

export interface DescribeClientQuotasRequestMatchComponent {
  entityType: ClientQuotaEntityType
  matchType: typeof ClientQuotaMatchTypes.EXACT
  match: NullableString
}

export interface DescribeClientQuotasRequestSpecialComponent {
  entityType: ClientQuotaEntityType
  matchType: typeof ClientQuotaMatchTypes.DEFAULT | typeof ClientQuotaMatchTypes.ANY
}

export type DescribeClientQuotasRequestComponent =
  | DescribeClientQuotasRequestMatchComponent
  | DescribeClientQuotasRequestSpecialComponent

export type DescribeClientQuotasRequest = Parameters<typeof createRequest>

export interface DescribeClientQuotasResponseValue {
  key: ClientQuotaKey
  value: number
}

export interface DescribeClientQuotasResponseEntity {
  entityType: ClientQuotaEntityType
  entityName: NullableString
}

export interface DescribeClientQuotasResponseEntry {
  entity: DescribeClientQuotasResponseEntity[]
  values: DescribeClientQuotasResponseValue[]
}

export interface DescribeClientQuotasResponse {
  throttleTimeMs: number
  errorCode: number
  errorMessage: NullableString
  entries: DescribeClientQuotasResponseEntry[] | null
}

/*
  DescribeClientQuotas Request (Version: 0) => [components] strict
    components => entity_type match_type match
      entity_type => STRING
      match_type => INT8
      match => NULLABLE_STRING
    strict => BOOLEAN
*/
export function createRequest (components: DescribeClientQuotasRequestComponent[], strict: boolean): Writer {
  return Writer.create()
    .appendArray(
      components,
      (w, c) => {
        w.appendString(c.entityType, false)
          .appendInt8(c.matchType)
          .appendString('match' in c ? c.match : null, false)
      },
      false,
      false
    )
    .appendBoolean(strict)
}

/*
  DescribeClientQuotas Response (Version: 0) => throttle_time_ms error_code error_message [entries]
    throttle_time_ms => INT32
    error_code => INT16
    error_message => NULLABLE_STRING
    entries => [entity] [values]
      entity => entity_type entity_name
        entity_type => STRING
        entity_name => NULLABLE_STRING
      values => key value
        key => STRING
        value => FLOAT64
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): DescribeClientQuotasResponse {
  const response: DescribeClientQuotasResponse = {
    throttleTimeMs: reader.readInt32(),
    errorCode: reader.readInt16(),
    errorMessage: reader.readNullableString(false),
    entries: reader.readNullableArray(
      r => {
        return {
          entity: r.readArray(
            r => {
              return {
                entityType: r.readString(false) as ClientQuotaEntityType,
                entityName: r.readNullableString(false)
              }
            },
            false,
            false
          ),
          values: r.readArray(
            r => {
              return {
                key: r.readString(false) as ClientQuotaKey,
                value: r.readFloat64()
              }
            },
            false,
            false
          )
        }
      },
      false,
      false
    )
  }

  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '/': [response.errorCode, response.errorMessage] }, response)
  }

  return response
}

export const api = createAPI<DescribeClientQuotasRequest, DescribeClientQuotasResponse>(
  48,
  0,
  createRequest,
  parseResponse,
  false,
  false
)
