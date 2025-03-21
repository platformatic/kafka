import BufferList from 'bl'
import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export interface AlterClientQuotasRequestEntity {
  entityType: string
  entityName: NullableString
}

export interface AlterClientQuotasRequestOp {
  key: string
  value: number
  remove: boolean
}

export interface AlterClientQuotasRequestEntry {
  entities: AlterClientQuotasRequestEntity[]
  ops: AlterClientQuotasRequestOp[]
}

export type AlterClientQuotasRequest = Parameters<typeof createRequest>

export interface AlterClientQuotasResponseEntity {
  entityType: string
  entityName: NullableString
}

export interface AlterClientQuotasResponseEntries {
  errorCode: number
  errorMessage: NullableString
  entity: AlterClientQuotasResponseEntity[]
}

export interface AlterClientQuotasResponse {
  throttleTimeMs: number
  entries: AlterClientQuotasResponseEntries[]
}

/*
  AlterClientQuotas Request (Version: 1) => [entries] validate_only TAG_BUFFER
    entries => [entity] [ops] TAG_BUFFER
      entity => entity_type entity_name TAG_BUFFER
        entity_type => COMPACT_STRING
        entity_name => COMPACT_NULLABLE_STRING
      ops => key value remove TAG_BUFFER
        key => COMPACT_STRING
        value => FLOAT64
        remove => BOOLEAN
    validate_only => BOOLEAN
*/
function createRequest (entries: AlterClientQuotasRequestEntry[], validateOnly: boolean): Writer {
  return Writer.create()
    .appendArray(entries, (w, e) => {
      w.appendArray(e.entities, (w, e) => {
        w.appendString(e.entityType).appendString(e.entityName)
      }).appendArray(e.ops, (w, o) => {
        w.appendString(o.key).appendFloat64(o.value).appendBoolean(o.remove)
      })
    })
    .appendBoolean(validateOnly)
    .appendTaggedFields()
}

/*
  AlterClientQuotas Response (Version: 1) => throttle_time_ms [entries] TAG_BUFFER
    throttle_time_ms => INT32
    entries => error_code error_message [entity] TAG_BUFFER
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
      entity => entity_type entity_name TAG_BUFFER
        entity_type => COMPACT_STRING
        entity_name => COMPACT_NULLABLE_STRING
*/
function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  raw: BufferList
): AlterClientQuotasResponse {
  const reader = Reader.from(raw)
  const errors: ResponseErrorWithLocation[] = []

  const response: AlterClientQuotasResponse = {
    throttleTimeMs: reader.readInt32(),
    entries: reader.readArray((r, i) => {
      const entry = {
        errorCode: r.readInt16(),
        errorMessage: r.readString(),
        entity: r.readArray(r => {
          return {
            entityType: r.readString()!,
            entityName: r.readString()
          }
        })!
      }

      if (entry.errorCode !== 0) {
        errors.push([`/entries/${i}`, entry.errorCode])
      }

      return entry
    })!
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const alterClientQuotasV1 = createAPI<AlterClientQuotasRequest, AlterClientQuotasResponse>(
  49,
  1,
  createRequest,
  parseResponse
)
