import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'
import type { ClientQuotaEntityType, ClientQuotaKey } from '../enumerations.ts'

export type { ClientQuotaEntityType, ClientQuotaKey } from '../enumerations.ts'

export interface AlterClientQuotasRequestEntity {
  entityType: ClientQuotaEntityType
  entityName?: NullableString
}

export interface AlterClientQuotaRequestOpAddition {
  key: ClientQuotaKey
  value: number
  remove: false
}

export interface AlterClientQuotaRequestOpRemoval {
  key: ClientQuotaKey
  remove: true
}

export type AlterClientQuotasRequestOp = AlterClientQuotaRequestOpAddition | AlterClientQuotaRequestOpRemoval

export interface AlterClientQuotasRequestEntry {
  entities: AlterClientQuotasRequestEntity[]
  ops: AlterClientQuotasRequestOp[]
}

export type AlterClientQuotasRequest = Parameters<typeof createRequest>

export interface AlterClientQuotasResponseEntity {
  entityType: ClientQuotaEntityType
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
  AlterClientQuotas Request (Version: 0) => [entries] validate_only
    entries => [entity] [ops]
      entity => entity_type entity_name
        entity_type => STRING
        entity_name => NULLABLE_STRING
      ops => key value remove
        key => STRING
        value => FLOAT64
        remove => BOOLEAN
    validate_only => BOOLEAN
*/
export function createRequest (entries: AlterClientQuotasRequestEntry[], validateOnly: boolean): Writer {
  return Writer.create()
    .appendArray(
      entries,
      (w, entry) => {
        w.appendArray(
          entry.entities,
          (w, entity) => {
            w.appendString(entity.entityType, false).appendString(entity.entityName, false)
          },
          false,
          false
        ).appendArray(
          entry.ops,
          (w, op) => {
            w.appendString(op.key, false)
              .appendFloat64((op as AlterClientQuotaRequestOpAddition).value ?? 0)
              .appendBoolean(op.remove)
          },
          false,
          false
        )
      },
      false,
      false
    )
    .appendBoolean(validateOnly)
}

/*
  AlterClientQuotas Response (Version: 0) => throttle_time_ms [entries]
    throttle_time_ms => INT32
    entries => error_code error_message [entity]
      error_code => INT16
      error_message => NULLABLE_STRING
      entity => entity_type entity_name
        entity_type => STRING
        entity_name => NULLABLE_STRING
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): AlterClientQuotasResponse {
  const errors: ResponseErrorWithLocation[] = []
  const response: AlterClientQuotasResponse = {
    throttleTimeMs: reader.readInt32(),
    entries: reader.readArray(
      (r, index) => {
        const entry = {
          errorCode: r.readInt16(),
          errorMessage: r.readNullableString(false),
          entity: r.readArray(
            r => ({ entityType: r.readString(false) as ClientQuotaEntityType, entityName: r.readNullableString(false) }),
            false,
            false
          )
        }

        if (entry.errorCode !== 0) {
          errors.push([`/entries/${index}`, [entry.errorCode, entry.errorMessage]])
        }

        return entry
      },
      false,
      false
    )
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const api = createAPI<AlterClientQuotasRequest, AlterClientQuotasResponse>(
  49,
  0,
  createRequest,
  parseResponse,
  false,
  false
)
