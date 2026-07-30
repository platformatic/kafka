import { ResponseError } from '../../errors.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'
import type {
  DescribeClientQuotasRequestComponent,
  DescribeClientQuotasResponse,
  DescribeClientQuotasResponseEntry
} from './describe-client-quotas-v0.ts'

import type { ClientQuotaEntityType, ClientQuotaKey } from '../enumerations.ts'

export type {
  DescribeClientQuotasRequestComponent,
  DescribeClientQuotasRequestMatchComponent,
  DescribeClientQuotasRequestSpecialComponent,
  DescribeClientQuotasResponse,
  DescribeClientQuotasResponseEntity,
  DescribeClientQuotasResponseEntry,
  DescribeClientQuotasResponseValue
} from './describe-client-quotas-v0.ts'
export type { ClientQuotaEntityType, ClientQuotaKey } from '../enumerations.ts'

export type DescribeClientQuotasRequest = Parameters<typeof createRequest>

export function createRequest (components: DescribeClientQuotasRequestComponent[], strict: boolean): Writer {
  return Writer.create()
    .appendArray(components, (writer, component) => {
      writer
        .appendString(component.entityType)
        .appendInt8(component.matchType)
        .appendString('match' in component ? component.match : null)
    })
    .appendBoolean(strict)
    .appendTaggedFields()
}

export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): DescribeClientQuotasResponse {
  const response: DescribeClientQuotasResponse = {
    throttleTimeMs: reader.readInt32(),
    errorCode: reader.readInt16(),
    errorMessage: reader.readNullableString(),
    entries: reader.readNullableArray(r => ({
      entity: r.readArray(r => ({
        entityType: r.readString() as ClientQuotaEntityType,
        entityName: r.readNullableString()
      })),
      values: r.readArray(r => ({ key: r.readString() as ClientQuotaKey, value: r.readFloat64() }))
    })) as DescribeClientQuotasResponseEntry[] | null
  }
  reader.readTaggedFields()

  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '/': [response.errorCode, response.errorMessage] }, response)
  }

  return response
}

export const api = createAPI<DescribeClientQuotasRequest, DescribeClientQuotasResponse>(
  48,
  1,
  createRequest,
  parseResponse
)
