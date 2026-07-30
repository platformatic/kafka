import { deepStrictEqual, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { ConfigResourceTypes, listConfigResourcesV1, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { api, createRequest, parseResponse } = listConfigResourcesV1

test('createRequest serializes resource types', () => {
  const reader = Reader.from(createRequest([ConfigResourceTypes.CLIENT_METRICS, ConfigResourceTypes.GROUP]))

  deepStrictEqual(reader.readArray(r => r.readInt8(), true, false), [16, 32])
  reader.readTaggedFields()
  strictEqual(reader.remaining, 0)
})

test('uses version 1', () => {
  strictEqual(api.version, 1)
})

test('parseResponse processes config resources', () => {
  const writer = Writer.create()
    .appendInt32(0)
    .appendInt16(0)
    .appendArray(
      [{ resourceName: 'metrics', resourceType: ConfigResourceTypes.CLIENT_METRICS }],
      (w, resource) => w.appendString(resource.resourceName, true).appendInt8(resource.resourceType).appendTaggedFields(),
      true,
      false
    )
    .appendTaggedFields()

  deepStrictEqual(parseResponse(1, 74, 1, Reader.from(writer)), {
    throttleTimeMs: 0,
    errorCode: 0,
    configResources: [{ resourceName: 'metrics', resourceType: ConfigResourceTypes.CLIENT_METRICS }]
  })
})

test('parseResponse reports errors', () => {
  const writer = Writer.create().appendInt32(0).appendInt16(42).appendArray([], () => {}, true, false).appendTaggedFields()

  throws(() => parseResponse(1, 74, 1, Reader.from(writer)), ResponseError)
})
