import { strictEqual, throws } from 'node:assert'
import test from 'node:test'
import {
  getTelemetrySubscriptionsV0,
  listClientMetricsResourcesV0,
  pushTelemetryV0,
  Reader,
  ResponseError,
  Writer
} from '../../../src/index.ts'

function appendUnknownTag (writer: Writer): Writer {
  return writer.appendUnsignedVarInt(1).appendUnsignedVarInt(42).appendUnsignedVarInt(2).append(Buffer.from([1, 2]))
}

test('flexible telemetry responses consume root tags before errors', () => {
  const responses = [
    [
      71,
      getTelemetrySubscriptionsV0,
      Writer.create()
        .appendInt32(0)
        .appendInt16(1)
        .appendUUID(null)
        .appendInt32(0)
        .appendArray([], () => {}, true, false)
        .appendInt32(0)
        .appendInt32(0)
        .appendBoolean(false)
        .appendArray([], () => {}, true, false)
    ],
    [72, pushTelemetryV0, Writer.create().appendInt32(0).appendInt16(1)],
    [74, listClientMetricsResourcesV0, Writer.create().appendInt32(0).appendInt16(1).appendArray([], () => {})]
  ] as const

  for (const [key, api, writer] of responses) {
    const reader = Reader.from(appendUnknownTag(writer))
    throws(() => api.parseResponse(1, key, 0, reader), ResponseError)
    strictEqual(reader.remaining, 0)
  }
})

test('ListClientMetricsResources v0 consumes resource unknown tagged fields', () => {
  const reader = Reader.from(
    appendUnknownTag(
      Writer.create().appendInt32(0).appendInt16(0).appendArray([{ name: 'resource' }], (writer, resource) => {
        writer.appendString(resource.name)
        appendUnknownTag(writer)
      }, true, false)
    )
  )

  listClientMetricsResourcesV0.parseResponse(1, 74, 0, reader)
  strictEqual(reader.remaining, 0)
})
