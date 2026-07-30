import { strictEqual, throws } from 'node:assert'
import test from 'node:test'
import * as apiVersionsV3 from '../../../src/apis/metadata/api-versions-v3.ts'
import * as apiVersionsV4 from '../../../src/apis/metadata/api-versions-v4.ts'
import * as findCoordinatorV4 from '../../../src/apis/metadata/find-coordinator-v4.ts'
import * as findCoordinatorV5 from '../../../src/apis/metadata/find-coordinator-v5.ts'
import * as findCoordinatorV6 from '../../../src/apis/metadata/find-coordinator-v6.ts'
import * as metadataV9 from '../../../src/apis/metadata/metadata-v9.ts'
import * as metadataV10 from '../../../src/apis/metadata/metadata-v10.ts'
import * as metadataV11 from '../../../src/apis/metadata/metadata-v11.ts'
import * as metadataV12 from '../../../src/apis/metadata/metadata-v12.ts'
import { ResponseError } from '../../../src/errors.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'

function appendUnknownTag (writer: Writer): Writer {
  return writer
    .appendUnsignedVarInt(1)
    .appendUnsignedVarInt(42)
    .appendUnsignedVarInt(2)
    .append(Buffer.from([1, 2]))
}

test('flexible metadata responses consume root tags before errors', async t => {
  for (const [version, api] of [
    [3, apiVersionsV3],
    [4, apiVersionsV4]
  ] as const) {
    await t.test(`ApiVersions v${version}`, () => {
      const reader = Reader.from(
        appendUnknownTag(
          Writer.create()
            .appendInt16(1)
            .appendArray([], () => {})
            .appendInt32(0)
        )
      )
      throws(() => api.parseResponse(1, 18, version, reader), ResponseError)
      strictEqual(reader.remaining, 0)
    })
  }

  for (const [version, api] of [
    [4, findCoordinatorV4],
    [5, findCoordinatorV5],
    [6, findCoordinatorV6]
  ] as const) {
    await t.test(`FindCoordinator v${version}`, () => {
      const reader = Reader.from(
        appendUnknownTag(
          Writer.create()
            .appendInt32(0)
            .appendArray([], () => {})
        )
      )
      api.parseResponse(1, 10, version, reader)
      strictEqual(reader.remaining, 0)
    })
  }

  for (const [version, api] of [
    [9, metadataV9],
    [10, metadataV10],
    [11, metadataV11],
    [12, metadataV12]
  ] as const) {
    await t.test(`Metadata v${version}`, () => {
      const writer = Writer.create()
        .appendInt32(0)
        .appendArray([], () => {})
        .appendString(null)
        .appendInt32(-1)
        .appendArray([], () => {})
      if (version < 11) {
        writer.appendInt32(0)
      }
      const reader = Reader.from(appendUnknownTag(writer))
      api.parseResponse(1, 3, version, reader)
      strictEqual(reader.remaining, 0)
    })
  }
})

test('flexible metadata responses consume nested tags before errors', async t => {
  for (const [version, api] of [
    [3, apiVersionsV3],
    [4, apiVersionsV4]
  ] as const) {
    await t.test(`ApiVersions v${version}`, () => {
      const writer = Writer.create()
        .appendInt16(1)
        .appendArray(
          [1],
          (w, key) => {
            w.appendInt16(key).appendInt16(0).appendInt16(1)
            appendUnknownTag(w)
          },
          true,
          false
        )
        .appendInt32(0)
      const reader = Reader.from(appendUnknownTag(writer))
      throws(() => api.parseResponse(1, 18, version, reader), ResponseError)
      strictEqual(reader.remaining, 0)
    })
  }

  for (const [version, api] of [
    [4, findCoordinatorV4],
    [5, findCoordinatorV5],
    [6, findCoordinatorV6]
  ] as const) {
    await t.test(`FindCoordinator v${version}`, () => {
      const writer = Writer.create()
        .appendInt32(0)
        .appendArray(
          [0],
          w => {
            w.appendString('group')
              .appendInt32(1)
              .appendString('host')
              .appendInt32(9092)
              .appendInt16(1)
              .appendString('error')
            appendUnknownTag(w)
          },
          true,
          false
        )
      const reader = Reader.from(appendUnknownTag(writer))
      throws(() => api.parseResponse(1, 10, version, reader), ResponseError)
      strictEqual(reader.remaining, 0)
    })
  }

  for (const [version, api] of [
    [9, metadataV9],
    [10, metadataV10],
    [11, metadataV11],
    [12, metadataV12]
  ] as const) {
    await t.test(`Metadata v${version}`, () => {
      const writer = Writer.create()
        .appendInt32(0)
        .appendArray(
          [0],
          w => {
            w.appendInt32(1).appendString('host').appendInt32(9092).appendString(null)
            appendUnknownTag(w)
          },
          true,
          false
        )
        .appendString(null)
        .appendInt32(-1)
        .appendArray(
          [0],
          w => {
            w.appendInt16(0).appendString('topic')
            if (version >= 10) {
              w.appendUUID(null)
            }
            w.appendBoolean(false)
              .appendArray(
                [0],
                w => {
                  w.appendInt16(1)
                    .appendInt32(0)
                    .appendInt32(1)
                    .appendInt32(1)
                    .appendArray([], () => {}, true, false)
                    .appendArray([], () => {}, true, false)
                    .appendArray([], () => {}, true, false)
                  appendUnknownTag(w)
                },
                true,
                false
              )
              .appendInt32(0)
            appendUnknownTag(w)
          },
          true,
          false
        )
      if (version < 11) {
        writer.appendInt32(0)
      }
      const reader = Reader.from(appendUnknownTag(writer))
      throws(() => api.parseResponse(1, 3, version, reader), ResponseError)
      strictEqual(reader.remaining, 0)
    })
  }
})
