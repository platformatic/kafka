import { deepStrictEqual, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { FeatureUpgradeTypes, Reader, ResponseError, updateFeaturesV0, Writer } from '../../../src/index.ts'

test('UpdateFeatures v0 converts upgrade types to the legacy allowDowngrade flag and parses feature results', () => {
  const { api, createRequest, parseResponse } = updateFeaturesV0
  const featureUpdates = [
    { feature: 'metadata.version', maxVersionLevel: 3, upgradeType: FeatureUpgradeTypes.UPGRADE },
    { feature: 'metadata.version', maxVersionLevel: 2, upgradeType: FeatureUpgradeTypes.SAFE_DOWNGRADE },
    { feature: 'metadata.version', maxVersionLevel: 1, upgradeType: FeatureUpgradeTypes.UNSAFE_DOWNGRADE }
  ]
  const request = createRequest(100, featureUpdates, true)
  deepStrictEqual(request.buffer, Buffer.from('0000006404116d657461646174612e76657273696f6e00030000116d657461646174612e76657273696f6e00020100116d657461646174612e76657273696f6e0001010000', 'hex'))
  deepStrictEqual(request.buffer, createRequest(100, featureUpdates, false).buffer)

  const requestReader = Reader.from(request)
  strictEqual(requestReader.readInt32(), 100)
  deepStrictEqual(requestReader.readArray(reader => ({ feature: reader.readString(), maxVersionLevel: reader.readInt16(), allowDowngrade: reader.readBoolean() })), [
    { feature: 'metadata.version', maxVersionLevel: 3, allowDowngrade: false },
    { feature: 'metadata.version', maxVersionLevel: 2, allowDowngrade: true },
    { feature: 'metadata.version', maxVersionLevel: 1, allowDowngrade: true }
  ])
  requestReader.readTaggedFields()
  strictEqual(requestReader.remaining, 0)
  deepStrictEqual({ key: api.key, version: api.version }, { key: 57, version: 0 })
  const responseReader = Reader.from(Writer.create().appendInt32(1).appendInt16(0).appendString(null).appendArray([{ feature: 'metadata.version', errorCode: 0, errorMessage: null }], (writer, result) => writer.appendString(result.feature).appendInt16(result.errorCode).appendString(result.errorMessage)).appendUnsignedVarInt(1).appendUnsignedVarInt(42).appendUnsignedVarInt(1).appendUnsignedInt8(0))
  deepStrictEqual(parseResponse(1, 57, 0, responseReader), { throttleTimeMs: 1, errorCode: 0, errorMessage: null, results: [{ feature: 'metadata.version', errorCode: 0, errorMessage: null }] })
  strictEqual(responseReader.remaining, 0)
  throws(() => parseResponse(1, 57, 0, Reader.from(Writer.create().appendInt32(0).appendInt16(15).appendString('failed').appendArray([], () => {}).appendTaggedFields())), ResponseError)
})
