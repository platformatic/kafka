import { deepStrictEqual, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { apiVersionsV0, Reader, ResponseError, Writer } from '../../../src/index.ts'

test('ApiVersions v0 uses the legacy wire shape and normalizes throttle time', () => {
  strictEqual(apiVersionsV0.createRequest('client', '1.0.0').length, 0)
  const response = apiVersionsV0.parseResponse(
    1,
    18,
    0,
    Reader.from(
      Writer.create()
        .appendInt16(0)
        .appendArray([0], w => w.appendInt16(0).appendInt16(0).appendInt16(9), false, false)
    )
  )
  deepStrictEqual(response, {
    errorCode: 0,
    apiKeys: [{ apiKey: 0, name: 'Produce', minVersion: 0, maxVersion: 9 }],
    throttleTimeMs: 0
  })
  throws(
    () =>
      apiVersionsV0.parseResponse(
        1,
        18,
        0,
        Reader.from(
          Writer.create()
            .appendInt16(35)
            .appendArray([], () => {}, false, false)
        )
      ),
    ResponseError
  )
})
