import { deepStrictEqual, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { findCoordinatorV0, Reader, ResponseError, Writer } from '../../../src/index.ts'

test('FindCoordinator v0 omits key type and normalizes legacy fields', () => {
  strictEqual(findCoordinatorV0.createRequest(1, ['group']).buffer.toString('hex'), '000567726f7570')
  const response = findCoordinatorV0.parseResponse(
    1,
    10,
    0,
    Reader.from(Writer.create().appendInt16(0).appendInt32(1).appendString('broker', false).appendInt32(9092))
  )
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    coordinators: [{ key: '', errorCode: 0, errorMessage: null, nodeId: 1, host: 'broker', port: 9092 }]
  })
  throws(
    () =>
      findCoordinatorV0.parseResponse(
        1,
        10,
        0,
        Reader.from(Writer.create().appendInt16(15).appendInt32(-1).appendString('', false).appendInt32(0))
      ),
    ResponseError
  )
})

test('FindCoordinator v0 uses the first key and defaults an empty key', () => {
  strictEqual(findCoordinatorV0.createRequest(0, ['group-1', 'group-2']).buffer.toString('hex'), '000767726f75702d31')
  strictEqual(findCoordinatorV0.createRequest(0, []).buffer.toString('hex'), '0000')
})
