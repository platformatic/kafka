import { deepStrictEqual } from 'node:assert'
import test from 'node:test'
import * as codec from '../../../src/apis/consumer/join-group-v0.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'
test('JoinGroup v0 omits rebalance timeout and normalizes absent protocol type', () => {
  const request = Reader.from(codec.createRequest('group', 1, 2, 'member', null, 'consumer', []))
  deepStrictEqual([request.readString(false), request.readInt32(), request.readString(false)], ['group', 1, 'member'])
  const response = codec.parseResponse(
    1,
    11,
    0,
    Reader.from(
      Writer.create()
        .appendInt16(0)
        .appendInt32(1)
        .appendString('range', false)
        .appendString('leader', false)
        .appendString('member', false)
        .appendArray([], () => {}, false, false)
    )
  )
  deepStrictEqual(response.throttleTimeMs, 0)
  deepStrictEqual([response.protocolName, response.protocolType], ['range', null])
})

test('JoinGroup v0 normalizes a malformed null protocol name', () => {
  const response = codec.parseResponse(
    1,
    11,
    0,
    Reader.from(
      Writer.create()
        .appendInt16(0)
        .appendInt32(1)
        .appendString(null, false)
        .appendString('leader', false)
        .appendString('member', false)
        .appendArray([], () => {}, false, false)
    )
  )
  deepStrictEqual(response.protocolName, '')
})
