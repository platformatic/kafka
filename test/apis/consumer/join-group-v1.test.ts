import { deepStrictEqual } from 'node:assert'
import test from 'node:test'
import * as codec from '../../../src/apis/consumer/join-group-v1.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'
test('JoinGroup v1 writes rebalance timeout', () => {
  const reader = Reader.from(codec.createRequest('group', 1, 2, 'member', null, 'consumer', []))
  reader.readString(false)
  reader.readInt32()
  deepStrictEqual(reader.readInt32(), 2)
})

test('JoinGroup v1 normalizes an absent protocol type', () => {
  const response = codec.parseResponse(
    1,
    11,
    1,
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
  deepStrictEqual([response.protocolName, response.protocolType], ['range', null])
})

test('JoinGroup v1 normalizes a malformed null protocol name', () => {
  const response = codec.parseResponse(
    1,
    11,
    1,
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
