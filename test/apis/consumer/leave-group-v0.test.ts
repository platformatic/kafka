import { deepStrictEqual } from 'node:assert'
import test from 'node:test'
import * as codec from '../../../src/apis/consumer/leave-group-v0.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'
test('LeaveGroup v0 serializes the first normalized member and response', () => {
  const request = Reader.from(codec.createRequest('group', [{ memberId: 'member' }]))
  deepStrictEqual([request.readString(false), request.readString(false)], ['group', 'member'])
  deepStrictEqual(codec.parseResponse(1, 13, 0, Reader.from(Writer.create().appendInt16(0))), {
    throttleTimeMs: 0,
    errorCode: 0,
    members: []
  })
})
