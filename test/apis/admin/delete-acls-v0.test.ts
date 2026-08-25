import { deepStrictEqual } from 'node:assert'
import test from 'node:test'
import { Reader, ResourcePatternTypes, Writer } from '../../../src/index.ts'
import { api, parseResponse } from '../../../src/apis/admin/delete-acls-v0.ts'
test('DeleteAcls v0 uses classic request and response headers', () => {
  const sent: unknown[][] = []
  api({ send: (...args: unknown[]) => sent.push(args) } as never, [])
  deepStrictEqual(sent[0].slice(0, 2).concat(sent[0].slice(4, 6)), [31, 0, false, false])
})

test('DeleteAcls v0 defaults resource pattern type to LITERAL', () => {
  const response = parseResponse(1, 31, 0, Reader.from(Writer.create()
    .appendInt32(0)
    .appendArray([{}], writer => writer
      .appendInt16(0)
      .appendString(null, false)
      .appendArray([{}], writer => writer
        .appendInt16(0)
        .appendString(null, false)
        .appendInt8(2)
        .appendString('topic', false)
        .appendString('User:alice', false)
        .appendString('*', false)
        .appendInt8(3)
        .appendInt8(3), false, false), false, false)))

  deepStrictEqual(response.filterResults[0].matchingAcls[0].resourcePatternType, ResourcePatternTypes.LITERAL)
})
