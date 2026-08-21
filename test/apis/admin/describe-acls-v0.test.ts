import { deepStrictEqual, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { ResponseError } from '../../../src/errors.ts'
import { api, createRequest, parseResponse } from '../../../src/apis/admin/describe-acls-v0.ts'
import type { AclFilter } from '../../../src/apis/types.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'

test('DescribeAcls v0 handles classic requests, responses, errors, and headers', () => {
  const filter: AclFilter = { resourceType: 2, resourceName: 'topic', resourcePatternType: 3, principal: 'User:alice', host: '*', operation: 3, permissionType: 3 }
  const request = Reader.from(createRequest(filter))
  strictEqual(request.readInt8(), 2)
  strictEqual(request.readString(false), 'topic')
  request.readString(false)
  request.readString(false)
  request.readInt8()
  request.readInt8()
  strictEqual(request.remaining, 0)
  const reader = Reader.from(Writer.create().appendInt32(1).appendInt16(0).appendString(null, false).appendArray([], () => {}, false, false))
  deepStrictEqual(parseResponse(1, api.key, api.version, reader).resources, [])
  strictEqual(reader.remaining, 0)
  throws(() => parseResponse(1, api.key, api.version, Reader.from(Writer.create().appendInt32(0).appendInt16(15).appendString('failed', false).appendArray([], () => {}, false, false))), ResponseError)
  const sent: unknown[][] = []
  api({ send: (...args: unknown[]) => sent.push(args) } as never, { resourceType: 2, resourceName: null, resourcePatternType: 3, principal: null, host: null, operation: 1, permissionType: 1 })
  deepStrictEqual(sent[0].slice(0, 2).concat(sent[0].slice(4, 6)), [29, 0, false, false])
})
