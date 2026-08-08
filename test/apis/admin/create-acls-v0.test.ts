import { deepStrictEqual, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { ResponseError } from '../../../src/errors.ts'
import { api, createRequest, parseResponse } from '../../../src/apis/admin/create-acls-v0.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'

test('CreateAcls v0 handles classic requests, responses, errors, and headers', () => {
  const request = Reader.from(createRequest([{ resourceType: 2, resourceName: 'topic', resourcePatternType: 3, principal: 'User:alice', host: '*', operation: 3, permissionType: 3 }]))
  strictEqual(request.readArray(reader => ({ resourceType: reader.readInt8(), resourceName: reader.readString(false), principal: reader.readString(false), host: reader.readString(false), operation: reader.readInt8(), permissionType: reader.readInt8() }), false, false)[0].principal, 'User:alice')
  strictEqual(request.remaining, 0)
  const reader = Reader.from(Writer.create().appendInt32(1).appendArray([{}], writer => writer.appendInt16(0).appendString(null, false), false, false))
  deepStrictEqual(parseResponse(1, api.key, api.version, reader), { throttleTimeMs: 1, results: [{ errorCode: 0, errorMessage: null }] })
  strictEqual(reader.remaining, 0)
  throws(() => parseResponse(1, api.key, api.version, Reader.from(Writer.create().appendInt32(0).appendArray([{}], writer => writer.appendInt16(15).appendString('failed', false), false, false))), ResponseError)
  const sent: unknown[][] = []
  api({ send: (...args: unknown[]) => sent.push(args) } as never, [])
  deepStrictEqual(sent[0].slice(0, 2).concat(sent[0].slice(4, 6)), [30, 0, false, false])
})
