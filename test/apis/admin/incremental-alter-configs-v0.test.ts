import { deepStrictEqual, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { ResponseError } from '../../../src/errors.ts'
import { api, createRequest, parseResponse } from '../../../src/apis/admin/incremental-alter-configs-v0.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'

test('IncrementalAlterConfigs v0 handles classic requests, responses, errors, and headers', () => {
  const request = Reader.from(createRequest([{ resourceType: 2, resourceName: 'topic', configs: [{ name: 'cleanup.policy', configOperation: 0, value: 'compact' }] }], true))
  strictEqual(request.readArray(reader => ({ resourceType: reader.readInt8(), resourceName: reader.readString(false), configs: reader.readArray(reader => ({ name: reader.readString(false), configOperation: reader.readInt8(), value: reader.readString(false) }), false, false) }), false, false)[0].resourceName, 'topic')
  strictEqual(request.readBoolean(), true)
  strictEqual(request.remaining, 0)
  const reader = Reader.from(Writer.create().appendInt32(1).appendArray([{}], writer => writer.appendInt16(0).appendString(null, false).appendInt8(2).appendString('topic', false), false, false))
  deepStrictEqual(parseResponse(1, api.key, api.version, reader), { throttleTimeMs: 1, responses: [{ errorCode: 0, errorMessage: null, resourceType: 2, resourceName: 'topic' }] })
  strictEqual(reader.remaining, 0)
  throws(() => parseResponse(1, api.key, api.version, Reader.from(Writer.create().appendInt32(0).appendArray([{}], writer => writer.appendInt16(15).appendString('failed', false).appendInt8(2).appendString('topic', false), false, false))), ResponseError)
  const sent: unknown[][] = []
  api({ send: (...args: unknown[]) => sent.push(args) } as never, [], false)
  deepStrictEqual(sent[0].slice(0, 2).concat(sent[0].slice(4, 6)), [44, 0, false, false])
})
