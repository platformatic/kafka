import { deepStrictEqual, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { ResponseError } from '../../../src/errors.ts'
import { api, createRequest, parseResponse } from '../../../src/apis/admin/create-partitions-v0.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'

test('CreatePartitions v0 handles classic requests, responses, errors, and headers', () => {
  const request = Reader.from(createRequest([{ name: 'topic', count: 3, assignments: [] }], 100, false))
  strictEqual(request.readArray(reader => reader.readString(false), false, false)[0], 'topic')
  const reader = Reader.from(Writer.create().appendInt32(1).appendArray([{}], writer => writer.appendString('topic', false).appendInt16(0).appendString(null, false), false, false))
  deepStrictEqual(parseResponse(1, api.key, api.version, reader).results[0].name, 'topic')
  strictEqual(reader.remaining, 0)
  throws(() => parseResponse(1, api.key, api.version, Reader.from(Writer.create().appendInt32(0).appendArray([{}], writer => writer.appendString('topic', false).appendInt16(15).appendString('failed', false), false, false))), ResponseError)
  const sent: unknown[][] = []
  api({ send: (...args: unknown[]) => sent.push(args) } as never, [], 0, false)
  deepStrictEqual(sent[0].slice(0, 2).concat(sent[0].slice(4, 6)), [37, 0, false, false])
})
