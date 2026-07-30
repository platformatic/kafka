import { deepStrictEqual, strictEqual } from 'node:assert'
import test from 'node:test'
import { api, createRequest, parseResponse } from '../../../src/apis/admin/delete-topics-v0.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'

test('DeleteTopics v0 serializes names, ignores topic IDs, and normalizes legacy responses', () => {
  const requestReader = Reader.from(createRequest([
    { name: 'by-name' },
    { topicId: 'not-a-uuid' },
    { name: 'both', topicId: '12345678-1234-1234-1234-123456789abc' }
  ], 1000))
  deepStrictEqual(
    requestReader.readArray(r => r.readString(false), false, false),
    ['by-name', '', 'both']
  )
  strictEqual(requestReader.readInt32(), 1000)
  const response = parseResponse(
    1,
    api.key,
    api.version,
    Reader.from(
      Writer.create().appendArray(['topic'], writer => writer.appendString('topic', false).appendInt16(0), false, false)
    )
  )
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    responses: [{ name: 'topic', topicId: '00000000-0000-0000-0000-000000000000', errorCode: 0, errorMessage: null }]
  })
})
