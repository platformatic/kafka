import { deepStrictEqual, strictEqual } from 'node:assert'
import test from 'node:test'
import { api, createRequest, parseResponse } from '../../../src/apis/admin/delete-topics-v4.ts'
import { Reader } from '../../../src/protocol/reader.ts'

test('DeleteTopics v4 consumes flexible boundaries and headers', () => {
  const request = createRequest([
    { name: 'by-name' },
    { topicId: 'not-a-uuid' },
    { name: 'both', topicId: '12345678-1234-1234-1234-123456789abc' }
  ], 42)
  deepStrictEqual(request.buffer, Buffer.from('040862792d6e616d6500010005626f7468000000002a00', 'hex'))

  const reader = Reader.from(Buffer.from('000000010206746f70696300000000', 'hex'))
  deepStrictEqual(parseResponse(1, 20, 4, reader), { throttleTimeMs: 1, responses: [{ name: 'topic', topicId: '00000000-0000-0000-0000-000000000000', errorCode: 0, errorMessage: null }] })
  strictEqual(reader.remaining, 0)

  let sent: unknown[] = []
  api({ send: (...args: unknown[]) => { sent = args } } as never, [], 0)
  deepStrictEqual({ key: api.key, version: api.version, requestTags: sent[4], responseTags: sent[5] }, { key: 20, version: 4, requestTags: true, responseTags: true })
})
