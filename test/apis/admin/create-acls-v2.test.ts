import { deepStrictEqual, strictEqual } from 'node:assert'
import test from 'node:test'
import { api, createRequest, parseResponse } from '../../../src/apis/admin/create-acls-v2.ts'
import { Reader } from '../../../src/protocol/reader.ts'

test('CreateAcls v2 consumes flexible boundaries and headers', () => {
  const request = createRequest([{ resourceType: 2, resourceName: 'topic', resourcePatternType: 3, principal: 'User:alice', host: '*', operation: 3, permissionType: 3 }])
  deepStrictEqual(request.buffer, Buffer.from('020206746f706963030b557365723a616c696365022a03030000', 'hex'))

  const reader = Reader.from(Buffer.from('00000001020000000000', 'hex'))
  deepStrictEqual(parseResponse(1, 30, 2, reader), { throttleTimeMs: 1, results: [{ errorCode: 0, errorMessage: null }] })
  strictEqual(reader.remaining, 0)

  let sent: unknown[] = []
  api({ send: (...args: unknown[]) => { sent = args } } as never, [])
  deepStrictEqual({ key: api.key, version: api.version, requestTags: sent[4], responseTags: sent[5] }, { key: 30, version: 2, requestTags: true, responseTags: true })
})
