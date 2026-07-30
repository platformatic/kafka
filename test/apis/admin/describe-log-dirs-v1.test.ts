import { deepStrictEqual, strictEqual } from 'node:assert'
import test from 'node:test'
import { api, createRequest, parseResponse } from '../../../src/apis/admin/describe-log-dirs-v1.ts'
import { Reader } from '../../../src/protocol/reader.ts'

test('DescribeLogDirs v1 uses classic boundaries and headers', () => {
  const request = createRequest([{ name: 'topic', partitions: [2] }])
  deepStrictEqual(request.buffer, Buffer.from('000000010005746f7069630000000100000002', 'hex'))

  const reader = Reader.from(Buffer.from('0000000100000000', 'hex'))
  deepStrictEqual(parseResponse(1, 35, 1, reader), { throttleTimeMs: 1, errorCode: 0, results: [] })
  strictEqual(reader.remaining, 0)

  let sent: unknown[] = []
  api({ send: (...args: unknown[]) => { sent = args } } as never, [])
  deepStrictEqual({ key: api.key, version: api.version, requestTags: sent[4], responseTags: sent[5] }, { key: 35, version: 1, requestTags: false, responseTags: false })
})
