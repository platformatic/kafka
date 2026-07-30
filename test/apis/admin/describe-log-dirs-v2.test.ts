import { deepStrictEqual, strictEqual } from 'node:assert'
import test from 'node:test'
import { api, createRequest, parseResponse } from '../../../src/apis/admin/describe-log-dirs-v2.ts'
import { Reader } from '../../../src/protocol/reader.ts'

test('DescribeLogDirs v2 consumes flexible boundaries and headers', () => {
  const request = createRequest([{ name: 'topic', partitions: [2] }])
  deepStrictEqual(request.buffer, Buffer.from('0206746f70696302000000020000', 'hex'))

  const reader = Reader.from(Buffer.from('00000001020000052f746d700206746f7069630200000002000000000000000300000000000000040000000000', 'hex'))
  deepStrictEqual(parseResponse(1, 35, 2, reader), {
    throttleTimeMs: 1,
    errorCode: 0,
    results: [{
      errorCode: 0,
      logDir: '/tmp',
      topics: [{ name: 'topic', partitions: [{ partitionIndex: 2, partitionSize: 3n, offsetLag: 4n, isFutureKey: false }] }],
      totalBytes: -1n,
      usableBytes: -1n
    }]
  })
  strictEqual(reader.remaining, 0)

  let sent: unknown[] = []
  api({ send: (...args: unknown[]) => { sent = args } } as never, [])
  deepStrictEqual({ key: api.key, version: api.version, requestTags: sent[4], responseTags: sent[5] }, { key: 35, version: 2, requestTags: true, responseTags: true })
})
