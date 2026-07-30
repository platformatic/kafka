import { deepStrictEqual, strictEqual } from 'node:assert'
import test from 'node:test'
import { Reader } from '../../../src/protocol/reader.ts'
import { api, createRequest, parseResponse } from '../../../src/apis/admin/describe-log-dirs-v0.ts'
test('DescribeLogDirs v0 uses classic request and response headers', () => {
  const sent: unknown[][] = []
  api({ send: (...args: unknown[]) => sent.push(args) } as never, [])
  deepStrictEqual(sent[0].slice(0, 2).concat(sent[0].slice(4, 6)), [35, 0, false, false])
})

test('DescribeLogDirs v0 reads throttle time before results', () => {
  const reader = Reader.from(
    Buffer.from(
      '0000002a000000010000000a2f746d702f6b61666b61000000010005746f7069630000000100000003000000000000007b00000000000001c801',
      'hex'
    )
  )
  const response = parseResponse(1, api.key, api.version, reader)

  deepStrictEqual(response, {
    throttleTimeMs: 42,
    errorCode: 0,
    results: [
      {
        errorCode: 0,
        logDir: '/tmp/kafka',
        topics: [
          {
            name: 'topic',
            partitions: [{ partitionIndex: 3, partitionSize: 123n, offsetLag: 456n, isFutureKey: true }]
          }
        ],
        totalBytes: -1n,
        usableBytes: -1n
      }
    ]
  })
  strictEqual(reader.remaining, 0)
})

test('DescribeLogDirs v0 encodes null topics with a classic nullable array', () => {
  const reader = Reader.from(createRequest(null))
  strictEqual(reader.readNullableArray(() => undefined, false, false), null)
  strictEqual(reader.remaining, 0)
})
