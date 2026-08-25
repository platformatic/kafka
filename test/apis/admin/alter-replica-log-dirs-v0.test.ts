import { deepStrictEqual, strictEqual } from 'node:assert'
import test from 'node:test'
import { api, parseResponse } from '../../../src/apis/admin/alter-replica-log-dirs-v0.ts'
import { Reader } from '../../../src/protocol/reader.ts'

test('AlterReplicaLogDirs v0 uses classic request and response headers', () => {
  const sent: unknown[][] = []
  api({ send: (...args: unknown[]) => sent.push(args) } as never, [])
  deepStrictEqual(sent[0].slice(0, 2).concat(sent[0].slice(4, 6)), [34, 0, false, false])
})

test('AlterReplicaLogDirs v0 reads throttle time before results', () => {
  const reader = Reader.from(Buffer.from('0000002a000000010005746f70696300000001000000030000', 'hex'))

  deepStrictEqual(parseResponse(1, api.key, api.version, reader), {
    throttleTimeMs: 42,
    results: [{ topicName: 'topic', partitions: [{ partitionIndex: 3, errorCode: 0 }] }]
  })
  strictEqual(reader.remaining, 0)
})
